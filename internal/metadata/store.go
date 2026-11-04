package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrNoSuchBucket    = errors.New("NoSuchBucket")
	ErrNoSuchKey       = errors.New("NoSuchKey")
	ErrBucketExists    = errors.New("BucketAlreadyExists")
	ErrVersionNotFound = errors.New("VersionNotFound")
)

var (
	bucketBuckets  = []byte("buckets")
	bucketObjects  = []byte("objects")  // key: "bucket/key" -> currentVersionID (or "DELETED")
	bucketVersions = []byte("versions") // key: "versionID" -> VersionMeta (JSON)
)

// ShardLoc records the storage node hosting a specific shard
type ShardLoc struct {
	StripeIdx uint32 `json:"stripe_idx"`
	ShardIdx  uint16 `json:"shard_idx"`
	Node      string `json:"node"`
}

// VersionMeta stores full metadata for an object version.
type VersionMeta struct {
	VersionID    string     `json:"version_id"`
	Bucket       string     `json:"bucket"`
	Key          string     `json:"key"`
	Size         int64      `json:"size"`
	ETag         string     `json:"etag"`
	StripeCount  uint32     `json:"stripe_count"`
	StripeSize   int64      `json:"stripe_size"`
	StripeSHAs   []string   `json:"stripe_shas"` // hex SHA-256 of each stripe
	Locations    []ShardLoc `json:"locations"`
	Missing      []ShardLoc `json:"missing"` // shards that failed during write
	IsDeleteMark bool       `json:"is_delete_mark"`
	CreatedAt    time.Time  `json:"created_at"`
}

// Store wraps BBolt embedded key-value database.
type Store struct {
	db *bolt.DB
}

// Open creates or opens the bbolt metadata store at path.
func Open(dbPath string) (*Store, error) {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt: %w", err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketBuckets, bucketObjects, bucketVersions} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{db: db}, nil
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

// CreateBucket creates a new S3 bucket if it doesn't already exist.
func (s *Store) CreateBucket(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketBuckets)
		if b.Get([]byte(name)) != nil {
			return ErrBucketExists
		}
		return b.Put([]byte(name), []byte(time.Now().UTC().Format(time.RFC3339)))
	})
}

// BucketExists checks if a bucket exists.
func (s *Store) BucketExists(name string) (bool, error) {
	var exists bool
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketBuckets)
		exists = b.Get([]byte(name)) != nil
		return nil
	})
	return exists, err
}

func objectKey(bucket, key string) []byte {
	return []byte(bucket + "/" + key)
}

// CommitVersion atomically saves VersionMeta and swaps the current_version pointer for (bucket, key).
// This is the atomic visibility point.
func (s *Store) CommitVersion(meta *VersionMeta) error {
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		bBuckets := tx.Bucket(bucketBuckets)
		if bBuckets.Get([]byte(meta.Bucket)) == nil {
			return ErrNoSuchBucket
		}

		bVersions := tx.Bucket(bucketVersions)
		if err := bVersions.Put([]byte(meta.VersionID), metaBytes); err != nil {
			return err
		}

		bObjects := tx.Bucket(bucketObjects)
		objKey := objectKey(meta.Bucket, meta.Key)
		return bObjects.Put(objKey, []byte(meta.VersionID))
	})
}

// GetCurrentVersion retrieves current active version for bucket and key.
func (s *Store) GetCurrentVersion(bucket, key string) (*VersionMeta, error) {
	var meta VersionMeta
	err := s.db.View(func(tx *bolt.Tx) error {
		bBuckets := tx.Bucket(bucketBuckets)
		if bBuckets.Get([]byte(bucket)) == nil {
			return ErrNoSuchBucket
		}

		bObjects := tx.Bucket(bucketObjects)
		verIDBytes := bObjects.Get(objectKey(bucket, key))
		if verIDBytes == nil {
			return ErrNoSuchKey
		}

		bVersions := tx.Bucket(bucketVersions)
		metaBytes := bVersions.Get(verIDBytes)
		if metaBytes == nil {
			return ErrNoSuchKey
		}

		if err := json.Unmarshal(metaBytes, &meta); err != nil {
			return err
		}

		if meta.IsDeleteMark {
			return ErrNoSuchKey
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// GetVersion retrieves a specific version by its version ID.
func (s *Store) GetVersion(versionID string) (*VersionMeta, error) {
	var meta VersionMeta
	err := s.db.View(func(tx *bolt.Tx) error {
		bVersions := tx.Bucket(bucketVersions)
		metaBytes := bVersions.Get([]byte(versionID))
		if metaBytes == nil {
			return ErrVersionNotFound
		}
		return json.Unmarshal(metaBytes, &meta)
	})
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// UpdateVersionMetadata updates version metadata (e.g., after scrubber repairs missing shards).
func (s *Store) UpdateVersionMetadata(meta *VersionMeta) error {
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		bVersions := tx.Bucket(bucketVersions)
		return bVersions.Put([]byte(meta.VersionID), metaBytes)
	})
}

// ListObjectsResult represents an item in ListObjects
type ObjectItem struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
	VersionID    string
}

// ListObjects lists active objects in a bucket with prefix, continuation token, and max keys.
func (s *Store) ListObjects(bucket, prefix, continuationToken string, maxKeys int) ([]ObjectItem, string, error) {
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	var items []ObjectItem
	var nextContinuation string

	err := s.db.View(func(tx *bolt.Tx) error {
		bBuckets := tx.Bucket(bucketBuckets)
		if bBuckets.Get([]byte(bucket)) == nil {
			return ErrNoSuchBucket
		}

		bObjects := tx.Bucket(bucketObjects)
		bVersions := tx.Bucket(bucketVersions)
		c := bObjects.Cursor()

		bPrefix := []byte(bucket + "/" + prefix)
		var seekKey []byte
		if continuationToken != "" {
			seekKey = []byte(bucket + "/" + continuationToken)
		} else {
			seekKey = bPrefix
		}

		for k, v := c.Seek(seekKey); k != nil; k, v = c.Next() {
			kStr := string(k)
			if !hasPrefix(k, []byte(bucket+"/")) {
				break
			}
			objKey := kStr[len(bucket)+1:]
			if continuationToken != "" && objKey <= continuationToken {
				continue
			}
			if prefix != "" && !hasPrefix([]byte(objKey), []byte(prefix)) {
				break
			}

			metaBytes := bVersions.Get(v)
			if metaBytes == nil {
				continue
			}
			var meta VersionMeta
			if err := json.Unmarshal(metaBytes, &meta); err != nil {
				continue
			}
			if meta.IsDeleteMark {
				continue
			}

			if len(items) >= maxKeys {
				nextContinuation = items[len(items)-1].Key
				break
			}

			items = append(items, ObjectItem{
				Key:          meta.Key,
				Size:         meta.Size,
				ETag:         meta.ETag,
				LastModified: meta.CreatedAt,
				VersionID:    meta.VersionID,
			})
		}
		return nil
	})

	return items, nextContinuation, err
}

func hasPrefix(s, prefix []byte) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i := range prefix {
		if s[i] != prefix[i] {
			return false
		}
	}
	return true
}

// AllVersions returns all recorded versions (for Scrubber and GC).
func (s *Store) AllVersions() ([]VersionMeta, error) {
	var list []VersionMeta
	err := s.db.View(func(tx *bolt.Tx) error {
		bVersions := tx.Bucket(bucketVersions)
		return bVersions.ForEach(func(k, v []byte) error {
			var m VersionMeta
			if err := json.Unmarshal(v, &m); err == nil {
				list = append(list, m)
			}
			return nil
		})
	})
	return list, err
}

// AllActiveVersionIDs returns a set of all version IDs currently pointed to by bucket/key objects.
func (s *Store) AllActiveVersionIDs() (map[string]bool, error) {
	active := make(map[string]bool)
	err := s.db.View(func(tx *bolt.Tx) error {
		bObjects := tx.Bucket(bucketObjects)
		return bObjects.ForEach(func(k, v []byte) error {
			active[string(v)] = true
			return nil
		})
	})
	return active, err
}

// ActiveVersions returns all currently active VersionMeta objects (not superseded, not delete markers).
func (s *Store) ActiveVersions() ([]VersionMeta, error) {
	var list []VersionMeta
	err := s.db.View(func(tx *bolt.Tx) error {
		bObjects := tx.Bucket(bucketObjects)
		bVersions := tx.Bucket(bucketVersions)
		return bObjects.ForEach(func(k, v []byte) error {
			metaBytes := bVersions.Get(v)
			if metaBytes == nil {
				return nil
			}
			var m VersionMeta
			if err := json.Unmarshal(metaBytes, &m); err == nil && !m.IsDeleteMark {
				list = append(list, m)
			}
			return nil
		})
	})
	return list, err
}
