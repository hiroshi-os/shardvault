package metadata

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMetadataStoreBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "metatest-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "meta.db")
	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 1. Bucket operations
	if err := s.CreateBucket("photos"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	// duplicate bucket
	if err := s.CreateBucket("photos"); err != ErrBucketExists {
		t.Fatalf("expected ErrBucketExists, got %v", err)
	}
	exists, err := s.BucketExists("photos")
	if err != nil || !exists {
		t.Fatalf("expected bucket to exist")
	}

	// 2. Commit Version
	v1 := &VersionMeta{
		VersionID:    "01J9V1",
		Bucket:       "photos",
		Key:          "cat.jpg",
		Size:         1000,
		ETag:         "abcd123",
		StripeCount:  1,
		StripeSize:   1048576,
		StripeSHAs:   []string{"sha1"},
		CreatedAt:    time.Now().UTC(),
		IsDeleteMark: false,
	}

	if err := s.CommitVersion(v1); err != nil {
		t.Fatalf("CommitVersion v1 failed: %v", err)
	}

	// 3. Read back Current Version
	cur, err := s.GetCurrentVersion("photos", "cat.jpg")
	if err != nil {
		t.Fatalf("GetCurrentVersion failed: %v", err)
	}
	if cur.VersionID != "01J9V1" || cur.Size != 1000 || cur.ETag != "abcd123" {
		t.Fatalf("version metadata mismatch: %+v", cur)
	}

	// 4. Overwrite with Version 2
	v2 := &VersionMeta{
		VersionID:    "01J9V2",
		Bucket:       "photos",
		Key:          "cat.jpg",
		Size:         2000,
		ETag:         "efgh456",
		StripeCount:  2,
		StripeSize:   1048576,
		StripeSHAs:   []string{"sha1", "sha2"},
		CreatedAt:    time.Now().UTC(),
		IsDeleteMark: false,
	}
	if err := s.CommitVersion(v2); err != nil {
		t.Fatalf("CommitVersion v2 failed: %v", err)
	}

	cur2, err := s.GetCurrentVersion("photos", "cat.jpg")
	if err != nil {
		t.Fatalf("GetCurrentVersion v2 failed: %v", err)
	}
	if cur2.VersionID != "01J9V2" || cur2.Size != 2000 {
		t.Fatalf("version v2 mismatch: %+v", cur2)
	}

	// Older version v1 still retrievable by ID
	oldV1, err := s.GetVersion("01J9V1")
	if err != nil || oldV1.Size != 1000 {
		t.Fatalf("historical v1 get failed: %+v, err: %v", oldV1, err)
	}

	// 5. Delete marker
	vDel := &VersionMeta{
		VersionID:    "01J9VDEL",
		Bucket:       "photos",
		Key:          "cat.jpg",
		IsDeleteMark: true,
		CreatedAt:    time.Now().UTC(),
	}
	if err := s.CommitVersion(vDel); err != nil {
		t.Fatalf("Commit delete marker failed: %v", err)
	}
	_, err = s.GetCurrentVersion("photos", "cat.jpg")
	if err != ErrNoSuchKey {
		t.Fatalf("expected ErrNoSuchKey after delete marker, got %v", err)
	}

	// 6. List objects
	vNew := &VersionMeta{
		VersionID: "01J9VNEW",
		Bucket:    "photos",
		Key:       "dog.jpg",
		Size:      500,
		ETag:      "dogetag",
		CreatedAt: time.Now().UTC(),
	}
	if err := s.CommitVersion(vNew); err != nil {
		t.Fatal(err)
	}

	items, _, err := s.ListObjects("photos", "", "", 10)
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	if len(items) != 1 || items[0].Key != "dog.jpg" {
		t.Fatalf("expected 1 item (dog.jpg), got %d items: %+v", len(items), items)
	}
}
