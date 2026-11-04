package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/hiroshi-os/shardvault/internal/gateway"
	"github.com/hiroshi-os/shardvault/internal/metadata"
	"github.com/hiroshi-os/shardvault/internal/node"
	"github.com/hiroshi-os/shardvault/internal/scrubber"
)

func TestChaosSmoke(t *testing.T) {
	// 1. Start 6 in-process storage nodes
	numNodes := 6
	nodeServers := make([]*httptest.Server, numNodes)
	nodeDirs := make([]string, numNodes)
	nodeURLs := make([]string, numNodes)

	for i := 0; i < numNodes; i++ {
		dataDir := t.TempDir()
		nodeDirs[i] = dataDir
		nd, err := node.New(node.Config{
			DataDir: dataDir,
		})
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}
		ts := httptest.NewServer(nd.Handler())
		t.Cleanup(ts.Close)
		nodeServers[i] = ts
		nodeURLs[i] = ts.URL
	}

	// 2. Start Gateway
	metaPath := filepath.Join(t.TempDir(), "meta.db")
	metaStore, err := metadata.Open(metaPath)
	if err != nil {
		t.Fatalf("failed to init metadata store: %v", err)
	}
	t.Cleanup(func() { _ = metaStore.Close() })

	gwCfg := gateway.Config{
		StorageNodes: nodeURLs,
		K:            4,
		M:            2,
		WriteQuorum:  5,
		StripeSize:   256 * 1024, // 256 KiB stripe for quick testing
		HedgeDelay:   25 * time.Millisecond,
	}
	gw, err := gateway.New(gwCfg, metaStore)
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}

	sc := scrubber.New(metaStore, gwCfg)
	gw.SetAdminOps(sc.Scrub, sc.GarbageCollect)

	gwServer := httptest.NewServer(gw.Handler())
	t.Cleanup(gwServer.Close)

	client := gwServer.Client()
	bucket := "chaos-smoke-bucket"

	// Create bucket
	putReq, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s", gwServer.URL, bucket), nil)
	putResp, err := client.Do(putReq)
	if err != nil || putResp.StatusCode != http.StatusOK {
		t.Fatalf("failed to create bucket: status %v, err %v", putResp.StatusCode, err)
	}
	_ = putResp.Body.Close()

	// 3. Upload 200 objects (<= 1 MiB)
	numObjects := 200
	objects := make([]ObjectInfo, numObjects)
	seed := int64(7)

	for i := 0; i < numObjects; i++ {
		var size int
		bucketRoll := (int(seed)*31 + i*17) % 100
		if bucketRoll < 60 {
			size = 1024 + ((i * 1021) % (63 * 1024)) // 1-64 KiB
		} else if bucketRoll < 90 {
			size = 64*1024 + ((i * 1021) % (448 * 1024)) // 64-512 KiB
		} else {
			size = 512*1024 + ((i * 1021) % (512 * 1024)) // 512 KiB - 1 MiB
		}

		data := make([]byte, size)
		for j := range data {
			data[j] = byte((i*11 + j*13) % 256)
		}
		h := sha256.Sum256(data)
		shaStr := hex.EncodeToString(h[:])
		key := fmt.Sprintf("smoke_obj_%04d.bin", i)

		uploadReq, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s/%s", gwServer.URL, bucket, key), bytes.NewReader(data))
		uploadResp, err := client.Do(uploadReq)
		if err != nil || uploadResp.StatusCode != http.StatusOK {
			t.Fatalf("upload object %d failed: err %v", i, err)
		}
		_ = uploadResp.Body.Close()

		objects[i] = ObjectInfo{Key: key, Size: int64(size), SHA: shaStr}
	}

	verifyObjects := func(noVerify bool) (identical, wrong, s503 int) {
		for _, obj := range objects {
			req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s/%s", gwServer.URL, bucket, obj.Key), nil)
			if noVerify {
				req.Header.Set("x-sv-no-verify", "1")
			}
			resp, err := client.Do(req)
			if err != nil {
				s503++
				continue
			}
			if resp.StatusCode == http.StatusServiceUnavailable {
				s503++
				_ = resp.Body.Close()
				continue
			}
			if resp.StatusCode != http.StatusOK {
				s503++
				_ = resp.Body.Close()
				continue
			}
			data, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()

			gotSHA := hex.EncodeToString(sha256Sum(data))
			if gotSHA == obj.SHA {
				identical++
			} else {
				wrong++
			}
		}
		return
	}

	// Baseline read verification
	id, wr, c503 := verifyObjects(false)
	if id != numObjects || wr > 0 || c503 > 0 {
		t.Fatalf("baseline verification failed: id=%d, wr=%d, 503=%d", id, wr, c503)
	}

	// 4. Phase 1: Kill 2 random nodes (m=2 fault tolerance)
	t.Logf("Running Phase 1: 2 nodes offline...")
	_ = setNodeFault(client, nodeURLs[0], true)
	_ = setNodeFault(client, nodeURLs[1], true)

	id, wr, c503 = verifyObjects(false)
	t.Logf("Phase 1: identical=%d, wrong=%d, 503=%d", id, wr, c503)
	if wr > 0 {
		t.Fatalf("Phase 1 invariant violated: wrong bytes = %d (must be 0)", wr)
	}
	if id != numObjects {
		t.Fatalf("Phase 1 failed: expected all objects to be read within m=2 fault tolerance, got %d identical", id)
	}

	// Revive nodes
	_ = setNodeFault(client, nodeURLs[0], false)
	_ = setNodeFault(client, nodeURLs[1], false)

	// 5. Phase 2: Corrupt 1% of shard files on disk across surviving nodes
	t.Logf("Running Phase 2: corrupting shards on disk...")
	corruptCount := corruptShards(nodeDirs, 0.05) // corrupt 5% of shard files
	t.Logf("Corrupted %d shard files", corruptCount)

	id, wr, c503 = verifyObjects(false)
	t.Logf("Phase 2: identical=%d, wrong=%d, 503=%d", id, wr, c503)
	if wr > 0 {
		t.Fatalf("Phase 2 invariant violated: wrong bytes = %d (must be 0)", wr)
	}

	// 6. Phase 4: Run scrubber to repair
	t.Logf("Running Phase 4: Scrubber & Repair...")
	scrubReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/admin/scrub", gwServer.URL), nil)
	sResp, err := client.Do(scrubReq)
	if err == nil {
		_ = sResp.Body.Close()
	}
	time.Sleep(500 * time.Millisecond)

	id, wr, c503 = verifyObjects(false)
	t.Logf("Post-Scrub: identical=%d, wrong=%d, 503=%d", id, wr, c503)
	if wr > 0 {
		t.Fatalf("Post-scrub invariant violated: wrong bytes = %d (must be 0)", wr)
	}

	// 7. Negative Control (-no-verify)
	// Must return wrong bytes when verification is disabled and shards are corrupted!
	t.Logf("Running Negative Control with -no-verify...")
	_ = corruptShards(nodeDirs, 0.20)
	_, wrNeg, _ := verifyObjects(true)
	t.Logf("Negative Control (-no-verify): wrong bytes = %d", wrNeg)
	if wrNeg == 0 {
		t.Fatalf("Negative control failed: expected wrong-bytes > 0 under -no-verify, got %d", wrNeg)
	}
}

func sha256Sum(b []byte) []byte {
	h := sha256.Sum256(b)
	return h[:]
}
