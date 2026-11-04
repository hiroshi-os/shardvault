package node

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/hiroshi-os/shardvault/internal/shard"
)

func TestNodeBasicCRUD(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "shardnode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	n, err := New(Config{DataDir: tmpDir})
	if err != nil {
		t.Fatal(err)
	}
	handler := n.Handler()

	// 1. Healthz
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	// 2. Put shard
	shardPayload := []byte("hello world storage payload")
	hdr := &shard.ShardHeader{
		Version:         shard.CurrentVersion,
		ObjectVersionID: "01J9TEST0123456789ABCDEF01",
		StripeIdx:       0,
		ShardIdx:        0,
		DataLen:         uint64(len(shardPayload)),
		DataCRC32C:      shard.Checksum(shardPayload),
	}
	encodedHdr := hdr.Encode()
	fullData := append(encodedHdr, shardPayload...)

	shardID := "01J9TEST0123456789ABCDEF01/0/0"
	req = httptest.NewRequest(http.MethodPut, "/shards/"+shardID, bytes.NewReader(fullData))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT failed with code %d: %s", rr.Code, rr.Body.String())
	}

	// 3. Get shard
	req = httptest.NewRequest(http.MethodGet, "/shards/"+shardID, nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET failed with code %d: %s", rr.Code, rr.Body.String())
	}
	if !bytes.Equal(rr.Body.Bytes(), fullData) {
		t.Fatalf("retrieved shard content mismatch")
	}

	// 4. List shards
	req = httptest.NewRequest(http.MethodGet, "/shards?prefix=01J9TEST", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("LIST failed with code %d: %s", rr.Code, rr.Body.String())
	}
	lines := strings.TrimSpace(rr.Body.String())
	if lines != shardID {
		t.Fatalf("expected shard %q, got %q", shardID, lines)
	}

	// 5. Corrupt shard on disk, verify GET detects corruption
	shardPath := n.shardFilePath(shardID)
	corruptBytes, err := os.ReadFile(shardPath)
	if err != nil {
		t.Fatal(err)
	}
	// corrupt a byte in payload
	corruptBytes[len(corruptBytes)-1] ^= 0xFF
	if err := os.WriteFile(shardPath, corruptBytes, 0644); err != nil {
		t.Fatal(err)
	}

	req = httptest.NewRequest(http.MethodGet, "/shards/"+shardID, nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for corrupted shard, got %d", rr.Code)
	}
	if rr.Header().Get("x-sv-corrupt") != "1" {
		t.Fatalf("expected x-sv-corrupt: 1 header")
	}

	// 6. Delete shard
	req = httptest.NewRequest(http.MethodDelete, "/shards/"+shardID, nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("DELETE failed with code %d", rr.Code)
	}

	// 7. Get after delete -> 404
	req = httptest.NewRequest(http.MethodGet, "/shards/"+shardID, nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}
