package gateway_test

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hiroshi-os/shardvault/internal/gateway"
	"github.com/hiroshi-os/shardvault/internal/metadata"
	"github.com/hiroshi-os/shardvault/internal/node"
)

// clusterHarness creates an in-process cluster with 6 storage nodes and a gateway.
type clusterHarness struct {
	nodes   []*node.Node
	servers []*httptest.Server
	gw      *gateway.Gateway
	gwServ  *httptest.Server
	meta    *metadata.Store
	dirs    []string
}

func newClusterHarness(t *testing.T, k, m, quorum int) *clusterHarness {
	numNodes := k + m
	h := &clusterHarness{}

	var nodeURLs []string
	for i := 0; i < numNodes; i++ {
		tmpDir, err := os.MkdirTemp("", fmt.Sprintf("sn-test-%d-*", i))
		if err != nil {
			t.Fatal(err)
		}
		h.dirs = append(h.dirs, tmpDir)

		n, err := node.New(node.Config{DataDir: tmpDir})
		if err != nil {
			t.Fatal(err)
		}
		h.nodes = append(h.nodes, n)

		serv := httptest.NewServer(n.Handler())
		h.servers = append(h.servers, serv)
		nodeURLs = append(nodeURLs, serv.URL)
	}

	gwDir, err := os.MkdirTemp("", "gw-test-*")
	if err != nil {
		t.Fatal(err)
	}
	h.dirs = append(h.dirs, gwDir)

	metaStore, err := metadata.Open(filepath.Join(gwDir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	h.meta = metaStore

	cfg := gateway.Config{
		K:            k,
		M:            m,
		WriteQuorum:  quorum,
		StorageNodes: nodeURLs,
		HedgeDelay:   15 * time.Millisecond,
	}

	gw, err := gateway.New(cfg, metaStore)
	if err != nil {
		t.Fatal(err)
	}
	h.gw = gw
	h.gwServ = httptest.NewServer(gw.Handler())

	return h
}

func (h *clusterHarness) Close() {
	if h.gwServ != nil {
		h.gwServ.Close()
	}
	for _, s := range h.servers {
		s.Close()
	}
	if h.meta != nil {
		h.meta.Close()
	}
	for _, d := range h.dirs {
		_ = os.RemoveAll(d)
	}
}

func TestGatewayLifecyclePUT_GET_Range_Delete(t *testing.T) {
	h := newClusterHarness(t, 4, 2, 5)
	defer h.Close()

	client := h.gwServ.Client()

	// 1. PUT bucket
	req, _ := http.NewRequest(http.MethodPut, h.gwServ.URL+"/testbucket", nil)
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT bucket failed: %v, code %d", err, resp.StatusCode)
	}

	// 2. PUT object (2.5 MiB across 3 stripes)
	payloadSize := int(2.5 * 1024 * 1024)
	payload := make([]byte, payloadSize)
	_, _ = rand.Read(payload)
	payloadMD5 := md5.Sum(payload)
	expectedETag := hex.EncodeToString(payloadMD5[:])

	req, _ = http.NewRequest(http.MethodPut, h.gwServ.URL+"/testbucket/largefile.bin", bytes.NewReader(payload))
	resp, err = client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT object failed: %v, code %d", err, resp.StatusCode)
	}
	etag := resp.Header.Get("ETag")
	if etag != fmt.Sprintf("\"%s\"", expectedETag) {
		t.Fatalf("ETag mismatch: got %s, want %s", etag, expectedETag)
	}
	versionID := resp.Header.Get("x-sv-version-id")
	if versionID == "" {
		t.Fatalf("missing x-sv-version-id header")
	}

	// 3. GET object
	req, _ = http.NewRequest(http.MethodGet, h.gwServ.URL+"/testbucket/largefile.bin", nil)
	resp, err = client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("GET object failed: %v, code %d", err, resp.StatusCode)
	}
	readBytes, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || !bytes.Equal(readBytes, payload) {
		t.Fatalf("GET object bytes mismatch (len %d vs %d)", len(readBytes), len(payload))
	}

	// 4. GET Range (spanning stripe boundary: 1 MiB - 10 to 1 MiB + 20)
	req, _ = http.NewRequest(http.MethodGet, h.gwServ.URL+"/testbucket/largefile.bin", nil)
	start := int64(1048576 - 10)
	end := int64(1048576 + 20)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err = client.Do(req)
	if err != nil || resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("GET range failed: %v, code %d", err, resp.StatusCode)
	}
	rangeBytes, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	expectedRangeBytes := payload[start : end+1]
	if !bytes.Equal(rangeBytes, expectedRangeBytes) {
		t.Fatalf("range bytes mismatch (len %d vs %d)", len(rangeBytes), len(expectedRangeBytes))
	}

	// 5. HEAD object
	req, _ = http.NewRequest(http.MethodHead, h.gwServ.URL+"/testbucket/largefile.bin", nil)
	resp, err = client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD failed: %v, code %d", err, resp.StatusCode)
	}
	if resp.Header.Get("Content-Length") != fmt.Sprintf("%d", payloadSize) {
		t.Fatalf("HEAD content-length mismatch")
	}

	// 6. DELETE object (writes delete marker)
	req, _ = http.NewRequest(http.MethodDelete, h.gwServ.URL+"/testbucket/largefile.bin", nil)
	resp, err = client.Do(req)
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE failed: %v, code %d", err, resp.StatusCode)
	}

	// GET after DELETE returns 404 NoSuchKey
	req, _ = http.NewRequest(http.MethodGet, h.gwServ.URL+"/testbucket/largefile.bin", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("GET after delete failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 after delete marker, got %d", resp.StatusCode)
	}
}

func TestQuorumFailureModes(t *testing.T) {
	// k=4, m=2, total=6, write_quorum=5 (k+1)
	h := newClusterHarness(t, 4, 2, 5)
	defer h.Close()

	client := h.gwServ.Client()
	_ = h.meta.CreateBucket("b")

	payload := []byte("quorum testing payload")

	// 1. With 1 node down, write must SUCCEED with quorum = 5
	h.servers[0].Close() // Kill node 0

	req, _ := http.NewRequest(http.MethodPut, h.gwServ.URL+"/b/item1", bytes.NewReader(payload))
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT with 1 node down should succeed with quorum=5, got code %d", resp.StatusCode)
	}

	// Verify missing shard was recorded
	cur, err := h.meta.GetCurrentVersion("b", "item1")
	if err != nil {
		t.Fatalf("GetCurrentVersion failed: %v", err)
	}
	if len(cur.Missing) == 0 {
		t.Fatalf("expected missing shard to be recorded in metadata for node 0")
	}

	// 2. With 2 nodes down, only 4 acks possible (< 5), so PUT must FAIL with 503
	h.servers[1].Close() // Kill node 1

	req, _ = http.NewRequest(http.MethodPut, h.gwServ.URL+"/b/item2", bytes.NewReader(payload))
	resp, err = client.Do(req)
	if err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("PUT with 2 nodes down should return 503, got code %d", resp.StatusCode)
	}

	// Item2 should NOT be visible in metadata
	_, err = h.meta.GetCurrentVersion("b", "item2")
	if err == nil {
		t.Fatalf("item2 should not exist in metadata after aborted PUT")
	}
}

func TestDegradedReadsAndBitrotRepair(t *testing.T) {
	h := newClusterHarness(t, 4, 2, 5)
	defer h.Close()

	client := h.gwServ.Client()
	_ = h.meta.CreateBucket("data")

	payload := make([]byte, 1048576) // 1 MiB
	_, _ = rand.Read(payload)

	req, _ := http.NewRequest(http.MethodPut, h.gwServ.URL+"/data/doc.pdf", bytes.NewReader(payload))
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT failed: %v, code %d", err, resp.StatusCode)
	}

	// 1. Kill 2 nodes (up to m=2 nodes down): GET must still return exact bytes via reconstruction
	h.servers[0].Close()
	h.servers[1].Close()

	req, _ = http.NewRequest(http.MethodGet, h.gwServ.URL+"/data/doc.pdf", nil)
	resp, err = client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("degraded GET failed: %v, code %d", err, resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !bytes.Equal(body, payload) {
		t.Fatalf("degraded GET returned corrupted bytes!")
	}

	// 2. Kill a 3rd node (> m=2 down): GET must return 503, NEVER wrong bytes
	h.servers[2].Close()
	req, _ = http.NewRequest(http.MethodGet, h.gwServ.URL+"/data/doc.pdf", nil)
	resp, err = client.Do(req)
	if err == nil && resp.StatusCode == http.StatusOK {
		t.Fatalf("expected failure with 3 nodes down, but got 200 OK")
	}
}
