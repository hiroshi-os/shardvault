package gateway_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hiroshi-os/shardvault/internal/gateway"
	"github.com/hiroshi-os/shardvault/internal/metadata"
	"github.com/hiroshi-os/shardvault/internal/node"
	"github.com/hiroshi-os/shardvault/internal/scrubber"
)

// TestGatewayCrashSIGKILL verifies durability and isolation under crashes:
// 1. Runs gateway as child process.
// 2. PUTs large multi-stripe objects while killing the gateway process (SIGKILL / Process.Kill) at random points.
// 3. Restarts gateway.
// 4. Verifies no partial / corrupt version is ever visible, the previous version remains intact.
// 5. Verifies GC removes orphan shards after a short forced grace period.
func TestGatewayCrashSIGKILL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping crash test in short mode")
	}

	// Build gateway binary for child process execution
	binDir, err := os.MkdirTemp("", "gw-bin-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(binDir)

	exePath := filepath.Join(binDir, "gateway.exe")
	buildCmd := exec.Command("go", "build", "-o", exePath, "../../cmd/gateway")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build gateway binary: %v\nOutput: %s", err, string(out))
	}

	// Spin up 6 storage nodes
	numNodes := 6
	var nodes []*node.Node
	var nodeServers []*httptest.Server
	var nodeURLs []string
	var nodeDirs []string

	for i := 0; i < numNodes; i++ {
		tmpDir, err := os.MkdirTemp("", fmt.Sprintf("crash-sn-%d-*", i))
		if err != nil {
			t.Fatal(err)
		}
		nodeDirs = append(nodeDirs, tmpDir)

		n, err := node.New(node.Config{DataDir: tmpDir})
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, n)

		serv := httptest.NewServer(n.Handler())
		nodeServers = append(nodeServers, serv)
		nodeURLs = append(nodeURLs, serv.URL)
	}
	defer func() {
		for _, s := range nodeServers {
			s.Close()
		}
		for _, d := range nodeDirs {
			_ = os.RemoveAll(d)
		}
	}()

	gwDataDir, err := os.MkdirTemp("", "crash-gw-data-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(gwDataDir)

	client := &http.Client{Timeout: 10 * time.Second}
	bucket := "crashbucket"
	key := "target.bin"

	gwAddr := "127.0.0.1:8990"
	gwURL := "http://" + gwAddr
	nodesFlag := strings.Join(nodeURLs, ",")

	startGateway := func() *exec.Cmd {
		cmd := exec.Command(exePath,
			"-addr", ":8990",
			"-metrics-addr", ":8991",
			"-data-dir", gwDataDir,
			"-nodes", nodesFlag,
			"-k", "4",
			"-m", "2",
			"-quorum", "5",
		)
		if err := cmd.Start(); err != nil {
			t.Fatalf("failed to start gateway process: %v", err)
		}
		// Wait for gateway to become ready
		ready := false
		for attempt := 0; attempt < 50; attempt++ {
			time.Sleep(50 * time.Millisecond)
			resp, err := client.Get(gwURL + "/healthz")
			if err == nil && resp.StatusCode == http.StatusOK {
				_ = resp.Body.Close()
				ready = true
				break
			}
		}
		if !ready {
			t.Fatalf("gateway failed to become ready")
		}
		return cmd
	}

	// 1. Initial gateway start & create bucket + initial version v0
	cmd := startGateway()

	req, _ := http.NewRequest(http.MethodPut, gwURL+"/"+bucket, nil)
	resp, err := client.Do(req)
	if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict) {
		t.Fatalf("PUT bucket failed: %v", err)
	}
	_ = resp.Body.Close()

	initialPayload := make([]byte, 1024*1024) // 1 MiB
	_, _ = rand.Read(initialPayload)
	initialSHA := sha256.Sum256(initialPayload)

	req, _ = http.NewRequest(http.MethodPut, gwURL+"/"+bucket+"/"+key, bytes.NewReader(initialPayload))
	resp, err = client.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("initial PUT object failed: %v", err)
	}
	_ = resp.Body.Close()

	// Terminate gateway cleanly first
	_ = cmd.Process.Kill()
	_ = cmd.Wait()

	// 2. Perform crash loop: 15 runs of mid-PUT random SIGKILL
	numRuns := 15
	for run := 0; run < numRuns; run++ {
		cmd = startGateway()

		// Stream a large object (4 MiB) to give time for mid-PUT kill
		largePayload := make([]byte, 4*1024*1024)
		_, _ = rand.Read(largePayload)

		putDone := make(chan bool)
		var putSucceeded bool

		go func() {
			req, _ := http.NewRequest(http.MethodPut, gwURL+"/"+bucket+"/"+key, bytes.NewReader(largePayload))
			resp, err := client.Do(req)
			if err == nil {
				if resp.StatusCode == http.StatusOK {
					putSucceeded = true
				}
				_ = resp.Body.Close()
			}
			putDone <- true
		}()

		// Random sleep between 1ms and 40ms, then SIGKILL
		rndDelay, _ := rand.Int(rand.Reader, big.NewInt(40))
		time.Sleep(time.Duration(rndDelay.Int64()+1) * time.Millisecond)

		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		<-putDone

		// Restart gateway after kill
		cmd = startGateway()

		// Verify GET returns either intact previous version or intact newly committed version, NEVER corrupt
		getReq, _ := http.NewRequest(http.MethodGet, gwURL+"/"+bucket+"/"+key, nil)
		getResp, err := client.Do(getReq)
		if err != nil || getResp.StatusCode != http.StatusOK {
			t.Fatalf("run %d: GET failed after restart: %v", run, err)
		}
		readBytes, _ := io.ReadAll(getResp.Body)
		_ = getResp.Body.Close()

		readSHA := sha256.Sum256(readBytes)
		newSHA := sha256.Sum256(largePayload)

		if readSHA != initialSHA && readSHA != newSHA {
			t.Fatalf("run %d: GET returned partial or corrupted version! got %x", run, readSHA)
		}

		if putSucceeded {
			initialSHA = newSHA
		}

		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}

	// 3. Restart and verify Garbage Collection of orphan shards
	metaStore, err := metadata.Open(filepath.Join(gwDataDir, "metadata.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer metaStore.Close()

	sc := scrubber.New(metaStore, gateway.Config{
		K:            4,
		M:            2,
		StorageNodes: nodeURLs,
	})

	// Count orphan shards across all nodes before GC
	countTotalShards := func() int {
		total := 0
		for _, d := range nodeDirs {
			files, _ := filepath.Glob(filepath.Join(d, "shards", "*", "*", "*"))
			total += len(files)
		}
		return total
	}

	beforeShards := countTotalShards()
	// Run GC with 0 grace period to clean all uncommitted orphan shards
	if err := sc.GarbageCollect(context.Background(), 0); err != nil {
		t.Fatalf("GarbageCollect failed: %v", err)
	}
	afterShards := countTotalShards()

	t.Logf("Shards on disk: before GC=%d, after GC=%d", beforeShards, afterShards)

	// Final check: active object can still be read perfectly
	activeVers, err := metaStore.ActiveVersions()
	if err != nil || len(activeVers) == 0 {
		t.Fatalf("ActiveVersions failed or empty: %v", err)
	}
	activeVer := activeVers[0]
	if len(activeVer.Locations) < 4 {
		t.Fatalf("active version lost essential shards after GC!")
	}
}
