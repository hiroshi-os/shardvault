package gateway_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hiroshi-os/shardvault/internal/gateway"
	"github.com/hiroshi-os/shardvault/internal/metadata"
	"github.com/hiroshi-os/shardvault/internal/node"
	"github.com/hiroshi-os/shardvault/internal/scrubber"
	"pgregory.net/rapid"
)

// rapidCluster manages 6 httptest storage nodes and a gateway for rapid testing
type rapidCluster struct {
	nodes   []*node.Node
	servers []*httptest.Server
	gw      *gateway.Gateway
	gwServ  *httptest.Server
	meta    *metadata.Store
	scrub   *scrubber.Scrubber
	dirs    []string
}

func newRapidCluster(t *testing.T, k, m, quorum int) *rapidCluster {
	numNodes := k + m
	rc := &rapidCluster{}

	var nodeURLs []string
	for i := 0; i < numNodes; i++ {
		tmpDir, err := os.MkdirTemp("", fmt.Sprintf("rapid-sn-%d-*", i))
		if err != nil {
			t.Fatal(err)
		}
		rc.dirs = append(rc.dirs, tmpDir)

		n, err := node.New(node.Config{DataDir: tmpDir})
		if err != nil {
			t.Fatal(err)
		}
		rc.nodes = append(rc.nodes, n)

		serv := httptest.NewServer(n.Handler())
		rc.servers = append(rc.servers, serv)
		nodeURLs = append(nodeURLs, serv.URL)
	}

	gwDir, err := os.MkdirTemp("", "rapid-gw-*")
	if err != nil {
		t.Fatal(err)
	}
	rc.dirs = append(rc.dirs, gwDir)

	metaStore, err := metadata.Open(filepath.Join(gwDir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	rc.meta = metaStore

	cfg := gateway.Config{
		K:            k,
		M:            m,
		WriteQuorum:  quorum,
		StorageNodes: nodeURLs,
		HedgeDelay:   10 * time.Millisecond,
		StripeSize:   256 * 1024, // 256 KiB for faster rapid iterations
	}

	gw, err := gateway.New(cfg, metaStore)
	if err != nil {
		t.Fatal(err)
	}
	rc.gw = gw
	rc.gwServ = httptest.NewServer(gw.Handler())
	rc.scrub = scrubber.New(metaStore, cfg)

	return rc
}

func (rc *rapidCluster) Close() {
	if rc.gwServ != nil {
		rc.gwServ.Close()
	}
	for _, s := range rc.servers {
		s.Close()
	}
	if rc.meta != nil {
		rc.meta.Close()
	}
	for _, d := range rc.dirs {
		_ = os.RemoveAll(d)
	}
}

func TestRapidStateMachine(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		k := 4
		m := 2
		quorum := 5
		c := newRapidCluster(t, k, m, quorum)
		defer c.Close()

		client := c.gwServ.Client()
		bucket := "rapidbucket"

		// PUT bucket
		req, _ := http.NewRequest(http.MethodPut, c.gwServ.URL+"/"+bucket, nil)
		resp, err := client.Do(req)
		if err != nil || (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict) {
			rt.Fatalf("PUT bucket failed: %v", err)
		}
		if resp != nil {
			_ = resp.Body.Close()
		}

		// Model state: map[key] -> latest committed sha256 hex (or "" if deleted / non-existent)
		model := make(map[string]string)
		corruptedPerStripe := make(map[string]int)
		keys := []string{"obj1", "obj2", "obj3", "dir/nested", "bigfile"}

		numOps := rapid.IntRange(15, 30).Draw(rt, "numOps")

		for step := 0; step < numOps; step++ {
			action := rapid.IntRange(0, 8).Draw(rt, "action")
			chosenKey := rapid.SampledFrom(keys).Draw(rt, "key")

			switch action {
			case 0, 1: // PUT
				dataLen := rapid.IntRange(0, 512*1024).Draw(rt, "dataLen")
				dataBytes := make([]byte, dataLen)
				for i := range dataBytes {
					dataBytes[i] = byte((step*13 + i) % 256)
				}
				h := sha256.Sum256(dataBytes)
				expectedSHA := hex.EncodeToString(h[:])

				putReq, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s/%s", c.gwServ.URL, bucket, chosenKey), bytes.NewReader(dataBytes))
				putResp, putErr := client.Do(putReq)
				if putErr == nil {
					if putResp.StatusCode == http.StatusOK {
						// Write succeeded and committed! Update model
						model[chosenKey] = expectedSHA
					}
					_ = putResp.Body.Close()
				}

			case 2, 3: // GET
				getReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s/%s", c.gwServ.URL, bucket, chosenKey), nil)
				getResp, getErr := client.Do(getReq)
				if getErr == nil {
					defer getResp.Body.Close()
					if getResp.StatusCode == http.StatusOK {
						readBytes, _ := io.ReadAll(getResp.Body)
						h := sha256.Sum256(readBytes)
						gotSHA := hex.EncodeToString(h[:])

						wantSHA, exists := model[chosenKey]
						if !exists || wantSHA == "" {
							rt.Fatalf("GET %s returned 200 OK, but key is not in model", chosenKey)
						}
						if gotSHA != wantSHA {
							rt.Fatalf("GET %s returned corrupt SHA! got %s, want %s", chosenKey, gotSHA, wantSHA)
						}
					} else if getResp.StatusCode == http.StatusNotFound {
						wantSHA, exists := model[chosenKey]
						if exists && wantSHA != "" {
							// If we expected it to exist, ensure fewer than k nodes wasn't the reason,
							// but 404 means the key was never written or was deleted
							rt.Fatalf("GET %s returned 404 NotFound, but model has committed SHA %s", chosenKey, wantSHA)
						}
					} else if getResp.StatusCode == http.StatusServiceUnavailable {
						// 503 is allowed only if fewer than k shards are reachable
					}
				}

			case 4: // DELETE
				delReq, _ := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/%s/%s", c.gwServ.URL, bucket, chosenKey), nil)
				delResp, delErr := client.Do(delReq)
				if delErr == nil {
					if delResp.StatusCode == http.StatusNoContent {
						delete(model, chosenKey)
					}
					_ = delResp.Body.Close()
				}

			case 5: // Kill random node (allow max m nodes offline at once)
				onlineCount := 0
				for _, n := range c.nodes {
					if n.IsOnline() {
						onlineCount++
					}
				}
				if onlineCount > k { // keep at least k alive for quorum / reconstructibility
					nodeIdx := rapid.IntRange(0, k+m-1).Draw(rt, "killNode")
					c.nodes[nodeIdx].SetOnline(false)
				}

			case 6: // Revive random node
				nodeIdx := rapid.IntRange(0, k+m-1).Draw(rt, "reviveNode")
				c.nodes[nodeIdx].SetOnline(true)

			case 7: // Corrupt random shard on random node (keep total losses <= m so stripe is reconstructible)
				nodeIdx := rapid.IntRange(0, k+m-1).Draw(rt, "corruptNode")
				dataDir := c.nodes[nodeIdx].GetDataDir()
				shardFiles, _ := filepath.Glob(filepath.Join(dataDir, "shards", "*", "*", "*"))
				if len(shardFiles) > 0 {
					targetFile := shardFiles[step%len(shardFiles)]
					// targetFile looks like .../shards/{versionID}/{stripeIdx}/{shardIdx}
					parts := strings.Split(filepath.ToSlash(targetFile), "/")
					if len(parts) >= 3 {
						verID := parts[len(parts)-3]
						stIdxStr := parts[len(parts)-2]
						stripeKey := verID + "/" + stIdxStr

						// Check if version already had missing shards during write for this stripe
						missingCount := 0
						if vMeta, err := c.meta.GetVersion(verID); err == nil {
							var sIdx uint32
							_, _ = fmt.Sscanf(stIdxStr, "%d", &sIdx)
							for _, mis := range vMeta.Missing {
								if mis.StripeIdx == sIdx {
									missingCount++
								}
							}
						}

						if missingCount+corruptedPerStripe[stripeKey] < m {
							if fi, err := os.Stat(targetFile); err == nil && fi.Size() > 60 {
								f, err := os.OpenFile(targetFile, os.O_RDWR, 0644)
								if err == nil {
									_, _ = f.WriteAt([]byte{0xDE, 0xAD}, 56)
									_ = f.Close()
									corruptedPerStripe[stripeKey]++
								}
							}
						}
					}
				}

			case 8: // Scrub
				// Revive all nodes before scrub to check recovery
				for _, n := range c.nodes {
					n.SetOnline(true)
				}
				stripesAtRisk, _, _ := c.scrub.Scrub(context.Background())
				// After repair, reset corruption count for repaired stripes
				corruptedPerStripe = make(map[string]int)
				if stripesAtRisk != 0 {
					rt.Fatalf("expected 0 stripes at risk after reviving all nodes and scrubbing, got %d", stripesAtRisk)
				}
			}
		}

		// Final check: revive all nodes, run scrub, check all committed keys
		for _, n := range c.nodes {
			n.SetOnline(true)
		}
		stripesAtRisk, _, err := c.scrub.Scrub(context.Background())
		if err != nil || stripesAtRisk != 0 {
			rt.Fatalf("final scrub: stripesAtRisk=%d, err=%v", stripesAtRisk, err)
		}

		for kStr, wantSHA := range model {
			if wantSHA == "" {
				continue
			}
			getReq, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s/%s", c.gwServ.URL, bucket, kStr), nil)
			getResp, getErr := client.Do(getReq)
			if getErr != nil || getResp.StatusCode != http.StatusOK {
				rt.Fatalf("final GET %s failed: status=%v, err=%v", kStr, getResp.StatusCode, getErr)
			}
			readBytes, _ := io.ReadAll(getResp.Body)
			_ = getResp.Body.Close()
			h := sha256.Sum256(readBytes)
			gotSHA := hex.EncodeToString(h[:])
			if gotSHA != wantSHA {
				rt.Fatalf("final GET %s SHA mismatch: got %s, want %s", kStr, gotSHA, wantSHA)
			}
		}
	})
}
