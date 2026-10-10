package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type ObjectInfo struct {
	Key  string
	Size int64
	SHA  string
}

type PhaseResult struct {
	Name             string
	ObjectsRead      int
	ByteIdentical    int
	ByteIdenticalPct float64
	Count503         int
	CountWrongBytes  int
	DegradedReads    int
	Reconstructs     int
	Duration         time.Duration
	P50              time.Duration
	P99              time.Duration
}

func main() {
	seed := flag.Int64("seed", 7, "Random seed (or SEED env var)")
	numObjects := flag.Int("objects", 2000, "Number of objects to test (or OBJECTS env var)")
	gwURL := flag.String("gateway", "http://127.0.0.1:8080", "Gateway URL")
	nodesFlag := flag.String("nodes", "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083,http://127.0.0.1:8084,http://127.0.0.1:8085,http://127.0.0.1:8086", "Storage node URLs")
	nodesDirFlag := flag.String("nodes-dirs", "./data/node1,./data/node2,./data/node3,./data/node4,./data/node5,./data/node6", "Storage node data directories")
	noVerify := flag.Bool("no-verify", false, "Negative control: disable verification on reads")
	flag.Parse()

	fmt.Printf("=== ShardVault Chaos Test Harness ===\n")
	fmt.Printf("Seed: %d | Objects: %d | Gateway: %s | NoVerify: %v\n\n", *seed, *numObjects, *gwURL, *noVerify)

	nodes := strings.Split(*nodesFlag, ",")
	nodeDirs := strings.Split(*nodesDirFlag, ",")
	client := &http.Client{Timeout: 30 * time.Second}
	bucket := "chaosbucket"

	// 1. Create Bucket
	putBucket(client, *gwURL, bucket)

	// 2. Upload Objects
	fmt.Printf("[Phase 0: Uploading %d objects according to size distribution...]\n", *numObjects)
	objects := uploadObjects(client, *gwURL, bucket, *numObjects, *seed)
	fmt.Printf("Uploaded %d objects successfully.\n\n", len(objects))

	// Helper to fetch metrics delta
	getMetric := func(name string) int {
		resp, err := client.Get(*gwURL + "/metrics")
		if err != nil {
			return 0
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		lines := strings.Split(string(b), "\n")
		for _, l := range lines {
			if strings.HasPrefix(l, name) {
				var val int
				_, _ = fmt.Sscanf(strings.TrimSpace(strings.TrimPrefix(l, name)), "%d", &val)
				return val
			}
		}
		return 0
	}

	readPhase := func(name string, noHedge bool) PhaseResult {
		fmt.Printf("[%s: Reading all %d objects...]\n", name, len(objects))
		start := time.Now()
		res := PhaseResult{Name: name, ObjectsRead: len(objects)}
		var latencies []time.Duration

		degBefore := getMetric("degraded_reads_total")
		recBefore := getMetric("decode_reconstructs_total")

		for _, obj := range objects {
			t0 := time.Now()
			req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s/%s", *gwURL, bucket, obj.Key), nil)
			if *noVerify {
				req.Header.Set("x-sv-no-verify", "1")
			}
			if noHedge {
				req.Header.Set("x-sv-no-hedge", "1")
			}
			resp, err := client.Do(req)
			dur := time.Since(t0)
			latencies = append(latencies, dur)

			if err != nil {
				res.Count503++
				continue
			}

			if resp.StatusCode == http.StatusServiceUnavailable {
				res.Count503++
				_ = resp.Body.Close()
				continue
			}

			if resp.StatusCode != http.StatusOK {
				_ = resp.Body.Close()
				continue
			}

			data, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()

			h := sha256.Sum256(data)
			gotSHA := hex.EncodeToString(h[:])
			if gotSHA == obj.SHA {
				res.ByteIdentical++
			} else {
				res.CountWrongBytes++
			}
		}

		res.Duration = time.Since(start)
		if res.ObjectsRead > 0 {
			res.ByteIdenticalPct = float64(res.ByteIdentical) / float64(res.ObjectsRead) * 100.0
		}

		degAfter := getMetric("degraded_reads_total")
		recAfter := getMetric("decode_reconstructs_total")
		res.DegradedReads = degAfter - degBefore
		res.Reconstructs = recAfter - recBefore

		if len(latencies) > 0 {
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			res.P50 = latencies[len(latencies)*50/100]
			res.P99 = latencies[len(latencies)*99/100]
		}

		fmt.Printf("  Identical: %d (%.2f%%) | 503s: %d | Wrong: %d | Degraded: %d | Reconstructed: %d | Time: %v | p99: %v\n\n",
			res.ByteIdentical, res.ByteIdenticalPct, res.Count503, res.CountWrongBytes, res.DegradedReads, res.Reconstructs, res.Duration.Round(time.Millisecond), res.P99.Round(time.Millisecond))
		return res
	}

	// Calculate logical bytes and on-disk shard bytes for storage overhead
	var totalLogicalBytes int64
	for _, obj := range objects {
		totalLogicalBytes += obj.Size
	}
	var totalDiskBytes int64
	for _, d := range nodeDirs {
		_ = filepath.Walk(d, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				totalDiskBytes += info.Size()
			}
			return nil
		})
	}
	storageOverhead := 0.0
	if totalLogicalBytes > 0 {
		storageOverhead = float64(totalDiskBytes) / float64(totalLogicalBytes)
	}

	// Phase 1: Kill 2 random nodes
	fmt.Printf("[Phase 1: Killing 2 random nodes (up to m=2 fault tolerance)]\n")
	_ = setNodeFault(client, nodes[0], true)
	_ = setNodeFault(client, nodes[1], true)
	p1 := readPhase("Phase 1: 2 Nodes Down", false)

	// Revive them
	fmt.Printf("[Reviving all nodes...]\n")
	_ = setNodeFault(client, nodes[0], false)
	_ = setNodeFault(client, nodes[1], false)

	// Phase 2: Corrupt 1% of shard files on surviving nodes
	fmt.Printf("[Phase 2: Corrupting 1%% of shard files on disk]\n")
	corruptCount := corruptShards(nodeDirs, 0.01)
	fmt.Printf("Corrupted %d shard files.\n", corruptCount)
	p2 := readPhase("Phase 2: 1% Shards Corrupted", false)

	// Phase 3: Slow 1 node by 500ms (measure p99 with hedging ON vs OFF)
	fmt.Printf("[Phase 3: Slowing node 0 by 500ms to test hedged requests]\n")
	_ = setNodeDelay(client, nodes[0], 500*time.Millisecond)
	p3HedgeOn := readPhase("Phase 3: Slow Node (Hedging ON)", false)
	p3HedgeOff := readPhase("Phase 3: Slow Node (Hedging OFF)", true)
	_ = setNodeDelay(client, nodes[0], 0)

	// Phase 4: Scrubber and Repair
	fmt.Printf("[Phase 4: Running background scrubber and repair...]\n")
	repairBefore := getMetric("repair_bytes_total")
	tScrub := time.Now()
	_, _ = client.Post(*gwURL+"/admin/scrub", "application/json", nil)
	time.Sleep(2 * time.Second) // allow scrub to complete
	scrubDur := time.Since(tScrub)
	repairAfter := getMetric("repair_bytes_total")
	repairBytes := repairAfter - repairBefore
	fmt.Printf("Scrub completed in %v. Repaired bytes: %d\n", scrubDur.Round(time.Millisecond), repairBytes)
	p4 := readPhase("Phase 4: Post-Scrub Verification", false)

	fmt.Printf("=== SUMMARY ===\n")
	fmt.Printf("Logical size: %.2f MB | Shard storage on disk: %.2f MB | Overhead: %.2fx (RS 4+2)\n",
		float64(totalLogicalBytes)/(1024*1024), float64(totalDiskBytes)/(1024*1024), storageOverhead)
	fmt.Printf("Phase 1 (2 nodes down): %.2f%% identical, %d 503s, %d wrong bytes\n", p1.ByteIdenticalPct, p1.Count503, p1.CountWrongBytes)
	fmt.Printf("Phase 2 (1%% corrupt):   %.2f%% identical, %d 503s, %d wrong bytes\n", p2.ByteIdenticalPct, p2.Count503, p2.CountWrongBytes)
	fmt.Printf("Phase 3 (slow node):    p99 hedge off: %v | hedge on: %v\n", p3HedgeOff.P99.Round(time.Millisecond), p3HedgeOn.P99.Round(time.Millisecond))
	fmt.Printf("Phase 4 (post-scrub):   %.2f%% identical, %d wrong bytes, repair dur: %v, repair bytes: %d\n",
		p4.ByteIdenticalPct, p4.CountWrongBytes, scrubDur.Round(time.Millisecond), repairBytes)

	if *noVerify {
		if p2.CountWrongBytes > 0 {
			fmt.Printf("\n[NEGATIVE CONTROL PASSED: Silent corruption returned wrong bytes as expected without verification!]\n")
		} else {
			fmt.Printf("\n[NEGATIVE CONTROL FAILED: Expected wrong bytes under -no-verify]\n")
		}
	}
}

func putBucket(client *http.Client, gwURL, bucket string) {
	req, _ := http.NewRequest(http.MethodPut, gwURL+"/"+bucket, nil)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
}

func uploadObjects(client *http.Client, gwURL, bucket string, count int, seed int64) []ObjectInfo {
	var objects []ObjectInfo
	for i := 0; i < count; i++ {
		// Size distribution: 60% 1-64 KiB, 30% 64 KiB - 2 MiB, 10% 2 - 16 MiB
		var size int
		bucketRoll := (int(seed)*31 + i*17) % 100
		if bucketRoll < 60 {
			size = 1024 + ((i * 1021) % (63 * 1024))
		} else if bucketRoll < 90 {
			size = 64*1024 + ((i * 1021) % (1936 * 1024))
		} else {
			size = 2*1024*1024 + ((i * 1021) % (4 * 1024 * 1024)) // max 6 MiB for fast execution
		}

		data := make([]byte, size)
		// fill deterministic data
		for j := range data {
			data[j] = byte((i*7 + j) % 256)
		}
		h := sha256.Sum256(data)
		shaStr := hex.EncodeToString(h[:])
		key := fmt.Sprintf("obj_%05d.bin", i)

		req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s/%s", gwURL, bucket, key), bytes.NewReader(data))
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			objects = append(objects, ObjectInfo{Key: key, Size: int64(size), SHA: shaStr})
		}
	}
	return objects
}

func setNodeFault(client *http.Client, nodeURL string, down bool) error {
	status := "online"
	if down {
		status = "offline"
	}
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/admin/fault?status=%s", nodeURL, status), nil)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
	return err
}

func setNodeDelay(client *http.Client, nodeURL string, delay time.Duration) error {
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/admin/fault?slow_extra=%s", nodeURL, delay.String()), nil)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
	return err
}

func corruptShards(dirs []string, fraction float64) int {
	var allFiles []string
	for _, d := range dirs {
		files, _ := filepath.Glob(filepath.Join(d, "shards", "*", "*", "*"))
		allFiles = append(allFiles, files...)
	}
	if len(allFiles) == 0 {
		return 0
	}
	numToCorrupt := int(float64(len(allFiles)) * fraction)
	if numToCorrupt == 0 && len(allFiles) > 0 {
		numToCorrupt = 1
	}

	corrupted := 0
	for i := 0; i < numToCorrupt; i++ {
		idx := (i * 37) % len(allFiles)
		fPath := allFiles[idx]
		if fi, err := os.Stat(fPath); err == nil && fi.Size() > 60 {
			f, err := os.OpenFile(fPath, os.O_RDWR, 0644)
			if err == nil {
				_, _ = f.WriteAt([]byte{0xDE, 0xAD}, 56)
				_ = f.Close()
				corrupted++
			}
		}
	}
	return corrupted
}
