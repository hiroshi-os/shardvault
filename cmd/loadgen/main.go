package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type BenchmarkResult struct {
	Op          string
	SizeStr     string
	SizeBytes   int64
	Concurrency int
	Duration    time.Duration
	TotalOps    int64
	TotalBytes  int64
	OpsPerSec   float64
	MBPerSec    float64
	P50         time.Duration
	P90         time.Duration
	P99         time.Duration
}

func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	multiplier := int64(1)
	if strings.HasSuffix(s, "KIB") || strings.HasSuffix(s, "KB") {
		multiplier = 1024
		s = strings.TrimSuffix(strings.TrimSuffix(s, "KIB"), "KB")
	} else if strings.HasSuffix(s, "MIB") || strings.HasSuffix(s, "MB") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(strings.TrimSuffix(s, "MIB"), "MB")
	} else if strings.HasSuffix(s, "GIB") || strings.HasSuffix(s, "GB") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(strings.TrimSuffix(s, "GIB"), "GB")
	} else if strings.HasSuffix(s, "B") {
		s = strings.TrimSuffix(s, "B")
	}
	val, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, err
	}
	return val * multiplier, nil
}

func main() {
	opFlag := flag.String("op", "put,get", "Operations to run: put, get, or put,get")
	sizeFlag := flag.String("size", "64KiB,1MiB,16MiB", "Comma-separated sizes (e.g., 64KiB,1MiB,16MiB)")
	concurrency := flag.Int("c", 16, "Concurrency (number of worker goroutines)")
	duration := flag.Duration("d", 30*time.Second, "Duration for each benchmark run")
	seed := flag.Int64("seed", 1, "Random seed")
	gwURL := flag.String("gateway", "http://127.0.0.1:8080", "Gateway URL")
	bucket := flag.String("bucket", "loadgen-bucket", "Bucket name")
	nodesFlag := flag.String("nodes", "", "Comma-separated storage node URLs for fault injection")
	degraded := flag.Bool("degraded", false, "Simulate degraded reads (2 nodes down)")
	slowDelay := flag.Duration("slow", 0, "Slow 1 node by duration (e.g. 500ms)")
	noHedge := flag.Bool("no-hedge", false, "Disable hedging for hedging comparison")
	flag.Parse()

	fmt.Printf("=== ShardVault Load Generator ===\n")
	fmt.Printf("Gateway: %s | Bucket: %s | Concurrency: %d | Duration: %v | Seed: %d\n",
		*gwURL, *bucket, *concurrency, *duration, *seed)
	if *degraded {
		fmt.Printf("Degraded mode: 2 nodes taken offline\n")
	}
	if *slowDelay > 0 {
		fmt.Printf("Slow node mode: Node 0 delayed by %v (no-hedge: %v)\n", *slowDelay, *noHedge)
	}
	fmt.Println()

	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        2000,
			MaxIdleConnsPerHost: 500,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// 1. Ensure bucket exists
	bReq, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s", *gwURL, *bucket), nil)
	bResp, err := client.Do(bReq)
	if err == nil {
		_ = bResp.Body.Close()
	}

	// Parse sizes
	sizeStrings := strings.Split(*sizeFlag, ",")
	var sizes []int64
	for _, ss := range sizeStrings {
		sz, err := parseByteSize(ss)
		if err != nil {
			fmt.Printf("Error parsing size '%s': %v\n", ss, err)
			return
		}
		sizes = append(sizes, sz)
	}

	// Setup node faults if requested
	nodes := strings.Split(*nodesFlag, ",")
	if *degraded && len(nodes) >= 2 {
		setNodeFault(client, nodes[0], true)
		setNodeFault(client, nodes[1], true)
		defer func() {
			setNodeFault(client, nodes[0], false)
			setNodeFault(client, nodes[1], false)
		}()
	}
	if *slowDelay > 0 && len(nodes) >= 1 && nodes[0] != "" {
		setNodeDelay(client, nodes[0], *slowDelay)
		defer setNodeDelay(client, nodes[0], 0)
	}

	var results []BenchmarkResult

	ops := strings.Split(*opFlag, ",")
	for _, op := range ops {
		op = strings.TrimSpace(strings.ToLower(op))
		for i, sz := range sizes {
			szStr := sizeStrings[i]
			fmt.Printf("Running %s benchmark for size %s (%d bytes)...\n", strings.ToUpper(op), szStr, sz)

			// Pre-warm data for GET if needed
			if op == "get" {
				preWarmObjects(client, *gwURL, *bucket, sz, 50)
			}

			res := runBenchmark(client, *gwURL, *bucket, op, szStr, sz, *concurrency, *duration, *noHedge)
			results = append(results, res)

			fmt.Printf("Result: %s %s -> %.2f MB/s | %.1f ops/s | p50: %v | p99: %v\n\n",
				strings.ToUpper(op), szStr, res.MBPerSec, res.OpsPerSec, res.P50.Round(time.Microsecond), res.P99.Round(time.Microsecond))
		}
	}

	// Print Markdown summary table
	fmt.Printf("\n### Benchmark Summary Table\n")
	fmt.Printf("| Operation | Size | Concurrency | Throughput (MB/s) | IOPS (ops/s) | p50 Latency | p99 Latency |\n")
	fmt.Printf("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
	for _, r := range results {
		fmt.Printf("| %s | %s | %d | %.2f MB/s | %.1f ops/s | %v | %v |\n",
			strings.ToUpper(r.Op), r.SizeStr, r.Concurrency, r.MBPerSec, r.OpsPerSec, r.P50.Round(time.Microsecond), r.P99.Round(time.Microsecond))
	}
}

func preWarmObjects(client *http.Client, gwURL, bucket string, size int64, count int) {
	fmt.Printf("  Pre-warming %d objects of size %d bytes for GET benchmark...\n", count, size)
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for i := 0; i < count; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()
			key := fmt.Sprintf("prewarm_%d_%04d.bin", size, idx)
			req, _ := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/%s/%s", gwURL, bucket, key), bytes.NewReader(data))
			resp, err := client.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
		}(i)
	}
	wg.Wait()
}

func runBenchmark(
	client *http.Client,
	gwURL, bucket, op, sizeStr string,
	size int64,
	concurrency int,
	duration time.Duration,
	noHedge bool,
) BenchmarkResult {
	// Pre-create payload buffer for writes
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte((i * 17) % 256)
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var totalOps atomic.Int64
	var totalBytes atomic.Int64
	var totalErrors atomic.Int64

	workerLatencies := make([][]time.Duration, concurrency)

	var wg sync.WaitGroup
	startTime := time.Now()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var lats []time.Duration
			reqIdx := 0

			for {
				select {
				case <-ctx.Done():
					workerLatencies[workerID] = lats
					return
				default:
				}

				reqIdx++
				t0 := time.Now()

				if op == "put" {
					key := fmt.Sprintf("bench_%d_w%d_%06d.bin", size, workerID, reqIdx)
					url := fmt.Sprintf("%s/%s/%s", gwURL, bucket, key)
					req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(payload))
					if err != nil {
						totalErrors.Add(1)
						continue
					}
					resp, err := client.Do(req)
					if err != nil {
						totalErrors.Add(1)
						continue
					}
					_ = resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						dur := time.Since(t0)
						lats = append(lats, dur)
						totalOps.Add(1)
						totalBytes.Add(size)
					} else {
						totalErrors.Add(1)
					}
				} else if op == "get" {
					// Round-robin over prewarmed keys (50 keys)
					keyIdx := (workerID*1000 + reqIdx) % 50
					key := fmt.Sprintf("prewarm_%d_%04d.bin", size, keyIdx)
					url := fmt.Sprintf("%s/%s/%s", gwURL, bucket, key)
					req, err := http.NewRequest(http.MethodGet, url, nil)
					if err != nil {
						totalErrors.Add(1)
						continue
					}
					if noHedge {
						req.Header.Set("x-sv-no-hedge", "1")
					}
					resp, err := client.Do(req)
					if err != nil {
						totalErrors.Add(1)
						continue
					}
					n, _ := io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						dur := time.Since(t0)
						lats = append(lats, dur)
						totalOps.Add(1)
						totalBytes.Add(n)
					} else {
						totalErrors.Add(1)
					}
				}
			}
		}(w)
	}

	wg.Wait()
	actualDuration := time.Since(startTime)

	var allLats []time.Duration
	for _, l := range workerLatencies {
		allLats = append(allLats, l...)
	}

	var p50, p90, p99 time.Duration
	if len(allLats) > 0 {
		sort.Slice(allLats, func(i, j int) bool { return allLats[i] < allLats[j] })
		p50 = allLats[len(allLats)*50/100]
		p90 = allLats[len(allLats)*90/100]
		p99 = allLats[len(allLats)*99/100]
	}

	opsCount := totalOps.Load()
	bytesCount := totalBytes.Load()
	mbCount := float64(bytesCount) / (1024 * 1024)
	sec := actualDuration.Seconds()

	return BenchmarkResult{
		Op:          op,
		SizeStr:     sizeStr,
		SizeBytes:   size,
		Concurrency: concurrency,
		Duration:    actualDuration,
		TotalOps:    opsCount,
		TotalBytes:  bytesCount,
		OpsPerSec:   float64(opsCount) / sec,
		MBPerSec:    mbCount / sec,
		P50:         p50,
		P90:         p90,
		P99:         p99,
	}
}

func setNodeFault(client *http.Client, nodeURL string, down bool) {
	if nodeURL == "" {
		return
	}
	status := "online"
	if down {
		status = "offline"
	}
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/admin/fault?status=%s", nodeURL, status), nil)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
}

func setNodeDelay(client *http.Client, nodeURL string, delay time.Duration) {
	if nodeURL == "" {
		return
	}
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/admin/fault?slow_extra=%s", nodeURL, delay.String()), nil)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
}
