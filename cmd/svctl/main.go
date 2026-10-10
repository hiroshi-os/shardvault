package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hiroshi-os/shardvault/internal/gateway"
	"github.com/hiroshi-os/shardvault/internal/metadata"
	"github.com/hiroshi-os/shardvault/internal/scrubber"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	switch cmd {
	case "status":
		runStatus(os.Args[2:])
	case "scrub":
		runScrub(os.Args[2:])
	case "gc":
		runGC(os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: svctl <command> [options]")
	fmt.Println("Commands:")
	fmt.Println("  status            Show node health and redundancy status")
	fmt.Println("  scrub --now       Trigger immediate scrub and repair")
	fmt.Println("  gc --grace <dur>  Trigger garbage collection of orphan shards")
}

func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	nodesFlag := fs.String("nodes", "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083,http://127.0.0.1:8084,http://127.0.0.1:8085,http://127.0.0.1:8086", "Storage node addresses")
	_ = fs.Parse(args)

	fmt.Println("=== Storage Nodes Status ===")
	client := &http.Client{Timeout: 3 * time.Second}
	var healthyNodes int
	nodesList := parseNodes(*nodesFlag)
	for _, n := range nodesList {
		resp, err := client.Get(n + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			fmt.Printf("  [UP]   %s\n", n)
			healthyNodes++
			_ = resp.Body.Close()
		} else {
			fmt.Printf("  [DOWN] %s\n", n)
		}
	}
	fmt.Printf("\nRedundancy: %d/%d nodes healthy\n", healthyNodes, len(nodesList))
}

func runScrub(args []string) {
	fs := flag.NewFlagSet("scrub", flag.ExitOnError)
	nowFlag := fs.Bool("now", false, "Run scrub immediately")
	dataDir := fs.String("data-dir", "./data/gateway", "Gateway data directory")
	nodesFlag := fs.String("nodes", "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083,http://127.0.0.1:8084,http://127.0.0.1:8085,http://127.0.0.1:8086", "Storage nodes")
	_ = fs.Parse(args)

	if !*nowFlag {
		fmt.Println("Use --now to trigger immediate scrub")
		return
	}

	metaDBPath := filepath.Join(*dataDir, "metadata.db")
	store, err := metadata.Open(metaDBPath)
	if err != nil {
		fmt.Printf("failed to open metadata store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	cfg := gateway.Config{
		K:            4,
		M:            2,
		StorageNodes: parseNodes(*nodesFlag),
	}
	sc := scrubber.New(store, cfg)
	fmt.Println("Running scrub and repair...")
	start := time.Now()
	atRisk, repaired, err := sc.Scrub(context.Background())
	if err != nil {
		fmt.Printf("scrub failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Scrub completed in %v. Repaired: %d shards, Stripes at risk remaining: %d\n", time.Since(start), repaired, atRisk)
}

func runGC(args []string) {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	dataDir := fs.String("data-dir", "./data/gateway", "Gateway data directory")
	nodesFlag := fs.String("nodes", "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083,http://127.0.0.1:8084,http://127.0.0.1:8085,http://127.0.0.1:8086", "Storage nodes")
	grace := fs.Duration("grace", 10*time.Minute, "Grace period for orphan shards")
	_ = fs.Parse(args)

	metaDBPath := filepath.Join(*dataDir, "metadata.db")
	store, err := metadata.Open(metaDBPath)
	if err != nil {
		fmt.Printf("failed to open metadata store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	cfg := gateway.Config{
		K:            4,
		M:            2,
		StorageNodes: parseNodes(*nodesFlag),
	}
	sc := scrubber.New(store, cfg)
	fmt.Printf("Running garbage collection (grace period %v)...\n", *grace)
	if err := sc.GarbageCollect(context.Background(), *grace); err != nil {
		fmt.Printf("GC failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("GC completed.")
}

func parseNodes(s string) []string {
	var list []string
	for _, n := range strings.Split(s, ",") {
		n = strings.TrimSpace(n)
		if n != "" {
			list = append(list, n)
		}
	}
	return list
}
