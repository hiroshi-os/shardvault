package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hiroshi-os/shardvault/internal/gateway"
	"github.com/hiroshi-os/shardvault/internal/metadata"
	"github.com/hiroshi-os/shardvault/internal/scrubber"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	addr := flag.String("addr", ":8080", "Listen address for S3 API (or SV_LISTEN_ADDR)")
	metricsAddr := flag.String("metrics-addr", ":9090", "Listen address for Prometheus metrics (or SV_METRICS_ADDR)")
	dataDir := flag.String("data-dir", "./data/gateway", "Data directory (or SV_DATA_DIR)")
	nodesFlag := flag.String("nodes", "http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083,http://127.0.0.1:8084,http://127.0.0.1:8085,http://127.0.0.1:8086", "Comma-separated storage nodes (or SV_STORAGE_NODES)")
	kFlag := flag.Int("k", 4, "Reed-Solomon data shards (or SV_K)")
	mFlag := flag.Int("m", 2, "Reed-Solomon parity shards (or SV_M)")
	quorumFlag := flag.Int("quorum", 5, "Write quorum (or SV_WRITE_QUORUM)")
	hedgeDelayFlag := flag.Duration("hedge-delay", 20*time.Millisecond, "Hedge delay for parity fallback")
	scrubInterval := flag.Duration("scrub-interval", 5*time.Minute, "Interval between background scrubs")
	gcInterval := flag.Duration("gc-interval", 10*time.Minute, "Interval between garbage collection runs")
	disableVerify := flag.Bool("no-verify", false, "Disable CRC and stripe SHA verification (negative control)")
	flag.Parse()

	// Environment overrides
	if v := os.Getenv("SV_LISTEN_ADDR"); v != "" {
		*addr = v
	}
	if v := os.Getenv("SV_METRICS_ADDR"); v != "" {
		*metricsAddr = v
	}
	if v := os.Getenv("SV_DATA_DIR"); v != "" {
		*dataDir = v
	}
	if v := os.Getenv("SV_STORAGE_NODES"); v != "" {
		*nodesFlag = v
	}
	if v := os.Getenv("SV_K"); v != "" {
		if val, err := strconv.Atoi(v); err == nil {
			*kFlag = val
		}
	}
	if v := os.Getenv("SV_M"); v != "" {
		if val, err := strconv.Atoi(v); err == nil {
			*mFlag = val
		}
	}
	if v := os.Getenv("SV_WRITE_QUORUM"); v != "" {
		if val, err := strconv.Atoi(v); err == nil {
			*quorumFlag = val
		}
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		slog.Error("failed to create data dir", "err", err)
		os.Exit(1)
	}

	metaDBPath := filepath.Join(*dataDir, "metadata.db")
	metaStore, err := metadata.Open(metaDBPath)
	if err != nil {
		slog.Error("failed to open metadata store", "err", err)
		os.Exit(1)
	}
	defer metaStore.Close()

	var nodes []string
	for _, n := range strings.Split(*nodesFlag, ",") {
		n = strings.TrimSpace(n)
		if n != "" {
			nodes = append(nodes, n)
		}
	}

	gwCfg := gateway.Config{
		K:             *kFlag,
		M:             *mFlag,
		WriteQuorum:   *quorumFlag,
		StorageNodes:  nodes,
		HedgeDelay:    *hedgeDelayFlag,
		DisableVerify: *disableVerify,
	}

	gw, err := gateway.New(gwCfg, metaStore)
	if err != nil {
		slog.Error("failed to initialize gateway", "err", err)
		os.Exit(1)
	}

	// Initialize Scrubber and GC
	sc := scrubber.New(metaStore, gwCfg)
	gw.SetAdminOps(sc.Scrub, sc.GarbageCollect)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sc.StartBackground(ctx, *scrubInterval, *gcInterval)

	// Metrics HTTP server
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{Addr: *metricsAddr, Handler: metricsMux}
	go func() {
		slog.Info("starting metrics server", "addr", *metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("metrics server exited", "err", err)
		}
	}()

	// Gateway API server
	apiServer := &http.Server{Addr: *addr, Handler: gw.Handler()}
	go func() {
		slog.Info("starting gateway API server", "addr", *addr, "nodes", len(nodes), "quorum", *quorumFlag)
		if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("gateway API server failed", "err", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	slog.Info("shutting down gateway...")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = apiServer.Shutdown(shutdownCtx)
	_ = metricsServer.Shutdown(shutdownCtx)
	slog.Info("gateway exited cleanly")
}
