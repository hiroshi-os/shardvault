package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/hiroshi-os/shardvault/internal/node"
)

func main() {
	addr := flag.String("addr", ":8081", "Listen address (or env SV_LISTEN_ADDR)")
	dataDir := flag.String("data-dir", "./data/node", "Data directory (or env SV_DATA_DIR)")
	slowRate := flag.Float64("slow-rate", 0, "Probability [0, 1] of slow response")
	slowExtra := flag.Duration("slow-extra", 0, "Extra delay added to slow responses")
	failRate := flag.Float64("fail-rate", 0, "Probability [0, 1] of returning 500 error")
	flag.Parse()

	if envAddr := os.Getenv("SV_LISTEN_ADDR"); envAddr != "" {
		*addr = envAddr
	}
	if envDataDir := os.Getenv("SV_DATA_DIR"); envDataDir != "" {
		*dataDir = envDataDir
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := node.Config{
		DataDir:   *dataDir,
		SlowRate:  *slowRate,
		SlowExtra: *slowExtra,
		FailRate:  *failRate,
	}

	n, err := node.New(cfg)
	if err != nil {
		slog.Error("failed to initialize storage node", "error", err)
		os.Exit(1)
	}

	server := &http.Server{
		Addr:         *addr,
		Handler:      n.Handler(),
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
	}

	slog.Info("starting storagenode", "addr", *addr, "dataDir", *dataDir)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("storagenode server failed", "error", err)
		os.Exit(1)
	}
}
