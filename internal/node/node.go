package node

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hiroshi-os/shardvault/internal/shard"
)

// Config holds configuration for a storage node.
type Config struct {
	DataDir   string
	SlowRate  float64       // probability [0, 1] of delaying requests
	SlowExtra time.Duration // amount of extra delay
	FailRate  float64       // probability [0, 1] of returning 500 error
}

// Node represents a dumb shard store node.
type Node struct {
	cfg     Config
	mu      sync.RWMutex
	running atomic.Bool
}

// New creates a new storage node instance.
func New(cfg Config) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}
	// Also ensure tmp directory exists
	tmpDir := filepath.Join(cfg.DataDir, "tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create tmp dir: %w", err)
	}
	n := &Node{cfg: cfg}
	n.running.Store(true)
	return n, nil
}

// SetOnline enables or disables the node (simulating crash / shutdown).
func (n *Node) SetOnline(online bool) {
	n.running.Store(online)
}

// IsOnline returns true if the node is online.
func (n *Node) IsOnline() bool {
	return n.running.Load()
}

// GetDataDir returns the configured data directory.
func (n *Node) GetDataDir() string {
	return n.cfg.DataDir
}

// SetChaos dynamically updates chaos simulation parameters (useful for tests and harnesses).
func (n *Node) SetChaos(slowRate float64, slowExtra time.Duration, failRate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cfg.SlowRate = slowRate
	n.cfg.SlowExtra = slowExtra
	n.cfg.FailRate = failRate
}

func (n *Node) maybeInjectChaos(w http.ResponseWriter) bool {
	if !n.running.Load() {
		http.Error(w, "node offline", http.StatusServiceUnavailable)
		return true
	}
	n.mu.RLock()
	slowRate := n.cfg.SlowRate
	slowExtra := n.cfg.SlowExtra
	failRate := n.cfg.FailRate
	n.mu.RUnlock()

	if failRate > 0 {
		r, _ := rand.Int(rand.Reader, big.NewInt(10000))
		if float64(r.Int64())/10000.0 < failRate {
			http.Error(w, "chaos: injected failure", http.StatusInternalServerError)
			return true
		}
	}

	if slowRate > 0 && slowExtra > 0 {
		r, _ := rand.Int(rand.Reader, big.NewInt(10000))
		if float64(r.Int64())/10000.0 < slowRate {
			time.Sleep(slowExtra)
		}
	}
	return false
}

// Handler returns the HTTP handler for the storage node.
func (n *Node) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", n.handleHealthz)
	mux.HandleFunc("/shards/", n.handleShard)
	mux.HandleFunc("/shards", n.handleListShards)
	return mux
}

func (n *Node) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if n.maybeInjectChaos(w) {
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK\n"))
}

// idFromPath cleans and extracts shard ID from URL path "/shards/{id...}".
func idFromPath(p string) (string, error) {
	prefix := "/shards/"
	if !strings.HasPrefix(p, prefix) {
		return "", errors.New("invalid path")
	}
	id := strings.TrimPrefix(p, prefix)
	id = strings.Trim(id, "/")
	if id == "" || strings.Contains(id, "..") {
		return "", errors.New("invalid shard id")
	}
	return id, nil
}

// shardFilePath returns the on-disk path for a shard ID.
func (n *Node) shardFilePath(id string) string {
	parts := strings.Split(id, "/")
	return filepath.Join(append([]string{n.cfg.DataDir, "shards"}, parts...)...)
}

// handleShard routes GET, PUT, DELETE /shards/{id}
func (n *Node) handleShard(w http.ResponseWriter, r *http.Request) {
	if n.maybeInjectChaos(w) {
		return
	}

	id, err := idFromPath(r.URL.Path)
	if err != nil {
		http.Error(w, "invalid shard id", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		n.handlePutShard(w, r, id)
	case http.MethodGet:
		n.handleGetShard(w, r, id)
	case http.MethodDelete:
		n.handleDeleteShard(w, r, id)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// PUT /shards/{id}
// Writes to tmp, fsyncs, renames, and fsyncs directory before acking.
func (n *Node) handlePutShard(w http.ResponseWriter, r *http.Request, id string) {
	targetPath := n.shardFilePath(id)
	targetDir := filepath.Dir(targetPath)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		http.Error(w, fmt.Sprintf("mkdir failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Create unique tmp file
	rndBytes := make([]byte, 8)
	_, _ = rand.Read(rndBytes)
	tmpFile := filepath.Join(n.cfg.DataDir, "tmp", fmt.Sprintf("shard-%s-%s.tmp", strings.ReplaceAll(id, "/", "_"), hex.EncodeToString(rndBytes)))

	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		http.Error(w, fmt.Sprintf("create tmp failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Stream payload to tmp file
	_, err = io.Copy(f, r.Body)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tmpFile)
		http.Error(w, fmt.Sprintf("write tmp failed: %v", err), http.StatusInternalServerError)
		return
	}

	// 1. fsync tmp file
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpFile)
		http.Error(w, fmt.Sprintf("fsync tmp failed: %v", err), http.StatusInternalServerError)
		return
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpFile)
		http.Error(w, fmt.Sprintf("close tmp failed: %v", err), http.StatusInternalServerError)
		return
	}

	// 2. rename tmp to target (remove target if exists for Windows atomic overwrite)
	_ = os.Remove(targetPath)
	if err := os.Rename(tmpFile, targetPath); err != nil {
		_ = os.Remove(tmpFile)
		http.Error(w, fmt.Sprintf("rename failed: %v", err), http.StatusInternalServerError)
		return
	}

	// 3. fsync directory
	dirF, err := os.Open(targetDir)
	if err == nil {
		_ = dirF.Sync()
		_ = dirF.Close()
	}

	w.WriteHeader(http.StatusCreated)
}

// GET /shards/{id}
// Reads shard file, parses header, verifies CRC32C over payload.
// Returns 500 with 'x-sv-corrupt: 1' if corrupt.
// Returns 404 if not found.
func (n *Node) handleGetShard(w http.ResponseWriter, r *http.Request, id string) {
	path := n.shardFilePath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "shard not found", http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("read error: %v", err), http.StatusInternalServerError)
		return
	}

	// Verify header & payload CRC32C unless no-verify is requested for negative control
	noVerify := r.Header.Get("x-sv-no-verify") == "1"
	if !noVerify {
		h, err := shard.DecodeHeader(data)
		if err != nil {
			w.Header().Set("x-sv-corrupt", "1")
			http.Error(w, fmt.Sprintf("corrupt shard header: %v", err), http.StatusInternalServerError)
			return
		}

		if uint64(len(data)-shard.HeaderSize) != h.DataLen {
			w.Header().Set("x-sv-corrupt", "1")
			http.Error(w, "corrupt shard: length mismatch", http.StatusInternalServerError)
			return
		}

		payload := data[shard.HeaderSize:]
		actualCRC := shard.Checksum(payload)
		if actualCRC != h.DataCRC32C {
			w.Header().Set("x-sv-corrupt", "1")
			http.Error(w, "corrupt shard: crc mismatch", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// DELETE /shards/{id}
func (n *Node) handleDeleteShard(w http.ResponseWriter, r *http.Request, id string) {
	path := n.shardFilePath(id)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		http.Error(w, fmt.Sprintf("delete failed: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /shards?prefix=
// Returns newline-separated list of shard IDs matching the prefix.
func (n *Node) handleListShards(w http.ResponseWriter, r *http.Request) {
	if n.maybeInjectChaos(w) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	prefix := r.URL.Query().Get("prefix")
	shardsBase := filepath.Join(n.cfg.DataDir, "shards")

	var matched []string
	_ = filepath.Walk(shardsBase, func(p string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(shardsBase, p)
		if err != nil {
			return nil
		}
		// normalize to forward slashes
		slashRel := filepath.ToSlash(rel)
		if strings.HasPrefix(slashRel, prefix) {
			matched = append(matched, slashRel)
		}
		return nil
	})

	w.Header().Set("Content-Type", "text/plain")
	for _, id := range matched {
		_, _ = fmt.Fprintf(w, "%s\n", id)
	}
}
