package scrubber

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/hiroshi-os/shardvault/internal/ec"
	"github.com/hiroshi-os/shardvault/internal/gateway"
	"github.com/hiroshi-os/shardvault/internal/metadata"
	"github.com/hiroshi-os/shardvault/internal/placement"
	"github.com/hiroshi-os/shardvault/internal/shard"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricScrubShardsChecked = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "scrub_shards_checked_total",
			Help: "Total shard replicas checked by scrubber",
		},
	)
	metricCorruptShardsFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "corrupt_shards_found_total",
			Help: "Total corrupt or missing shards discovered during scrub",
		},
	)
	metricShardsRepaired = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "shards_repaired_total",
			Help: "Total shards successfully reconstructed and repaired",
		},
	)
	metricRepairBytes = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "repair_bytes_total",
			Help: "Total payload bytes written during repair",
		},
	)
	metricStripesAtRiskScrub = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "scrub_stripes_at_risk",
			Help: "Stripes with fewer than k+m healthy shards",
		},
	)
)

type Scrubber struct {
	meta       *metadata.Store
	cfg        gateway.Config
	codec      *ec.Codec
	hrw        *placement.HRW
	httpClient *http.Client
}

func New(meta *metadata.Store, cfg gateway.Config) *Scrubber {
	codec, _ := ec.NewCodec(cfg.K, cfg.M)
	hrw, _ := placement.NewHRW(cfg.StorageNodes)
	return &Scrubber{
		meta:  meta,
		cfg:   cfg,
		codec: codec,
		hrw:   hrw,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// StartBackground runs periodic scrub and garbage collection loops in the background.
func (s *Scrubber) StartBackground(ctx context.Context, scrubInterval, gcInterval time.Duration) {
	go func() {
		scrubTicker := time.NewTicker(scrubInterval)
		defer scrubTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-scrubTicker.C:
				_, _, _ = s.Scrub(ctx)
			}
		}
	}()

	go func() {
		gcTicker := time.NewTicker(gcInterval)
		defer gcTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-gcTicker.C:
				_ = s.GarbageCollect(ctx, 10*time.Minute)
			}
		}
	}()
}

// Scrub walks active versions in metadata, verifies shard presence and CRC,
// reconstructs missing or corrupt shards from healthy ones, and writes them to designated nodes.
// Returns (stripesAtRisk, repairedShards, error).
func (s *Scrubber) Scrub(ctx context.Context) (int, int, error) {
	versions, err := s.meta.ActiveVersions()
	if err != nil {
		return 0, 0, err
	}

	remainingStripesAtRisk := 0
	totalRepaired := 0
	totalShards := s.cfg.K + s.cfg.M

	for _, v := range versions {
		if v.IsDeleteMark {
			continue
		}

		// Group known locations by stripeIdx
		locByStripe := make(map[uint32]map[uint16]string)
		for _, loc := range v.Locations {
			if locByStripe[loc.StripeIdx] == nil {
				locByStripe[loc.StripeIdx] = make(map[uint16]string)
			}
			locByStripe[loc.StripeIdx][loc.ShardIdx] = loc.Node
		}

		versionUpdated := false

		for stIdx := uint32(0); stIdx < v.StripeCount; stIdx++ {
			select {
			case <-ctx.Done():
				return remainingStripesAtRisk, totalRepaired, ctx.Err()
			default:
			}

			hrwKey := fmt.Sprintf("%s/%d", v.VersionID, stIdx)
			hrwNodes, err := s.hrw.PickNodes(hrwKey, totalShards)
			if err != nil {
				continue
			}

			// Check all k+m shards for this stripe
			shardsPayload := make([][]byte, totalShards)
			healthyCount := 0
			var missingIndices []int

			for shIdx := 0; shIdx < totalShards; shIdx++ {
				metricScrubShardsChecked.Inc()
				targetNode := ""
				if locByStripe[stIdx] != nil {
					targetNode = locByStripe[stIdx][uint16(shIdx)]
				}
				if targetNode == "" {
					targetNode = hrwNodes[shIdx]
				}

				payload, err := s.fetchAndVerifyShard(ctx, targetNode, v.VersionID, stIdx, shIdx)
				if err != nil {
					metricCorruptShardsFound.Inc()
					missingIndices = append(missingIndices, shIdx)
				} else {
					shardsPayload[shIdx] = payload
					healthyCount++
				}
			}

			stripeRepairedCount := 0
			// If we have at least K healthy shards and any missing/corrupt shards, repair them!
			if len(missingIndices) > 0 {
				if healthyCount < s.cfg.K {
					fmt.Printf("[scrubber] Cannot repair stripe %d of ver %s: healthyCount=%d < K=%d, missing=%v\n", stIdx, v.VersionID, healthyCount, s.cfg.K, missingIndices)
				} else if err := s.codec.Reconstruct(shardsPayload); err != nil {
					fmt.Printf("[scrubber] Reconstruct stripe %d failed: %v (healthyCount=%d, missing=%v)\n", stIdx, err, healthyCount, missingIndices)
				} else {
					for _, missIdx := range missingIndices {
						repPayload := shardsPayload[missIdx]
						nodeToStore := hrwNodes[missIdx]

						// If designated node is unreachable, use next HRW candidate node
						if !s.isNodeHealthy(ctx, nodeToStore) {
							allHRW, _ := s.hrw.PickNodes(hrwKey, len(s.cfg.StorageNodes))
							for _, cand := range allHRW {
								if s.isNodeHealthy(ctx, cand) {
									nodeToStore = cand
									break
								}
							}
						}

						// Write reconstructed shard
						if err := s.writeShard(ctx, nodeToStore, v.VersionID, stIdx, missIdx, repPayload); err != nil {
							fmt.Printf("[scrubber] writeShard to %s failed: %v\n", nodeToStore, err)
						} else {
							metricShardsRepaired.Inc()
							metricRepairBytes.Add(float64(len(repPayload)))
							totalRepaired++
							stripeRepairedCount++

							// Update in-memory map
							if locByStripe[stIdx] == nil {
								locByStripe[stIdx] = make(map[uint16]string)
							}
							locByStripe[stIdx][uint16(missIdx)] = nodeToStore

							// Update location list (replace existing if present, else append)
							found := false
							for i, l := range v.Locations {
								if l.StripeIdx == stIdx && l.ShardIdx == uint16(missIdx) {
									v.Locations[i].Node = nodeToStore
									found = true
									break
								}
							}
							if !found {
								v.Locations = append(v.Locations, metadata.ShardLoc{
									StripeIdx: stIdx,
									ShardIdx:  uint16(missIdx),
									Node:      nodeToStore,
								})
							}

							// Remove from missing list
							var newMissing []metadata.ShardLoc
							for _, m := range v.Missing {
								if !(m.StripeIdx == stIdx && m.ShardIdx == uint16(missIdx)) {
									newMissing = append(newMissing, m)
								}
							}
							v.Missing = newMissing
							versionUpdated = true
						}
					}
				}
			}

			// A stripe is at risk if after repair attempts it still has fewer than totalShards healthy shards
			if (healthyCount + stripeRepairedCount) < totalShards {
				remainingStripesAtRisk++
			}
		}

		if versionUpdated {
			_ = s.meta.UpdateVersionMetadata(&v)
		}
	}

	metricStripesAtRiskScrub.Set(float64(remainingStripesAtRisk))
	return remainingStripesAtRisk, totalRepaired, nil
}

func (s *Scrubber) isNodeHealthy(ctx context.Context, nodeURL string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nodeURL+"/healthz", nil)
	if err != nil {
		return false
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (s *Scrubber) fetchAndVerifyShard(ctx context.Context, nodeURL, versionID string, stripeIdx uint32, shardIdx int) ([]byte, error) {
	shardURL := fmt.Sprintf("%s/shards/%s/%d/%d", nodeURL, versionID, stripeIdx, shardIdx)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, shardURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if len(data) < shard.HeaderSize {
		return nil, shard.ErrHeaderTooShort
	}

	hdr, err := shard.DecodeHeader(data[:shard.HeaderSize])
	if err != nil {
		return nil, err
	}
	payload := data[shard.HeaderSize:]
	if shard.Checksum(payload) != hdr.DataCRC32C {
		return nil, shard.ErrCorruptData
	}
	return payload, nil
}

func (s *Scrubber) writeShard(ctx context.Context, nodeURL, versionID string, stripeIdx uint32, shardIdx int, payload []byte) error {
	hdr := &shard.ShardHeader{
		Version:         shard.CurrentVersion,
		ObjectVersionID: versionID,
		StripeIdx:       stripeIdx,
		ShardIdx:        uint16(shardIdx),
		DataLen:         uint64(len(payload)),
		DataCRC32C:      shard.Checksum(payload),
	}
	body := append(hdr.Encode(), payload...)

	shardURL := fmt.Sprintf("%s/shards/%s/%d/%d", nodeURL, versionID, stripeIdx, shardIdx)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, shardURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("node put returned status %d", resp.StatusCode)
	}
	return nil
}

// GarbageCollect removes orphan shards and shards of superseded or deleted versions past gracePeriod.
// Never deletes shards referenced by any committed active version.
func (s *Scrubber) GarbageCollect(ctx context.Context, gracePeriod time.Duration) error {
	versions, err := s.meta.AllVersions()
	if err != nil {
		return err
	}

	activeVerIDs, err := s.meta.AllActiveVersionIDs()
	if err != nil {
		return err
	}

	// Build whitelist of active shards that must NEVER be deleted
	keepShards := make(map[string]bool)
	for _, v := range versions {
		if activeVerIDs[v.VersionID] && !v.IsDeleteMark {
			for _, loc := range v.Locations {
				id := fmt.Sprintf("%s/%d/%d", v.VersionID, loc.StripeIdx, loc.ShardIdx)
				keepShards[id] = true
			}
		}
	}

	// For each storage node, list all shards and delete candidates
	for _, nodeURL := range s.cfg.StorageNodes {
		listURL := nodeURL + "/shards"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, listURL, nil)
		if err != nil {
			continue
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		lines := strings.Split(strings.TrimSpace(string(body)), "\n")
		for _, shardID := range lines {
			shardID = strings.TrimSpace(shardID)
			if shardID == "" {
				continue
			}

			// If referenced by an active version, keep it!
			if keepShards[shardID] {
				continue
			}

			// Check age of version from ID if possible (ULID timestamp)
			parts := strings.Split(shardID, "/")
			if len(parts) >= 1 {
				verULID, err := ulid.Parse(parts[0])
				if err == nil {
					age := time.Since(time.UnixMilli(int64(verULID.Time())))
					if age < gracePeriod {
						// Inside grace period: do not delete
						continue
					}
				}
			}

			// Delete orphan shard
			delURL := fmt.Sprintf("%s/shards/%s", nodeURL, shardID)
			delReq, _ := http.NewRequestWithContext(ctx, http.MethodDelete, delURL, nil)
			if delResp, err := s.httpClient.Do(delReq); err == nil {
				_ = delResp.Body.Close()
			}
		}
	}

	return nil
}
