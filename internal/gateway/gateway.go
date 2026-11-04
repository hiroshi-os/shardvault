package gateway

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/hiroshi-os/shardvault/internal/ec"
	"github.com/hiroshi-os/shardvault/internal/metadata"
	"github.com/hiroshi-os/shardvault/internal/placement"
	"github.com/hiroshi-os/shardvault/internal/shard"
	"github.com/hiroshi-os/shardvault/internal/stripe"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricReqDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sv_request_duration_seconds",
			Help:    "Gateway request duration by operation",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"op"},
	)
	metricDegradedReads = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "degraded_reads_total",
			Help: "Total reads requiring parity reconstruction or falling back to parity",
		},
	)
	metricHedgesFired = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "hedges_fired_total",
			Help: "Total times hedged requests to parity shards were dispatched",
		},
	)
	metricDecodeReconstructs = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "decode_reconstructs_total",
			Help: "Total stripe reconstructions performed",
		},
	)
	metricNodeErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "node_errors_total",
			Help: "Errors encountered per storage node",
		},
		[]string{"node"},
	)
)

type Config struct {
	K             int           // data shards (e.g., 4)
	M             int           // parity shards (e.g., 2)
	WriteQuorum   int           // acks required per stripe (e.g., 5)
	StripeSize    int64         // default 1 MiB (1048576)
	StorageNodes  []string      // URLs e.g. http://127.0.0.1:8081
	HedgeDelay    time.Duration // delay before requesting parity shards
	DisableVerify bool          // negative control: disable CRC and stripe SHA verification
}

type ScrubFunc func(ctx context.Context) (stripesAtRisk int, repaired int, err error)
type GCFunc func(ctx context.Context, grace time.Duration) error

type Gateway struct {
	cfg        Config
	meta       *metadata.Store
	codec      *ec.Codec
	hrw        *placement.HRW
	httpClient *http.Client

	scrubFunc ScrubFunc
	gcFunc    GCFunc
}

func New(cfg Config, meta *metadata.Store) (*Gateway, error) {
	if cfg.K <= 0 || cfg.M <= 0 {
		return nil, errors.New("invalid K or M")
	}
	if cfg.WriteQuorum < cfg.K {
		return nil, fmt.Errorf("write quorum %d cannot be less than K %d", cfg.WriteQuorum, cfg.K)
	}
	if cfg.StripeSize <= 0 {
		cfg.StripeSize = 1048576 // 1 MiB
	}
	if cfg.HedgeDelay <= 0 {
		cfg.HedgeDelay = 20 * time.Millisecond
	}

	hrw, err := placement.NewHRW(cfg.StorageNodes)
	if err != nil {
		return nil, err
	}
	if len(cfg.StorageNodes) < cfg.K+cfg.M {
		return nil, errors.New("refusing to start: configured storage nodes fewer than k+m")
	}

	codec, err := ec.NewCodec(cfg.K, cfg.M)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 200,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	return &Gateway{
		cfg:        cfg,
		meta:       meta,
		codec:      codec,
		hrw:        hrw,
		httpClient: client,
	}, nil
}

func (gw *Gateway) SetDisableVerify(disable bool) {
	gw.cfg.DisableVerify = disable
}

func (gw *Gateway) SetAdminOps(scrub ScrubFunc, gc GCFunc) {
	gw.scrubFunc = scrub
	gw.gcFunc = gc
}

func (gw *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", gw.handleHealthz)
	mux.HandleFunc("/admin/scrub", gw.handleAdminScrub)
	mux.HandleFunc("/admin/gc", gw.handleAdminGC)
	mux.HandleFunc("/", gw.handleRoot)
	return mux
}

func (gw *Gateway) handleAdminScrub(w http.ResponseWriter, r *http.Request) {
	if gw.scrubFunc == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"repaired":0,"stripes_at_risk":0}` + "\n"))
		return
	}
	atRisk, repaired, err := gw.scrubFunc(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"repaired":%d,"stripes_at_risk":%d}`+"\n", repaired, atRisk)
}

func (gw *Gateway) handleAdminGC(w http.ResponseWriter, r *http.Request) {
	if gw.gcFunc == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	if err := gw.gcFunc(r.Context(), 10*time.Minute); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (gw *Gateway) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK\n"))
}

func (gw *Gateway) handleRoot(w http.ResponseWriter, r *http.Request) {
	p := strings.Trim(r.URL.Path, "/")
	if p == "" {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}

	parts := strings.SplitN(p, "/", 2)
	bucket := parts[0]

	// PUT /{bucket} creates bucket
	if len(parts) == 1 {
		if r.Method == http.MethodPut {
			gw.handlePutBucket(w, r, bucket)
			return
		}
		if r.Method == http.MethodGet {
			gw.handleListObjects(w, r, bucket)
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := parts[1]
	switch r.Method {
	case http.MethodPut:
		gw.handlePutObject(w, r, bucket, key)
	case http.MethodGet:
		gw.handleGetObject(w, r, bucket, key)
	case http.MethodHead:
		gw.handleHeadObject(w, r, bucket, key)
	case http.MethodDelete:
		gw.handleDeleteObject(w, r, bucket, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (gw *Gateway) handlePutBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	timer := prometheus.NewTimer(metricReqDuration.WithLabelValues("PUT_BUCKET"))
	defer timer.ObserveDuration()

	if err := gw.meta.CreateBucket(bucket); err != nil {
		if errors.Is(err, metadata.ErrBucketExists) {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// PUT /{bucket}/{key} with streaming body
func (gw *Gateway) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	timer := prometheus.NewTimer(metricReqDuration.WithLabelValues("PUT_OBJECT"))
	defer timer.ObserveDuration()

	exists, err := gw.meta.BucketExists(bucket)
	if err != nil || !exists {
		http.Error(w, "NoSuchBucket", http.StatusNotFound)
		return
	}

	versionID := ulid.Make().String()
	stripeSize := gw.cfg.StripeSize
	k := gw.cfg.K
	m := gw.cfg.M
	totalShards := k + m

	md5Hash := md5.New()
	bodyReader := io.TeeReader(r.Body, md5Hash)

	var totalBytes int64
	var stripeIdx uint32
	var stripeSHAs []string
	var committedLocations []metadata.ShardLoc
	var missingLocations []metadata.ShardLoc

	buf := make([]byte, stripeSize)
	for {
		n, readErr := io.ReadFull(bodyReader, buf)
		if n > 0 {
			totalBytes += int64(n)
			stripeData := buf[:n]

			// RS encode stripe
			shards, _, stripeSHA, err := gw.codec.EncodeStripe(stripeData)
			if err != nil {
				http.Error(w, fmt.Sprintf("encode error: %v", err), http.StatusInternalServerError)
				return
			}
			stripeSHAs = append(stripeSHAs, hex.EncodeToString(stripeSHA[:]))

			// Rendezvous HRW placement
			hrwKey := fmt.Sprintf("%s/%d", versionID, stripeIdx)
			nodes, err := gw.hrw.PickNodes(hrwKey, totalShards)
			if err != nil {
				http.Error(w, fmt.Sprintf("placement error: %v", err), http.StatusInternalServerError)
				return
			}

			// Parallel upload of k+m shards
			type uploadRes struct {
				shardIdx int
				node     string
				err      error
			}
			resCh := make(chan uploadRes, totalShards)

			for sIdx := 0; sIdx < totalShards; sIdx++ {
				sIdx := sIdx
				nodeURL := nodes[sIdx]
				shardPayload := shards[sIdx]

				go func() {
					hdr := &shard.ShardHeader{
						Version:         shard.CurrentVersion,
						ObjectVersionID: versionID,
						StripeIdx:       stripeIdx,
						ShardIdx:        uint16(sIdx),
						DataLen:         uint64(len(shardPayload)),
						DataCRC32C:      shard.Checksum(shardPayload),
					}
					body := append(hdr.Encode(), shardPayload...)

					shardURL := fmt.Sprintf("%s/shards/%s/%d/%d", nodeURL, versionID, stripeIdx, sIdx)
					req, err := http.NewRequest(http.MethodPut, shardURL, bytes.NewReader(body))
					if err != nil {
						resCh <- uploadRes{shardIdx: sIdx, node: nodeURL, err: err}
						return
					}
					req.Header.Set("Content-Type", "application/octet-stream")

					resp, err := gw.httpClient.Do(req)
					if err != nil {
						metricNodeErrors.WithLabelValues(nodeURL).Inc()
						resCh <- uploadRes{shardIdx: sIdx, node: nodeURL, err: err}
						return
					}
					_ = resp.Body.Close()
					if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
						metricNodeErrors.WithLabelValues(nodeURL).Inc()
						resCh <- uploadRes{shardIdx: sIdx, node: nodeURL, err: fmt.Errorf("node returned %d", resp.StatusCode)}
						return
					}

					resCh <- uploadRes{shardIdx: sIdx, node: nodeURL, err: nil}
				}()
			}

			// Collect responses and check quorum
			var acks int
			for i := 0; i < totalShards; i++ {
				res := <-resCh
				loc := metadata.ShardLoc{
					StripeIdx: stripeIdx,
					ShardIdx:  uint16(res.shardIdx),
					Node:      res.node,
				}
				if res.err == nil {
					acks++
					committedLocations = append(committedLocations, loc)
				} else {
					missingLocations = append(missingLocations, loc)
				}
			}

			if acks < gw.cfg.WriteQuorum {
				// Quorum not reached: abort PUT, return 503 ServiceUnavailable
				http.Error(w, fmt.Sprintf("ServiceUnavailable: quorum not met (got %d, need %d)", acks, gw.cfg.WriteQuorum), http.StatusServiceUnavailable)
				return
			}

			stripeIdx++
		}

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			http.Error(w, fmt.Sprintf("read body error: %v", readErr), http.StatusBadRequest)
			return
		}
	}

	// Handle 0-byte object edge case (1 empty stripe)
	if totalBytes == 0 {
		shards, _, stripeSHA, err := gw.codec.EncodeStripe([]byte{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		stripeSHAs = append(stripeSHAs, hex.EncodeToString(stripeSHA[:]))
		hrwKey := fmt.Sprintf("%s/0", versionID)
		nodes, err := gw.hrw.PickNodes(hrwKey, totalShards)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var acks int
		for sIdx := 0; sIdx < totalShards; sIdx++ {
			nodeURL := nodes[sIdx]
			shardPayload := shards[sIdx]
			hdr := &shard.ShardHeader{
				Version:         shard.CurrentVersion,
				ObjectVersionID: versionID,
				StripeIdx:       0,
				ShardIdx:        uint16(sIdx),
				DataLen:         uint64(len(shardPayload)),
				DataCRC32C:      shard.Checksum(shardPayload),
			}
			body := append(hdr.Encode(), shardPayload...)
			shardURL := fmt.Sprintf("%s/shards/%s/0/%d", nodeURL, versionID, sIdx)
			req, _ := http.NewRequest(http.MethodPut, shardURL, bytes.NewReader(body))
			resp, err := gw.httpClient.Do(req)
			loc := metadata.ShardLoc{StripeIdx: 0, ShardIdx: uint16(sIdx), Node: nodeURL}
			if err == nil && (resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated) {
				acks++
				committedLocations = append(committedLocations, loc)
				_ = resp.Body.Close()
			} else {
				missingLocations = append(missingLocations, loc)
			}
		}
		if acks < gw.cfg.WriteQuorum {
			http.Error(w, fmt.Sprintf("ServiceUnavailable: quorum not met (got %d, need %d)", acks, gw.cfg.WriteQuorum), http.StatusServiceUnavailable)
			return
		}
		stripeIdx = 1
	}

	etag := hex.EncodeToString(md5Hash.Sum(nil))

	// Single atomic metadata transaction commit
	vMeta := &metadata.VersionMeta{
		VersionID:    versionID,
		Bucket:       bucket,
		Key:          key,
		Size:         totalBytes,
		ETag:         etag,
		StripeCount:  stripeIdx,
		StripeSize:   stripeSize,
		StripeSHAs:   stripeSHAs,
		Locations:    committedLocations,
		Missing:      missingLocations,
		IsDeleteMark: false,
		CreatedAt:    time.Now().UTC(),
	}

	if err := gw.meta.CommitVersion(vMeta); err != nil {
		http.Error(w, fmt.Sprintf("commit failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", etag))
	w.Header().Set("x-sv-version-id", versionID)
	w.WriteHeader(http.StatusOK)
}

// GET /{bucket}/{key} with Hedged Degraded Reads and Range support
func (gw *Gateway) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	timer := prometheus.NewTimer(metricReqDuration.WithLabelValues("GET_OBJECT"))
	defer timer.ObserveDuration()

	meta, err := gw.meta.GetCurrentVersion(bucket, key)
	if err != nil {
		if errors.Is(err, metadata.ErrNoSuchKey) {
			http.Error(w, "NoSuchKey", http.StatusNotFound)
			return
		}
		if errors.Is(err, metadata.ErrNoSuchBucket) {
			http.Error(w, "NoSuchBucket", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if meta.Size == 0 {
		w.Header().Set("ETag", fmt.Sprintf("\"%s\"", meta.ETag))
		w.Header().Set("x-sv-version-id", meta.VersionID)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return
	}

	// Range header parsing
	rangeHdr := r.Header.Get("Range")
	reqStart := int64(0)
	reqEnd := meta.Size - 1
	isRange := false

	if rangeHdr != "" {
		if strings.HasPrefix(rangeHdr, "bytes=") {
			spec := strings.TrimPrefix(rangeHdr, "bytes=")
			parts := strings.Split(spec, "-")
			if len(parts) == 2 {
				s, err1 := strconv.ParseInt(parts[0], 10, 64)
				e, err2 := strconv.ParseInt(parts[1], 10, 64)
				if err1 == nil && err2 == nil && s >= 0 && e >= s && e < meta.Size {
					reqStart = s
					reqEnd = e
					isRange = true
				}
			}
		}
	}

	span, err := stripe.CalculateSpan(meta.Size, meta.StripeSize, reqStart, reqEnd)
	if err != nil {
		http.Error(w, "InvalidRange", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	// Map locations for quick lookup: map[stripeIdx]map[shardIdx]nodeURL
	locMap := make(map[uint32]map[uint16]string)
	for _, l := range meta.Locations {
		if locMap[l.StripeIdx] == nil {
			locMap[l.StripeIdx] = make(map[uint16]string)
		}
		locMap[l.StripeIdx][l.ShardIdx] = l.Node
	}

	contentLength := reqEnd - reqStart + 1

	// Fetch first stripe before sending headers to catch errors (503 Service Unavailable)
	firstStripeIdx := span.StartStripe
	var firstOrigLen int
	if int64(firstStripeIdx+1)*meta.StripeSize <= meta.Size {
		firstOrigLen = int(meta.StripeSize)
	} else {
		firstOrigLen = int(meta.Size - int64(firstStripeIdx)*meta.StripeSize)
	}
	var firstExpectedSHA [32]byte
	if int(firstStripeIdx) < len(meta.StripeSHAs) {
		b, _ := hex.DecodeString(meta.StripeSHAs[firstStripeIdx])
		copy(firstExpectedSHA[:], b)
	}

	noVerify := gw.cfg.DisableVerify || r.Header.Get("x-sv-no-verify") == "1"
	noHedge := r.Header.Get("x-sv-no-hedge") == "1"

	firstStripeBytes, err := gw.readStripeWithHedging(r.Context(), meta.VersionID, firstStripeIdx, firstOrigLen, firstExpectedSHA, locMap[firstStripeIdx], noVerify, noHedge)
	if err != nil {
		http.Error(w, "Service Unavailable: fewer than k valid shards reachable", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", meta.ETag))
	w.Header().Set("x-sv-version-id", meta.VersionID)
	w.Header().Set("Accept-Ranges", "bytes")

	if isRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", reqStart, reqEnd, meta.Size))
		w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
		w.WriteHeader(http.StatusOK)
	}

	relStart, relEnd := stripe.StripeByteBounds(firstStripeIdx, meta.StripeSize, reqStart, reqEnd)
	if relEnd > int64(len(firstStripeBytes)) {
		relEnd = int64(len(firstStripeBytes))
	}
	if relStart < relEnd {
		_, _ = w.Write(firstStripeBytes[relStart:relEnd])
	}

	// Stream remaining stripes in range
	for sIdx := span.StartStripe + 1; sIdx <= span.EndStripe; sIdx++ {
		var origLen int
		if int64(sIdx+1)*meta.StripeSize <= meta.Size {
			origLen = int(meta.StripeSize)
		} else {
			origLen = int(meta.Size - int64(sIdx)*meta.StripeSize)
		}

		var expectedSHA [32]byte
		if int(sIdx) < len(meta.StripeSHAs) {
			b, _ := hex.DecodeString(meta.StripeSHAs[sIdx])
			copy(expectedSHA[:], b)
		}

		stripeBytes, err := gw.readStripeWithHedging(r.Context(), meta.VersionID, sIdx, origLen, expectedSHA, locMap[sIdx], noVerify, noHedge)
		if err != nil {
			return
		}

		relStart, relEnd := stripe.StripeByteBounds(sIdx, meta.StripeSize, reqStart, reqEnd)
		if relEnd > int64(len(stripeBytes)) {
			relEnd = int64(len(stripeBytes))
		}
		if relStart < relEnd {
			_, _ = w.Write(stripeBytes[relStart:relEnd])
		}
	}
}

// readStripeWithHedging fetches k data shards concurrently.
// If any shard fails, has bad CRC, or is slower than hedge delay, requests parity shards.
// Decodes as soon as any k valid shards arrive.
func (gw *Gateway) readStripeWithHedging(
	ctx context.Context,
	versionID string,
	stripeIdx uint32,
	origLen int,
	expectedSHA [32]byte,
	nodeMap map[uint16]string,
	noVerify bool,
	noHedge bool,
) ([]byte, error) {
	k := gw.cfg.K
	m := gw.cfg.M
	total := k + m

	ctxStripe, cancel := context.WithCancel(ctx)
	defer cancel()

	type shardResult struct {
		shardIdx int
		data     []byte
		err      error
	}

	resCh := make(chan shardResult, total)
	var launched [64]bool // track launched requests

	fetchShard := func(sIdx int) {
		nodeURL := nodeMap[uint16(sIdx)]
		if nodeURL == "" {
			// fallback to HRW designation if not in locMap
			hrwKey := fmt.Sprintf("%s/%d", versionID, stripeIdx)
			nodes, err := gw.hrw.PickNodes(hrwKey, total)
			if err == nil && sIdx < len(nodes) {
				nodeURL = nodes[sIdx]
			}
		}
		if nodeURL == "" {
			resCh <- shardResult{shardIdx: sIdx, err: errors.New("no node for shard")}
			return
		}

		shardURL := fmt.Sprintf("%s/shards/%s/%d/%d", nodeURL, versionID, stripeIdx, sIdx)
		req, err := http.NewRequestWithContext(ctxStripe, http.MethodGet, shardURL, nil)
		if err != nil {
			resCh <- shardResult{shardIdx: sIdx, err: err}
			return
		}
		if noVerify {
			req.Header.Set("x-sv-no-verify", "1")
		}

		resp, err := gw.httpClient.Do(req)
		if err != nil {
			metricNodeErrors.WithLabelValues(nodeURL).Inc()
			resCh <- shardResult{shardIdx: sIdx, err: err}
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			metricNodeErrors.WithLabelValues(nodeURL).Inc()
			resCh <- shardResult{shardIdx: sIdx, err: fmt.Errorf("status %d", resp.StatusCode)}
			return
		}

		fullData, err := io.ReadAll(resp.Body)
		if err != nil {
			resCh <- shardResult{shardIdx: sIdx, err: err}
			return
		}

		if len(fullData) < shard.HeaderSize {
			resCh <- shardResult{shardIdx: sIdx, err: errors.New("shard too short")}
			return
		}

		hdr, err := shard.DecodeHeader(fullData[:shard.HeaderSize])
		if err != nil && !noVerify {
			resCh <- shardResult{shardIdx: sIdx, err: err}
			return
		}

		payload := fullData[shard.HeaderSize:]
		if !noVerify {
			if shard.Checksum(payload) != hdr.DataCRC32C {
				resCh <- shardResult{shardIdx: sIdx, err: shard.ErrCorruptData}
				return
			}
		}

		resCh <- shardResult{shardIdx: sIdx, data: payload, err: nil}
	}

	// 1. Launch requests for k data shards
	for i := 0; i < k; i++ {
		launched[i] = true
		go fetchShard(i)
	}

	shards := make([][]byte, total)
	validCount := 0
	completedCount := 0
	parityLaunched := false

	// Timer for hedge delay (disabled if noHedge is true)
	var hedgeCh <-chan time.Time
	if !noHedge {
		hedgeTimer := time.NewTimer(gw.cfg.HedgeDelay)
		defer hedgeTimer.Stop()
		hedgeCh = hedgeTimer.C
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-hedgeCh:
			hedgeCh = nil // disable further triggers
			// Hedging triggered: request remaining parity shards if not already launched
			if !parityLaunched && validCount < k {
				metricHedgesFired.Inc()
				metricDegradedReads.Inc()
				parityLaunched = true
				for i := k; i < total; i++ {
					if !launched[i] {
						launched[i] = true
						go fetchShard(i)
					}
				}
			}

		case res := <-resCh:
			if res.err == nil && len(res.data) > 0 {
				shards[res.shardIdx] = res.data
				validCount++
				if validCount >= k {
					// We have k shards! Cancel any outstanding requests
					cancel()

					// Check if any data shard was missing or if parity was used
					usedParity := false
					for i := k; i < total; i++ {
						if len(shards[i]) > 0 {
							usedParity = true
							break
						}
					}
					missingData := false
					for i := 0; i < k; i++ {
						if len(shards[i]) == 0 {
							missingData = true
							break
						}
					}

					if usedParity || missingData {
						metricDecodeReconstructs.Inc()
						metricDegradedReads.Inc()
					}

					decoded, err := gw.codec.DecodeStripe(shards, origLen, expectedSHA)
					if err != nil && !noVerify {
						return nil, err
					}
					if noVerify && decoded == nil {
						// Under -no-verify, construct data even with errors or bad checksums
						decoded = gw.directAssembleData(shards, origLen)
					}
					return decoded, nil
				}
			} else {
				// A shard failed or had bad CRC: dispatch parity immediately if not already launched
				if !parityLaunched {
					metricDegradedReads.Inc()
					parityLaunched = true
					for i := k; i < total; i++ {
						if !launched[i] {
							launched[i] = true
							go fetchShard(i)
						}
					}
				}
			}

			completedCount++
			if completedCount == total && validCount < k {
				return nil, ec.ErrTooFewShards
			}
		}
	}
}

// directAssembleData assembles unverified bytes directly from available data shards
func (gw *Gateway) directAssembleData(shards [][]byte, origLen int) []byte {
	buf := make([]byte, 0, origLen)
	remaining := origLen
	for i := 0; i < gw.cfg.K; i++ {
		if remaining <= 0 {
			break
		}
		if len(shards[i]) == 0 {
			// pad missing shard with zeros in negative control
			pad := origLen / gw.cfg.K
			buf = append(buf, make([]byte, pad)...)
			remaining -= pad
			continue
		}
		take := len(shards[i])
		if take > remaining {
			take = remaining
		}
		buf = append(buf, shards[i][:take]...)
		remaining -= take
	}
	return buf
}

func (gw *Gateway) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	timer := prometheus.NewTimer(metricReqDuration.WithLabelValues("HEAD_OBJECT"))
	defer timer.ObserveDuration()

	meta, err := gw.meta.GetCurrentVersion(bucket, key)
	if err != nil {
		if errors.Is(err, metadata.ErrNoSuchKey) || errors.Is(err, metadata.ErrNoSuchBucket) {
			http.Error(w, "NotFound", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", fmt.Sprintf("\"%s\"", meta.ETag))
	w.Header().Set("x-sv-version-id", meta.VersionID)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
}

func (gw *Gateway) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	timer := prometheus.NewTimer(metricReqDuration.WithLabelValues("DELETE_OBJECT"))
	defer timer.ObserveDuration()

	delVersionID := ulid.Make().String()
	delMeta := &metadata.VersionMeta{
		VersionID:    delVersionID,
		Bucket:       bucket,
		Key:          key,
		IsDeleteMark: true,
		CreatedAt:    time.Now().UTC(),
	}

	if err := gw.meta.CommitVersion(delMeta); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("x-sv-version-id", delVersionID)
	w.WriteHeader(http.StatusNoContent)
}

func (gw *Gateway) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	timer := prometheus.NewTimer(metricReqDuration.WithLabelValues("LIST_OBJECTS"))
	defer timer.ObserveDuration()

	prefix := r.URL.Query().Get("prefix")
	continuationToken := r.URL.Query().Get("continuation-token")
	maxKeysStr := r.URL.Query().Get("max-keys")
	maxKeys := 1000
	if maxKeysStr != "" {
		if mk, err := strconv.Atoi(maxKeysStr); err == nil && mk > 0 {
			maxKeys = mk
		}
	}

	items, nextCont, err := gw.meta.ListObjects(bucket, prefix, continuationToken, maxKeys)
	if err != nil {
		if errors.Is(err, metadata.ErrNoSuchBucket) {
			http.Error(w, "NoSuchBucket", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Output as XML (S3 standard)
	var sb strings.Builder
	sb.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	sb.WriteString("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n")
	sb.WriteString(fmt.Sprintf("  <Name>%s</Name>\n", xmlEscape(bucket)))
	sb.WriteString(fmt.Sprintf("  <Prefix>%s</Prefix>\n", xmlEscape(prefix)))
	sb.WriteString(fmt.Sprintf("  <MaxKeys>%d</MaxKeys>\n", maxKeys))
	sb.WriteString(fmt.Sprintf("  <IsTruncated>%t</IsTruncated>\n", nextCont != ""))
	if nextCont != "" {
		sb.WriteString(fmt.Sprintf("  <NextContinuationToken>%s</NextContinuationToken>\n", xmlEscape(nextCont)))
	}
	for _, it := range items {
		sb.WriteString("  <Contents>\n")
		sb.WriteString(fmt.Sprintf("    <Key>%s</Key>\n", xmlEscape(it.Key)))
		sb.WriteString(fmt.Sprintf("    <LastModified>%s</LastModified>\n", it.LastModified.UTC().Format(time.RFC3339)))
		sb.WriteString(fmt.Sprintf("    <ETag>&quot;%s&quot;</ETag>\n", it.ETag))
		sb.WriteString(fmt.Sprintf("    <Size>%d</Size>\n", it.Size))
		sb.WriteString("  </Contents>\n")
	}
	sb.WriteString("</ListBucketResult>")

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(sb.String()))
}

func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}
