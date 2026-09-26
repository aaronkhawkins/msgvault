//go:build sqlite_vec

package qdrantindex

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"go.kenn.io/msgvault/internal/vector"
	"go.kenn.io/msgvault/internal/vector/hybrid"
	"go.kenn.io/msgvault/internal/vector/sqlitevec"
)

// Backend accelerates only message search. Every other vector capability is
// delegated to SQLite, which keeps ownership of generations and embeddings.
type Backend struct {
	*sqlitevec.Backend
	endpoint string
	prefix   string
	mainDB   *sql.DB
	mu       sync.RWMutex
	lastErr  error
	cancel   context.CancelFunc
	done     chan struct{}
}

var _ vector.FusingBackend = (*Backend)(nil)

func Wrap(source *sqlitevec.Backend, mainDB *sql.DB, endpoint, prefix string) (*Backend, error) {
	if _, err := New(endpoint, CollectionForGeneration(prefix, 1)); err != nil {
		return nil, err
	}
	return &Backend{Backend: source, endpoint: endpoint, prefix: prefix, mainDB: mainDB}, nil
}

func (b *Backend) client(gen vector.GenerationID) (*Client, error) {
	return New(b.endpoint, CollectionForGeneration(b.prefix, int64(gen)))
}

// Start installs the SQLite outbox before any later embedding write and
// drains it in the background. Read-only clients use Wrap without Start.
func (b *Backend) Start(ctx context.Context) error {
	if err := b.EnableQdrantChangeLog(ctx); err != nil {
		return err
	}
	if b.done != nil {
		return nil
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.done = make(chan struct{})
	go func() {
		defer close(b.done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			if err := b.drainOnce(workerCtx); err != nil && !errors.Is(err, context.Canceled) {
				b.mu.Lock()
				b.lastErr = err
				b.mu.Unlock()
			} else if err == nil {
				b.mu.Lock()
				b.lastErr = nil
				b.mu.Unlock()
			}
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}

func (b *Backend) Close() error {
	if b.cancel != nil {
		b.cancel()
		<-b.done
	}
	return b.Backend.Close()
}

// Status makes lag and failed publication visible without exposing content.
func (b *Backend) Status(ctx context.Context) (pending int64, lastError string, err error) {
	pending, err = b.PendingQdrantChanges(ctx)
	b.mu.RLock()
	if b.lastErr != nil {
		lastError = b.lastErr.Error()
	}
	b.mu.RUnlock()
	if err == nil && pending == 0 && lastError == "" {
		if gen, genErr := b.ActiveGeneration(ctx); genErr == nil {
			sourceID, identityErr := b.QdrantSourceIdentity(ctx)
			if identityErr != nil {
				return pending, identityErr.Error(), err
			}
			client, clientErr := b.client(gen.ID)
			if clientErr != nil {
				return pending, clientErr.Error(), err
			}
			ready, readyErr := b.QdrantReady(ctx, gen.ID, client.CollectionName(), sourceID)
			if readyErr != nil {
				return pending, readyErr.Error(), err
			}
			if !ready {
				return pending, "Qdrant collection has not completed reconciliation", err
			}
			checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			provenanceOK := client.ProvenanceMatches(checkCtx, sourceID, int64(gen.ID))
			cancel()
			if !provenanceOK {
				return pending, "Qdrant collection provenance mismatch or unavailable", err
			}
			var expected int64
			if countErr := b.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM embeddings WHERE generation_id=?`, int64(gen.ID)).Scan(&expected); countErr != nil {
				lastError = countErr.Error()
			} else {
				checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				indexed, countErr := client.Count(checkCtx)
				cancel()
				if countErr != nil {
					lastError = countErr.Error()
				} else if indexed != expected {
					lastError = "Qdrant point count differs from authoritative embeddings"
				}
			}
		}
	}
	return pending, lastError, err
}

func (b *Backend) drainOnce(ctx context.Context) error {
	changes, err := b.NextQdrantChanges(ctx, 128)
	if err != nil || len(changes) == 0 {
		return err
	}
	sourceID, err := b.QdrantSourceIdentity(ctx)
	if err != nil {
		return err
	}
	byGen := make(map[vector.GenerationID][]sqlitevec.QdrantChange)
	for _, change := range changes {
		byGen[change.Generation] = append(byGen[change.Generation], change)
	}
	for gen, group := range byGen {
		client, err := b.client(gen)
		if err != nil {
			return err
		}
		var upserts []Point
		var deletes []uint64
		for _, change := range group {
			if change.Delete {
				deletes = append(deletes, change.EmbeddingID)
				continue
			}
			_, messageID, chunkIndex, values, exists, err := b.QdrantPoint(ctx, change.EmbeddingID)
			if err != nil {
				return err
			}
			if !exists {
				deletes = append(deletes, change.EmbeddingID)
				continue
			}
			upserts = append(upserts, Point{ID: change.EmbeddingID, MessageID: messageID, ChunkIndex: chunkIndex, Vector: values})
		}
		if len(upserts) > 0 {
			if err := client.EnsureCollection(ctx, len(upserts[0].Vector), sourceID, int64(gen)); err != nil {
				return err
			}
			if err := client.Upsert(ctx, upserts); err != nil {
				return err
			}
		}
		if err := client.Delete(ctx, deletes); err != nil {
			return err
		}
	}
	return b.AckQdrantChanges(ctx, changes[len(changes)-1].Sequence)
}

func (b *Backend) Search(ctx context.Context, gen vector.GenerationID, queryVec []float32, k int, filter vector.Filter) ([]vector.Hit, error) {
	if err := vector.ValidateFilter(filter); err != nil {
		return nil, err
	}
	if k <= 0 {
		return nil, nil
	}
	if !b.indexCurrent(ctx, gen) {
		return b.Backend.Search(ctx, gen, queryVec, k, filter)
	}
	client, err := b.client(gen)
	if err != nil {
		return nil, err
	}
	limit := min(max(k*8, 128), vector.MaxFilterMessageIDs)
	for {
		searchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		matches, err := client.Search(searchCtx, queryVec, limit)
		cancel()
		if err != nil {
			return b.Backend.Search(ctx, gen, queryVec, k, filter)
		}
		pointIDs := make([]uint64, len(matches))
		for i, match := range matches {
			pointIDs[i] = match.ID
		}
		current, err := b.QdrantMessageIDsForEmbeddingIDs(ctx, gen, pointIDs)
		if err != nil {
			return nil, err
		}
		for _, match := range matches {
			if current[match.ID] != match.MessageID || match.MessageID == 0 {
				return b.Backend.Search(ctx, gen, queryVec, k, filter)
			}
		}
		ids := make([]int64, 0, len(matches))
		seen := make(map[int64]bool, len(matches))
		for _, match := range matches {
			if !seen[match.MessageID] {
				seen[match.MessageID] = true
				ids = append(ids, match.MessageID)
			}
		}
		allowed, err := b.FilterQdrantCandidates(ctx, ids, filter)
		if err != nil {
			return nil, err
		}
		var hits []vector.Hit
		for _, match := range matches {
			if !allowed[match.MessageID] {
				continue
			}
			// Qdrant Euclid reports L2 distance, matching sqlite-vec's
			// 1-distance score for vectors of any norm.
			score := 1 - match.Score
			hits = append(hits, vector.Hit{MessageID: match.MessageID, Score: score, Rank: len(hits) + 1})
			allowed[match.MessageID] = false
			if len(hits) == k {
				if !b.indexCurrent(ctx, gen) {
					return b.Backend.Search(ctx, gen, queryVec, k, filter)
				}
				return hits, nil
			}
		}
		if len(matches) < limit {
			if !b.indexCurrent(ctx, gen) {
				return b.Backend.Search(ctx, gen, queryVec, k, filter)
			}
			return hits, nil
		}
		if limit >= vector.MaxFilterMessageIDs {
			// A selective filter needs a wider candidate population than
			// the bounded ANN request. Preserve complete search semantics.
			return b.Backend.Search(ctx, gen, queryVec, k, filter)
		}
		limit = min(limit*2, vector.MaxFilterMessageIDs)
	}
}

func (b *Backend) indexCurrent(ctx context.Context, gen vector.GenerationID) bool {
	pending, err := b.PendingQdrantChanges(ctx)
	if err != nil || pending != 0 {
		return false
	}
	sourceID, err := b.QdrantSourceIdentity(ctx)
	if err != nil {
		return false
	}
	client, err := b.client(gen)
	if err != nil {
		return false
	}
	ready, err := b.QdrantReady(ctx, gen, client.CollectionName(), sourceID)
	if err != nil || !ready {
		return false
	}
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	provenanceOK := client.ProvenanceMatches(checkCtx, sourceID, int64(gen))
	cancel()
	if !provenanceOK {
		return false
	}
	var expected int64
	if err := b.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM embeddings WHERE generation_id=?`, int64(gen)).Scan(&expected); err != nil {
		return false
	}
	checkCtx, cancel = context.WithTimeout(ctx, 2*time.Second)
	indexed, err := client.Count(checkCtx)
	cancel()
	return err == nil && indexed == expected
}

func (b *Backend) FusedSearch(ctx context.Context, req vector.FusedRequest) ([]vector.FusedHit, bool, error) {
	if req.QueryVec == nil {
		return b.Backend.FusedSearch(ctx, req)
	}
	if len(req.FTSTerms) == 0 {
		vec, err := b.Search(ctx, req.Generation, req.QueryVec, req.KPerSignal+1, req.Filter)
		if err != nil {
			return nil, false, err
		}
		out := hybrid.Fuse(nil, vec[:min(len(vec), req.KPerSignal)], req.RRFK, 1, nil, nil)
		if len(out) > req.Limit {
			out = out[:req.Limit]
		}
		return out, len(vec) > req.KPerSignal, nil
	}
	ftsReq := req
	ftsReq.QueryVec = nil
	ftsReq.SubjectBoost = 1
	ftsReq.Limit = req.KPerSignal
	fts, ftsSaturated, err := b.Backend.FusedSearch(ctx, ftsReq)
	if err != nil {
		return nil, false, err
	}
	bm25 := make([]vector.Hit, len(fts))
	for i, h := range fts {
		bm25[i] = vector.Hit{MessageID: h.MessageID, Score: h.BM25Score, Rank: i + 1}
	}
	vec, err := b.Search(ctx, req.Generation, req.QueryVec, req.KPerSignal+1, req.Filter)
	if err != nil {
		return nil, false, err
	}
	vecSaturated := len(vec) > req.KPerSignal
	vec = vec[:min(len(vec), req.KPerSignal)]
	var subjects map[int64]string
	if req.SubjectBoost > 1 && len(req.SubjectTerms) > 0 {
		ids := make([]int64, 0, len(bm25)+len(vec))
		for _, hit := range bm25 {
			ids = append(ids, hit.MessageID)
		}
		for _, hit := range vec {
			ids = append(ids, hit.MessageID)
		}
		encoded, _ := json.Marshal(ids)
		rows, err := b.mainDB.QueryContext(ctx, `SELECT id, COALESCE(subject,'') FROM messages WHERE id IN (SELECT value FROM json_each(?))`, string(encoded))
		if err != nil {
			return nil, false, err
		}
		defer rows.Close()
		subjects = make(map[int64]string, len(ids))
		for rows.Next() {
			var id int64
			var subject string
			if err := rows.Scan(&id, &subject); err != nil {
				return nil, false, err
			}
			subjects[id] = subject
		}
		if err := rows.Err(); err != nil {
			return nil, false, err
		}
	}
	out := hybrid.Fuse(bm25, vec, req.RRFK, req.SubjectBoost, req.SubjectTerms, subjects)
	if len(out) > req.Limit {
		out = out[:req.Limit]
	}
	return out, ftsSaturated || vecSaturated, nil
}
