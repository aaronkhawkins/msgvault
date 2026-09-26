//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"go.kenn.io/msgvault/internal/vector"
)

// QdrantChange is a committed embedding mutation awaiting publication to the
// derived search index. The row is retained until the external write succeeds.
type QdrantChange struct {
	Sequence    int64
	Generation  vector.GenerationID
	EmbeddingID uint64
	Delete      bool
}

// EnableQdrantChangeLog installs a small transactional outbox. SQLite records
// inserts and deletes in the same transaction as authoritative embedding rows.
func (b *Backend) EnableQdrantChangeLog(ctx context.Context) error {
	_, err := b.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS qdrant_source (
			id INTEGER PRIMARY KEY CHECK(id=1),
			source_id TEXT NOT NULL
		);
		INSERT OR IGNORE INTO qdrant_source(id, source_id) VALUES (1, lower(hex(randomblob(16))));
		CREATE TABLE IF NOT EXISTS qdrant_ready (
			generation_id INTEGER PRIMARY KEY,
			collection_name TEXT NOT NULL,
			source_id TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS qdrant_changes (
			sequence INTEGER PRIMARY KEY AUTOINCREMENT,
			generation_id INTEGER NOT NULL,
			embedding_id INTEGER NOT NULL,
			deleted INTEGER NOT NULL
		);
		CREATE TRIGGER IF NOT EXISTS qdrant_embeddings_insert AFTER INSERT ON embeddings BEGIN
			INSERT INTO qdrant_changes(generation_id, embedding_id, deleted) VALUES (NEW.generation_id, NEW.embedding_id, 0);
		END;
		CREATE TRIGGER IF NOT EXISTS qdrant_embeddings_delete AFTER DELETE ON embeddings BEGIN
			INSERT INTO qdrant_changes(generation_id, embedding_id, deleted) VALUES (OLD.generation_id, OLD.embedding_id, 1);
		END;
	`)
	return err
}

func (b *Backend) QdrantSourceIdentity(ctx context.Context) (string, error) {
	var id string
	err := b.db.QueryRowContext(ctx, `SELECT source_id FROM qdrant_source WHERE id=1`).Scan(&id)
	return id, err
}

func (b *Backend) QdrantReady(ctx context.Context, gen vector.GenerationID, collection string, sourceID string) (bool, error) {
	var exists bool
	err := b.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM qdrant_ready WHERE generation_id=? AND collection_name=? AND source_id=?)`, int64(gen), collection, sourceID).Scan(&exists)
	return exists, err
}

func (b *Backend) MarkQdrantReady(ctx context.Context, gen vector.GenerationID, collection, sourceID string) error {
	current, err := b.QdrantSourceIdentity(ctx)
	if err != nil {
		return err
	}
	if current != sourceID {
		return fmt.Errorf("Qdrant source identity changed")
	}
	_, err = b.db.ExecContext(ctx, `INSERT INTO qdrant_ready(generation_id,collection_name,source_id) VALUES(?,?,?)
		ON CONFLICT(generation_id) DO UPDATE SET collection_name=excluded.collection_name,source_id=excluded.source_id`, int64(gen), collection, sourceID)
	return err
}

func (b *Backend) PendingQdrantChanges(ctx context.Context) (int64, error) {
	var count int64
	err := b.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM qdrant_changes`).Scan(&count)
	return count, err
}

func (b *Backend) NextQdrantChanges(ctx context.Context, limit int) ([]QdrantChange, error) {
	if limit < 1 || limit > 1024 {
		return nil, fmt.Errorf("invalid qdrant change limit")
	}
	rows, err := b.db.QueryContext(ctx, `SELECT sequence, generation_id, embedding_id, deleted FROM qdrant_changes ORDER BY sequence LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QdrantChange
	for rows.Next() {
		var change QdrantChange
		if err := rows.Scan(&change.Sequence, &change.Generation, &change.EmbeddingID, &change.Delete); err != nil {
			return nil, err
		}
		out = append(out, change)
	}
	return out, rows.Err()
}

func (b *Backend) AckQdrantChanges(ctx context.Context, throughSequence int64) error {
	_, err := b.db.ExecContext(ctx, `DELETE FROM qdrant_changes WHERE sequence <= ?`, throughSequence)
	return err
}

// QdrantPoint loads the current full-precision vector for an outbox insert.
// A later delete may have removed it already, in which case exists is false.
func (b *Backend) QdrantPoint(ctx context.Context, embeddingID uint64) (gen vector.GenerationID, messageID int64, chunkIndex int, values []float32, exists bool, err error) {
	var dim int
	err = b.db.QueryRowContext(ctx, `SELECT g.id, g.dimension, e.message_id, e.chunk_index
		FROM embeddings e JOIN index_generations g ON g.id=e.generation_id
		WHERE e.embedding_id=?`, embeddingID).Scan(&gen, &dim, &messageID, &chunkIndex)
	if err == sql.ErrNoRows {
		return 0, 0, 0, nil, false, nil
	}
	if err != nil {
		return 0, 0, 0, nil, false, err
	}
	var blob []byte
	base := VectorTableName(dim)
	err = b.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT substr(vc.vectors, r.chunk_offset*?*4+1, ?*4)
		FROM %s_rowids r CROSS JOIN %s_vector_chunks00 vc ON vc.rowid=+r.chunk_id
		WHERE r.rowid=?`, base, base), dim, dim, embeddingID).Scan(&blob)
	if err == sql.ErrNoRows {
		return 0, 0, 0, nil, false, nil
	}
	if err != nil {
		return 0, 0, 0, nil, false, err
	}
	values, err = blobToFloat32(blob, dim)
	return gen, messageID, chunkIndex, values, err == nil, err
}

// FilterQdrantCandidates applies the current canonical message metadata to a
// bounded ANN candidate list. No metadata is copied into Qdrant, so label,
// recipient, source, deletion, and other filter changes take effect at once.
func (b *Backend) FilterQdrantCandidates(ctx context.Context, candidates []int64, filter vector.Filter) (map[int64]bool, error) {
	if len(candidates) == 0 {
		return map[int64]bool{}, nil
	}
	if len(candidates) > vector.MaxFilterMessageIDs {
		return nil, vector.ErrFilterTooLarge
	}
	if len(filter.MessageIDs) > 0 {
		allowed := make(map[int64]bool, len(filter.MessageIDs))
		for _, id := range filter.MessageIDs {
			allowed[id] = true
		}
		trimmed := candidates[:0]
		for _, id := range candidates {
			if allowed[id] {
				trimmed = append(trimmed, id)
			}
		}
		candidates = trimmed
		if len(candidates) == 0 {
			return map[int64]bool{}, nil
		}
	}
	filter.MessageIDs = candidates
	ids, err := b.filteredMessageIDs(ctx, filter)
	if err != nil {
		return nil, err
	}
	result := make(map[int64]bool, len(ids))
	for _, id := range ids {
		result[id] = true
	}
	return result, nil
}

// QdrantMessageIDsForEmbeddingIDs maps a bounded set of current point IDs to
// their message IDs for reconciliation checks without decoding vectors.
func (b *Backend) QdrantMessageIDsForEmbeddingIDs(ctx context.Context, gen vector.GenerationID, ids []uint64) (map[uint64]int64, error) {
	encoded, err := json.Marshal(ids)
	if err != nil {
		return nil, err
	}
	rows, err := b.db.QueryContext(ctx, `SELECT embedding_id, message_id FROM embeddings WHERE generation_id=? AND embedding_id IN (SELECT value FROM json_each(?))`, int64(gen), string(encoded))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[uint64]int64, len(ids))
	for rows.Next() {
		var id uint64
		var messageID int64
		if err := rows.Scan(&id, &messageID); err != nil {
			return nil, err
		}
		result[id] = messageID
	}
	return result, rows.Err()
}
