// msgvault-qdrant-seed copies existing authoritative vectors into a derived
// Qdrant collection. Its only SQLite writes create the outbox and readiness
// metadata; embedding and archive rows remain authoritative and untouched.
package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"go.kenn.io/msgvault/internal/vector"
	"go.kenn.io/msgvault/internal/vector/qdrantindex"
	"go.kenn.io/msgvault/internal/vector/sqlitevec"
)

type checkpointState struct {
	SourceID   string `json:"source_id"`
	Generation int64  `json:"generation_id"`
	Collection string `json:"collection"`
	LastID     uint64 `json:"last_id"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "qdrant seed:", err)
		os.Exit(1)
	}
}

func run() error {
	dbPath := flag.String("vectors", "", "vectors.db path")
	endpoint := flag.String("endpoint", "http://qdrant:6333", "private Qdrant endpoint")
	prefix := flag.String("collection-prefix", "msgvault", "collection prefix")
	checkpoint := flag.String("checkpoint", "", "owner-only progress file")
	reconcileOnly := flag.Bool("reconcile-only", false, "compare point IDs against current SQLite rows and repair the index")
	initOnly := flag.Bool("init", false, "install the transactional change log before seeding")
	flag.Parse()
	if *dbPath == "" || (!*reconcileOnly && !*initOnly && *checkpoint == "") {
		return fmt.Errorf("vectors and checkpoint paths are required")
	}
	if err := sqlitevec.RegisterExtension(); err != nil {
		return err
	}
	ctx := context.Background()
	if *initOnly {
		source, err := sqlitevec.Open(ctx, sqlitevec.Options{Path: *dbPath})
		if err != nil {
			return err
		}
		defer source.Close()
		if err := source.EnableQdrantChangeLog(ctx); err != nil {
			return err
		}
		fmt.Println("transactional Qdrant change log installed")
		return nil
	}
	db, err := sql.Open(sqlitevec.DriverName(), "file:"+*dbPath+"?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	var gen int64
	var dim int
	if err := db.QueryRowContext(ctx, "SELECT id, dimension FROM index_generations WHERE state='active'").Scan(&gen, &dim); err != nil {
		return err
	}
	var sourceID string
	if err := db.QueryRowContext(ctx, `SELECT source_id FROM qdrant_source WHERE id=1`).Scan(&sourceID); err != nil {
		return fmt.Errorf("install Qdrant change log first: %w", err)
	}
	client, err := qdrantindex.New(*endpoint, qdrantindex.CollectionForGeneration(*prefix, gen))
	if err != nil {
		return err
	}
	if err := client.EnsureCollection(ctx, dim, sourceID, gen); err != nil {
		return err
	}
	if *reconcileOnly {
		return reconcile(ctx, db, *dbPath, client, gen, dim, sourceID)
	}
	checkpointValue := checkpointState{SourceID: sourceID, Generation: gen, Collection: client.CollectionName()}
	if data, err := os.ReadFile(*checkpoint); err == nil {
		var previous checkpointState
		if err := json.Unmarshal(data, &previous); err != nil {
			return err
		}
		if previous.SourceID != sourceID || previous.Generation != gen || previous.Collection != client.CollectionName() {
			return fmt.Errorf("checkpoint belongs to another source, generation, or collection")
		}
		checkpointValue = previous
	} else if !os.IsNotExist(err) {
		return err
	}
	last := checkpointValue.LastID
	// Read vec0's chunk storage by the authoritative embedding rowid. A
	// virtual-table JOIN without a MATCH constraint scans the whole vector
	// corpus on every page, so the one-shot loader uses its stable shadow
	// layout while the application's normal reads keep using vec0.
	base := sqlitevec.VectorTableName(dim)
	query := fmt.Sprintf(`SELECT e.embedding_id, e.message_id, e.chunk_index,
		substr(vc.vectors, r.chunk_offset*?*4+1, ?*4)
		FROM embeddings e NOT INDEXED
		CROSS JOIN %s_rowids r ON r.rowid=e.embedding_id
		CROSS JOIN %s_vector_chunks00 vc ON vc.rowid=+r.chunk_id
		WHERE e.embedding_id>? AND e.generation_id=?
		ORDER BY e.embedding_id LIMIT 256`, base, base)
	planRows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, dim, dim, last, gen)
	if err != nil {
		return err
	}
	for planRows.Next() {
		var id, parent, aux int
		var detail string
		if err := planRows.Scan(&id, &parent, &aux, &detail); err != nil {
			planRows.Close()
			return err
		}
		fmt.Println("plan:", detail)
	}
	planRows.Close()
	start := time.Now()
	count := 0
	minNorm, maxNorm := math.Inf(1), 0.0
	for {
		rows, err := db.QueryContext(ctx, query, dim, dim, last, gen)
		if err != nil {
			return err
		}
		var batch []qdrantindex.Point
		for rows.Next() {
			var p qdrantindex.Point
			var blob []byte
			if err := rows.Scan(&p.ID, &p.MessageID, &p.ChunkIndex, &blob); err != nil {
				rows.Close()
				return err
			}
			if len(blob) != dim*4 {
				rows.Close()
				return fmt.Errorf("vector dimension mismatch at point %d", p.ID)
			}
			p.Vector = make([]float32, dim)
			var norm2 float64
			for i := range p.Vector {
				p.Vector[i] = math.Float32frombits(binary.LittleEndian.Uint32(blob[i*4:]))
				norm2 += float64(p.Vector[i]) * float64(p.Vector[i])
			}
			if count < 100 {
				norm := math.Sqrt(norm2)
				minNorm = math.Min(minNorm, norm)
				maxNorm = math.Max(maxNorm, norm)
			}
			batch = append(batch, p)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(batch) == 0 {
			break
		}
		if err := retryWrite(ctx, func() error { return client.Upsert(ctx, batch) }); err != nil {
			return err
		}
		last = batch[len(batch)-1].ID
		count += len(batch)
		checkpointValue.LastID = last
		encoded, _ := json.Marshal(checkpointValue)
		tmp := *checkpoint + ".tmp"
		if err := os.WriteFile(tmp, encoded, 0600); err != nil {
			return err
		}
		if err := os.Rename(tmp, *checkpoint); err != nil {
			return err
		}
		if count%2560 == 0 {
			fmt.Printf("indexed=%d last_id=%d elapsed=%s norm_sample=[%.6f,%.6f]\n", count, last, time.Since(start).Round(time.Second), minNorm, maxNorm)
		}
	}
	indexed, err := client.Count(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("seed pass complete generation=%d indexed_this_run=%d qdrant_count=%d last_id=%d elapsed=%s norm_sample=[%.6f,%.6f] checkpoint=%s; final reconciliation required\n", gen, count, indexed, last, time.Since(start).Round(time.Second), minNorm, maxNorm, filepath.Base(*checkpoint))
	return nil
}

func reconcile(ctx context.Context, db *sql.DB, dbPath string, client *qdrantindex.Client, gen int64, dim int, sourceID string) error {
	indexed := make(map[uint64]bool)
	var offset *uint64
	for {
		ids, next, err := client.ScrollIDs(ctx, offset, 1000)
		if err != nil {
			return err
		}
		for _, id := range ids {
			indexed[id] = true
		}
		if next == nil {
			break
		}
		offset = next
	}
	rows, err := db.QueryContext(ctx, `SELECT embedding_id FROM embeddings WHERE generation_id=?`, gen)
	if err != nil {
		return err
	}
	var missing []uint64
	var expected int64
	for rows.Next() {
		var id uint64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		expected++
		if indexed[id] {
			delete(indexed, id)
		} else {
			missing = append(missing, id)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	base := sqlitevec.VectorTableName(dim)
	lookup := fmt.Sprintf(`SELECT e.message_id,e.chunk_index,substr(vc.vectors,r.chunk_offset*?*4+1,?*4)
		FROM embeddings e CROSS JOIN %s_rowids r ON r.rowid=e.embedding_id
		CROSS JOIN %s_vector_chunks00 vc ON vc.rowid=+r.chunk_id
		WHERE e.embedding_id=? AND e.generation_id=?`, base, base)
	var points []qdrantindex.Point
	for _, id := range missing {
		var p qdrantindex.Point
		var blob []byte
		if err := db.QueryRowContext(ctx, lookup, dim, dim, id, gen).Scan(&p.MessageID, &p.ChunkIndex, &blob); err != nil {
			if err == sql.ErrNoRows {
				continue
			} // concurrent deletion; outbox will publish it
			return err
		}
		p.ID = id
		if len(blob) != dim*4 {
			return fmt.Errorf("vector dimension mismatch at point %d", id)
		}
		p.Vector = make([]float32, dim)
		for i := range p.Vector {
			p.Vector[i] = math.Float32frombits(binary.LittleEndian.Uint32(blob[i*4:]))
		}
		points = append(points, p)
		if len(points) == 128 {
			if err := retryWrite(ctx, func() error { return client.Upsert(ctx, points) }); err != nil {
				return err
			}
			points = points[:0]
		}
	}
	if err := retryWrite(ctx, func() error { return client.Upsert(ctx, points) }); err != nil {
		return err
	}
	var extra []uint64
	for id := range indexed {
		extra = append(extra, id)
	}
	for len(extra) > 0 {
		n := min(len(extra), 256)
		if err := retryWrite(ctx, func() error { return client.Delete(ctx, extra[:n]) }); err != nil {
			return err
		}
		extra = extra[n:]
	}
	actual, err := client.Count(ctx)
	if err != nil {
		return err
	}
	var pending int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM qdrant_changes`).Scan(&pending); err != nil {
		return err
	}
	if actual != expected && pending == 0 {
		return fmt.Errorf("Qdrant count differs from authoritative rows after reconciliation")
	}
	writer, err := sqlitevec.Open(ctx, sqlitevec.Options{Path: dbPath})
	if err != nil {
		return err
	}
	if err := writer.MarkQdrantReady(ctx, vector.GenerationID(gen), client.CollectionName(), sourceID); err != nil {
		writer.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	fmt.Printf("reconciled generation=%d expected_at_scan=%d missing_repaired=%d extra_deleted=%d qdrant_count=%d\n", gen, expected, len(missing), len(indexed), actual)
	return nil
}

func retryWrite(ctx context.Context, write func() error) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		if err = write(); err == nil {
			return nil
		}
		if attempt == 4 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(1<<attempt) * time.Second):
		}
	}
	return err
}
