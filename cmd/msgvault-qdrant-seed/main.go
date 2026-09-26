// msgvault-qdrant-seed copies existing authoritative vectors into a derived
// Qdrant collection. It never updates the source SQLite database.
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

	"go.kenn.io/msgvault/internal/vector/qdrantindex"
	"go.kenn.io/msgvault/internal/vector/sqlitevec"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "qdrant seed:", err)
		os.Exit(1)
	}
}

func run() error {
	dbPath := flag.String("vectors", "", "read-only vectors.db path")
	endpoint := flag.String("endpoint", "http://qdrant:6333", "private Qdrant endpoint")
	prefix := flag.String("collection-prefix", "msgvault", "collection prefix")
	checkpoint := flag.String("checkpoint", "", "owner-only progress file")
	reconcileOnly := flag.Bool("reconcile-only", false, "compare point IDs against current SQLite rows and repair the index")
	flag.Parse()
	if *dbPath == "" || (!*reconcileOnly && *checkpoint == "") {
		return fmt.Errorf("vectors and checkpoint paths are required")
	}
	if err := sqlitevec.RegisterExtension(); err != nil {
		return err
	}
	db, err := sql.Open(sqlitevec.DriverName(), "file:"+*dbPath+"?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	var gen int64
	var dim int
	if err := db.QueryRowContext(ctx, "SELECT id, dimension FROM index_generations WHERE state='active'").Scan(&gen, &dim); err != nil {
		return err
	}
	client, err := qdrantindex.New(*endpoint, qdrantindex.CollectionForGeneration(*prefix, gen))
	if err != nil {
		return err
	}
	if err := client.EnsureCollection(ctx, dim); err != nil {
		return err
	}
	if *reconcileOnly {
		return reconcile(ctx, db, client, gen, dim)
	}
	var last uint64
	if data, err := os.ReadFile(*checkpoint); err == nil {
		if err := json.Unmarshal(data, &last); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
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
		if err := client.Upsert(ctx, batch); err != nil {
			return err
		}
		last = batch[len(batch)-1].ID
		count += len(batch)
		encoded, _ := json.Marshal(last)
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
	fmt.Printf("complete generation=%d indexed_this_run=%d qdrant_count=%d last_id=%d elapsed=%s norm_sample=[%.6f,%.6f] checkpoint=%s\n", gen, count, indexed, last, time.Since(start).Round(time.Second), minNorm, maxNorm, filepath.Base(*checkpoint))
	return nil
}

func reconcile(ctx context.Context, db *sql.DB, client *qdrantindex.Client, gen int64, dim int) error {
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
			if err := client.Upsert(ctx, points); err != nil {
				return err
			}
			points = points[:0]
		}
	}
	if err := client.Upsert(ctx, points); err != nil {
		return err
	}
	var extra []uint64
	for id := range indexed {
		extra = append(extra, id)
	}
	for len(extra) > 0 {
		n := min(len(extra), 256)
		if err := client.Delete(ctx, extra[:n]); err != nil {
			return err
		}
		extra = extra[n:]
	}
	actual, err := client.Count(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("reconciled generation=%d expected_at_scan=%d missing_repaired=%d extra_deleted=%d qdrant_count=%d\n", gen, expected, len(missing), len(indexed), actual)
	return nil
}
