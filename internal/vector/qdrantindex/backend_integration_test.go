//go:build sqlite_vec

package qdrantindex

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/vector"
	"go.kenn.io/msgvault/internal/vector/sqlitevec"
)

func TestQdrantIndexPublishesAndRecoversFromFailure(t *testing.T) {
	endpoint := os.Getenv("QDRANT_TEST_URL")
	if endpoint == "" {
		t.Skip("QDRANT_TEST_URL is not set")
	}
	ctx := context.Background()
	prefix := fmt.Sprintf("msgvault_test_%d", time.Now().UnixNano())
	client, err := New(endpoint, CollectionForGeneration(prefix, 1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.request(context.Background(), "DELETE", client.collectionPath(), nil, nil) })
	dir := t.TempDir()
	require.NoError(t, sqlitevec.RegisterExtension())
	mainPath := filepath.Join(dir, "main.db")
	mainDB, err := sql.Open(sqlitevec.DriverName(), mainPath)
	require.NoError(t, err)
	defer func() { _ = mainDB.Close() }()
	_, err = mainDB.Exec(`CREATE TABLE messages(id INTEGER PRIMARY KEY, subject TEXT, list_id TEXT, message_type TEXT NOT NULL DEFAULT 'email',
		source_id INTEGER, conversation_id INTEGER, sender_id INTEGER, has_attachments BOOLEAN, size_estimate INTEGER, sent_at DATETIME,
		deleted_at DATETIME, deleted_from_source_at DATETIME, embed_gen INTEGER);
		CREATE VIRTUAL TABLE messages_fts USING fts5(subject, body, content='', contentless_delete=1);
		CREATE TABLE message_labels(message_id INTEGER, label_id INTEGER, PRIMARY KEY(message_id,label_id));
		CREATE TABLE message_recipients(id INTEGER PRIMARY KEY, message_id INTEGER, recipient_type TEXT, participant_id INTEGER);
		INSERT INTO messages(id,subject,source_id,has_attachments) VALUES (1,'lunch plans',1,0),(2,'meeting notes',1,0),(3,'travel',1,0);
		INSERT INTO messages_fts(rowid,subject,body) VALUES (1,'lunch plans','tomorrow'),(2,'meeting notes','quarterly'),(3,'travel','flight');`)
	require.NoError(t, err)
	vecPath := filepath.Join(dir, "vectors.db")
	open := func() *sqlitevec.Backend {
		b, err := sqlitevec.Open(ctx, sqlitevec.Options{Path: vecPath, MainPath: mainPath, MainDB: mainDB, Dimension: 4})
		require.NoError(t, err)
		return b
	}
	source := open()
	gen, err := source.CreateGeneration(ctx, "test", 4, "")
	require.NoError(t, err)
	backend, err := Wrap(source, mainDB, endpoint, prefix)
	require.NoError(t, err)
	require.NoError(t, backend.Start(ctx))
	sourceID, err := source.QdrantSourceIdentity(ctx)
	require.NoError(t, err)
	require.NoError(t, client.EnsureCollection(ctx, 4, sourceID, int64(gen)))
	unit := func(axis int) []float32 { v := make([]float32, 4); v[axis] = 1; return v }
	require.NoError(t, backend.Upsert(ctx, gen, []vector.Chunk{{MessageID: 1, Vector: unit(0)}, {MessageID: 2, Vector: unit(1)}}))
	waitIndex(t, backend, client, 2)
	require.NoError(t, source.MarkQdrantReady(ctx, gen, client.CollectionName(), sourceID))
	bootID, err := client.RotateBootID(ctx, sourceID, int64(gen))
	require.NoError(t, err)
	require.NoError(t, source.SetQdrantBootID(ctx, gen, client.CollectionName(), sourceID, bootID))
	assert.True(t, backend.indexCurrent(ctx, gen))
	// An older SQLite snapshot would carry an older marker. A mismatch
	// rejects the collection even when its point count is still equal.
	require.NoError(t, source.SetQdrantBootID(ctx, gen, client.CollectionName(), sourceID, "stale"))
	assert.False(t, backend.indexCurrent(ctx, gen))
	require.NoError(t, source.SetQdrantBootID(ctx, gen, client.CollectionName(), sourceID, bootID))
	// Cosine would rank [2,0] ahead of [1,0.1], reversing SQLite L2.
	require.NoError(t, backend.Upsert(ctx, gen, []vector.Chunk{
		{MessageID: 1, Vector: []float32{1, 0.1, 0, 0}},
		{MessageID: 2, Vector: []float32{2, 0, 0, 0}},
	}))
	waitIndex(t, backend, client, 2)
	exact, err := source.Search(ctx, gen, unit(0), 2, vector.Filter{})
	require.NoError(t, err)
	nonunit, err := backend.Search(ctx, gen, unit(0), 2, vector.Filter{})
	require.NoError(t, err)
	require.Len(t, nonunit, 2)
	assert.Equal(t, exact[0].MessageID, nonunit[0].MessageID)
	assert.InDelta(t, exact[0].Score, nonunit[0].Score, 0.0001)
	require.NoError(t, backend.Upsert(ctx, gen, []vector.Chunk{{MessageID: 1, Vector: unit(0)}, {MessageID: 2, Vector: unit(1)}}))
	waitIndex(t, backend, client, 2)
	hits, err := backend.Search(ctx, gen, unit(0), 1, vector.Filter{})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, int64(1), hits[0].MessageID)
	fused, _, err := backend.FusedSearch(ctx, vector.FusedRequest{
		FTSTerms: []string{"lunch"}, QueryVec: unit(0), Generation: gen,
		KPerSignal: 2, Limit: 2, RRFK: 60, SubjectBoost: 2,
		SubjectTerms: []string{"lunch"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, fused)
	assert.Equal(t, int64(1), fused[0].MessageID)
	assert.True(t, fused[0].SubjectBoosted)
	_, err = mainDB.Exec(`UPDATE messages SET has_attachments=1 WHERE id=1`)
	require.NoError(t, err)
	attach := true
	hits, err = backend.Search(ctx, gen, unit(0), 1, vector.Filter{HasAttachment: &attach})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, int64(1), hits[0].MessageID)
	require.NoError(t, backend.Upsert(ctx, gen, []vector.Chunk{{MessageID: 1, Vector: unit(1)}}))
	waitIndex(t, backend, client, 2)
	require.NoError(t, backend.Delete(ctx, gen, []int64{2}))
	waitIndex(t, backend, client, 1)
	require.NoError(t, backend.Close())

	// A failed external write leaves the committed SQLite change pending.
	source = open()
	broken, err := Wrap(source, mainDB, "http://127.0.0.1:1", prefix)
	require.NoError(t, err)
	require.NoError(t, broken.Start(ctx))
	require.NoError(t, broken.Upsert(ctx, gen, []vector.Chunk{{MessageID: 3, Vector: unit(2)}}))
	require.Eventually(t, func() bool {
		pending, lastError, statusErr := broken.Status(ctx)
		return statusErr == nil && pending > 0 && lastError != ""
	}, 6*time.Second, 100*time.Millisecond)
	require.NoError(t, broken.Close())
	source = open()
	restarted, err := Wrap(source, mainDB, endpoint, prefix)
	require.NoError(t, err)
	require.NoError(t, restarted.Start(ctx))
	defer func() { _ = restarted.Close() }()
	waitIndex(t, restarted, client, 2)
	hits, err = restarted.Search(ctx, gen, unit(2), 1, vector.Filter{})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, int64(3), hits[0].MessageID)
}

func waitIndex(t *testing.T, b *Backend, client *Client, want int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		pending, _, err := b.Status(context.Background())
		if err != nil || pending != 0 {
			return false
		}
		count, err := client.Count(context.Background())
		return err == nil && count == want
	}, 15*time.Second, 100*time.Millisecond)
}
