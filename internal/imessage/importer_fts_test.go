//go:build fts5

package imessage

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/testutil"
)

func TestImportIndexesMessagesAndRecipients(t *testing.T) {
	testutil.SkipIfPostgres(t, "directly queries the SQLite FTS5 table")
	chatPath := filepath.Join(t.TempDir(), "chat.db")
	chat, err := sql.Open("sqlite3", chatPath)
	require.NoError(t, err)
	_, err = chat.Exec(`PRAGMA journal_mode=WAL`)
	require.NoError(t, err)
	for _, statement := range []string{
		`CREATE TABLE message (ROWID INTEGER PRIMARY KEY, guid TEXT, text TEXT, attributedBody BLOB, date INTEGER, is_from_me INTEGER, service TEXT, cache_has_attachments INTEGER, handle_id INTEGER)`,
		`CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT)`,
		`CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, display_name TEXT, chat_identifier TEXT)`,
		`CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER)`,
		`CREATE TABLE chat_handle_join (chat_id INTEGER, handle_id INTEGER)`,
		`INSERT INTO handle VALUES (1, 'alice@example.com'), (2, '+15551234567')`,
		`INSERT INTO chat VALUES (1, 'group;+;synthetic', 'Synthetic group', NULL)`,
		`INSERT INTO chat_handle_join VALUES (1, 1), (1, 2)`,
		`INSERT INTO message VALUES (1, 'inbound', 'inboundtoken', NULL, 725760000, 0, 'iMessage', 0, 1), (2, 'outbound', 'outboundtoken', NULL, 725760001, 1, 'iMessage', 0, NULL)`,
		`INSERT INTO chat_message_join VALUES (1, 1), (1, 2)`,
	} {
		_, err := chat.Exec(statement)
		require.NoError(t, err, "create synthetic chat.db")
	}
	require.NoError(t, chat.Close())

	st := testutil.NewTestStore(t)
	require.True(t, st.FTS5Available())
	source, err := st.GetOrCreateSource("imessage", "synthetic-device")
	require.NoError(t, err)
	client, err := NewClient(chatPath, WithOwnerHandle("owner@example.com"))
	require.NoError(t, err)
	defer func() { assert.NoError(t, client.Close()) }()

	for range 2 {
		summary, err := client.Import(context.Background(), st, source.ID)
		require.NoError(t, err)
		assert.Equal(t, 2, summary.MessagesImported)
		assert.Zero(t, summary.Skipped)
	}

	rows, err := st.DB().Query(`SELECT m.source_message_id, f.body, f.from_addr, f.to_addr
		FROM messages_fts f JOIN messages m ON m.id = f.message_id ORDER BY m.source_message_id`)
	require.NoError(t, err)
	defer func() { assert.NoError(t, rows.Close()) }()
	var indexed []struct{ id, body, from, to string }
	for rows.Next() {
		var row struct{ id, body, from, to string }
		require.NoError(t, rows.Scan(&row.id, &row.body, &row.from, &row.to))
		indexed = append(indexed, row)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []struct{ id, body, from, to string }{
		{"1", "inboundtoken", "alice@example.com", "owner@example.com"},
		{"2", "outboundtoken", "owner@example.com", "alice@example.com +15551234567"},
	}, indexed)
	for _, term := range []string{"inboundtoken", "outboundtoken", "alice", "owner", "15551234567"} {
		var hits int
		require.NoError(t, st.DB().QueryRow(`SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH ?`, term).Scan(&hits))
		assert.Positive(t, hits, "search term %q", term)
	}
	assert.False(t, st.NeedsFTSBackfillQuick(), "complete imports should not need a full FTS backfill")

	chat, err = sql.Open("sqlite3", chatPath)
	require.NoError(t, err)
	_, err = chat.Exec(`UPDATE message SET text = 'editedtoken' WHERE ROWID = 1`)
	require.NoError(t, err)
	require.NoError(t, chat.Close())
	summary, err := client.Import(context.Background(), st, source.ID)
	require.NoError(t, err)
	assert.Equal(t, 2, summary.MessagesImported)
	var oldHits, newHits, total int
	require.NoError(t, st.DB().QueryRow(`SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'inboundtoken'`).Scan(&oldHits))
	require.NoError(t, st.DB().QueryRow(`SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'editedtoken'`).Scan(&newHits))
	require.NoError(t, st.DB().QueryRow(`SELECT COUNT(*) FROM messages_fts`).Scan(&total))
	assert.Zero(t, oldHits)
	assert.Equal(t, 1, newHits)
	assert.Equal(t, 2, total)
	assert.False(t, st.NeedsFTSBackfillQuick())

	// An empty source text leaves the previously archived body intact.
	// Re-importing it must not clear the corresponding search text.
	chat, err = sql.Open("sqlite3", chatPath)
	require.NoError(t, err)
	_, err = chat.Exec(`UPDATE message SET text = '' WHERE ROWID = 1`)
	require.NoError(t, err)
	require.NoError(t, chat.Close())
	_, err = client.Import(context.Background(), st, source.ID)
	require.NoError(t, err)
	require.NoError(t, st.DB().QueryRow(`SELECT COUNT(*) FROM messages_fts WHERE messages_fts MATCH 'editedtoken'`).Scan(&newHits))
	assert.Equal(t, 1, newHits)
}
