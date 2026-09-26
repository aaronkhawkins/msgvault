package store_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

func TestAdoptedGmailThreadRoutesToExistingConversationWithoutMerging(t *testing.T) {
	st := testutil.NewSQLiteTestStore(t)
	src, err := st.GetOrCreateSource("gmail", "owner@example.test")
	require.NoError(t, err)
	first, err := st.EnsureConversation(src.ID, "legacy-a", "First")
	require.NoError(t, err)
	second, err := st.EnsureConversation(src.ID, "legacy-b", "Second")
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	_, err = st.DB().Exec(st.Rebind(`INSERT INTO gmail_thread_adoption
		(source_id, gmail_thread_id, conversation_id) VALUES (?, ?, ?)`),
		src.ID, "gmail-thread", first)
	require.NoError(t, err)
	got, err := st.EnsureConversation(src.ID, "gmail-thread", "New mail")
	require.NoError(t, err)
	require.Equal(t, first, got)
	var oldID int64
	require.NoError(t, st.DB().QueryRow(st.Rebind(`SELECT id FROM conversations
		WHERE source_id = ? AND source_conversation_id = ?`),
		src.ID, "legacy-b").Scan(&oldID))
	require.Equal(t, second, oldID)
}

func TestGmailSourceIDRekeyPreservesEmbeddingWatermark(t *testing.T) {
	st := testutil.NewSQLiteTestStore(t)
	src, err := st.GetOrCreateSource("imap", "legacy@example.test")
	require.NoError(t, err)
	conversationID, err := st.EnsureConversation(src.ID, "legacy-thread", "Legacy")
	require.NoError(t, err)
	messageID, err := st.UpsertMessage(&store.Message{
		SourceID: src.ID, ConversationID: conversationID,
		SourceMessageID: "All Mail|1", MessageType: "email",
		Subject: sql.NullString{String: "Synthetic", Valid: true},
	})
	require.NoError(t, err)
	_, err = st.DB().Exec(st.Rebind(`UPDATE messages SET embed_gen = ? WHERE id = ?`),
		int64(7), messageID)
	require.NoError(t, err)
	var beforeClock int64
	require.NoError(t, st.DB().QueryRow(`SELECT sequence FROM embedding_change_clock
		WHERE singleton = 1`).Scan(&beforeClock))
	rekeyed, err := st.RekeyMessageSourceID(messageID, "All Mail|1", "a1")
	require.NoError(t, err)
	require.True(t, rekeyed)
	var gotID, gotGeneration, afterClock int64
	var gotSourceID string
	require.NoError(t, st.DB().QueryRow(st.Rebind(`SELECT id, source_message_id,
		embed_gen FROM messages WHERE id = ?`), messageID).Scan(
		&gotID, &gotSourceID, &gotGeneration))
	require.Equal(t, messageID, gotID)
	require.Equal(t, "a1", gotSourceID)
	require.Equal(t, int64(7), gotGeneration)
	require.NoError(t, st.DB().QueryRow(`SELECT sequence FROM embedding_change_clock
		WHERE singleton = 1`).Scan(&afterClock))
	require.Equal(t, beforeClock, afterClock,
		"source identity rekey must not enqueue existing embeddings")
}
