package store_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

func TestAdoptedGmailThreadRoutesToExistingConversationWithoutMerging(t *testing.T) {
	requirements := require.New(t)
	st := testutil.NewSQLiteTestStore(t)
	src, err := st.GetOrCreateSource("gmail", "owner@example.test")
	requirements.NoError(err)
	first, err := st.EnsureConversation(src.ID, "legacy-a", "First")
	requirements.NoError(err)
	second, err := st.EnsureConversation(src.ID, "legacy-b", "Second")
	requirements.NoError(err)
	requirements.NotEqual(first, second)
	_, err = st.DB().Exec(st.Rebind(`INSERT INTO gmail_thread_adoption
		(source_id, gmail_thread_id, conversation_id) VALUES (?, ?, ?)`),
		src.ID, "gmail-thread", first)
	requirements.NoError(err)
	got, err := st.EnsureConversation(src.ID, "gmail-thread", "New mail")
	requirements.NoError(err)
	requirements.Equal(first, got)
	var oldID int64
	requirements.NoError(st.DB().QueryRow(st.Rebind(`SELECT id FROM conversations
		WHERE source_id = ? AND source_conversation_id = ?`),
		src.ID, "legacy-b").Scan(&oldID))
	requirements.Equal(second, oldID)
}

func TestGmailSourceIDRekeyPreservesEmbeddingWatermark(t *testing.T) {
	requirements := require.New(t)
	st := testutil.NewSQLiteTestStore(t)
	src, err := st.GetOrCreateSource("imap", "legacy@example.test")
	requirements.NoError(err)
	conversationID, err := st.EnsureConversation(src.ID, "legacy-thread", "Legacy")
	requirements.NoError(err)
	messageID, err := st.UpsertMessage(&store.Message{
		SourceID: src.ID, ConversationID: conversationID,
		SourceMessageID: "All Mail|1", MessageType: "email",
		Subject: sql.NullString{String: "Synthetic", Valid: true},
	})
	requirements.NoError(err)
	_, err = st.DB().Exec(st.Rebind(`UPDATE messages SET embed_gen = ? WHERE id = ?`),
		int64(7), messageID)
	requirements.NoError(err)
	var beforeClock int64
	requirements.NoError(st.DB().QueryRow(`SELECT sequence FROM embedding_change_clock
		WHERE singleton = 1`).Scan(&beforeClock))
	rekeyed, err := st.RekeyMessageSourceID(messageID, "All Mail|1", "a1")
	requirements.NoError(err)
	requirements.True(rekeyed)
	var gotID, gotGeneration, afterClock int64
	var gotSourceID string
	requirements.NoError(st.DB().QueryRow(st.Rebind(`SELECT id, source_message_id,
		embed_gen FROM messages WHERE id = ?`), messageID).Scan(
		&gotID, &gotSourceID, &gotGeneration))
	requirements.Equal(messageID, gotID)
	requirements.Equal("a1", gotSourceID)
	requirements.Equal(int64(7), gotGeneration)
	requirements.NoError(st.DB().QueryRow(`SELECT sequence FROM embedding_change_clock
		WHERE singleton = 1`).Scan(&afterClock))
	requirements.Equal(beforeClock, afterClock,
		"source identity rekey must not enqueue existing embeddings")
}
