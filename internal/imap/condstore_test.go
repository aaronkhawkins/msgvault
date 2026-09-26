package imap

import (
	"testing"

	imapv2 "github.com/emersion/go-imap/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCondstoreUnchangedMailboxPublishesNoOp(t *testing.T) {
	count := uint32(2)
	addr, server := startQresyncTestServer(t, qresyncServerConfig{
		capabilities: []string{"IMAP4rev1 ENABLE CONDSTORE"},
		uidValidity:  77, uidNext: 3, highestModSeq: 10,
		numMessages: &count, searchUIDs: []imapv2.UID{1, 2},
	})
	client := newQresyncTestClient(t, addr, map[string]FolderState{
		"INBOX": {UIDValidity: 77, UIDNext: 3, HighestModSeq: 10, KnownUIDs: []uint32{1, 2}},
	})
	assert.Empty(t, listQresyncMessages(t, client))
	deltas := client.ObservedMailboxDeltas()
	require.Len(t, deltas, 1)
	assert.True(t, deltas[0].Incremental)
	assert.Equal(t, []uint32{1, 2}, deltas[0].State.KnownUIDs)
	commands := joinedCommands(server.commandsFor(1))
	assert.NotContains(t, commands, "SELECT INBOX")
	assert.NotContains(t, commands, "UID SEARCH")
	assert.NotContains(t, commands, "UID FETCH")
}

func TestCondstoreAppendAndSameCountSwap(t *testing.T) {
	for _, tt := range []struct {
		name, wantListed string
		currentUIDs      []imapv2.UID
		count            uint32
		wantVanished     []imapv2.UID
	}{
		{name: "append", wantListed: "INBOX|3", currentUIDs: []imapv2.UID{1, 2, 3}, count: 3},
		{name: "same count swap", wantListed: "INBOX|3", currentUIDs: []imapv2.UID{1, 3}, count: 2, wantVanished: []imapv2.UID{2}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			addr, server := startQresyncTestServer(t, qresyncServerConfig{
				capabilities: []string{"IMAP4rev1 ENABLE CONDSTORE"},
				uidValidity:  77, uidNext: 4, highestModSeq: 20,
				numMessages: &tt.count, searchUIDs: tt.currentUIDs,
				fetchChanged: []imapv2.UID{3},
			})
			client := newQresyncTestClient(t, addr, map[string]FolderState{
				"INBOX": {UIDValidity: 77, UIDNext: 3, HighestModSeq: 10, KnownUIDs: []uint32{1, 2}},
			})
			assert.Equal(t, []string{tt.wantListed}, listQresyncMessages(t, client))
			deltas := client.ObservedMailboxDeltas()
			require.Len(t, deltas, 1)
			assert.Equal(t, tt.wantVanished, deltas[0].VanishedUIDs)
			assert.Equal(t, uidsToUint32(tt.currentUIDs), deltas[0].State.KnownUIDs)
			assert.True(t, deltas[0].Incremental)
			commands := joinedCommands(server.commandsFor(1))
			assert.Contains(t, commands, "UID SEARCH UID 3:*")
			assert.Contains(t, commands, "UID SEARCH UID 1:*")
			assert.Contains(t, commands, "CHANGEDSINCE 10")
			assert.NotContains(t, commands, "VANISHED")
		})
	}
}

func TestCondstoreInvalidBaselineFallsBackToFullEnumeration(t *testing.T) {
	for _, tt := range []struct {
		name  string
		prior FolderState
	}{
		{name: "missing membership baseline", prior: FolderState{UIDValidity: 77, UIDNext: 3, HighestModSeq: 10}},
		{name: "UIDVALIDITY changed", prior: FolderState{UIDValidity: 76, UIDNext: 3, HighestModSeq: 10, KnownUIDs: []uint32{1, 2}}},
		{name: "no saved modseq", prior: FolderState{UIDValidity: 77, UIDNext: 3, KnownUIDs: []uint32{1, 2}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			count := uint32(2)
			addr, server := startQresyncTestServer(t, qresyncServerConfig{
				capabilities: []string{"IMAP4rev1 ENABLE CONDSTORE"},
				uidValidity:  77, uidNext: 3, highestModSeq: 20,
				numMessages: &count, searchUIDs: []imapv2.UID{1, 2},
			})
			client := newQresyncTestClient(t, addr, map[string]FolderState{"INBOX": tt.prior})
			assert.Equal(t, []string{"INBOX|1", "INBOX|2"}, listQresyncMessages(t, client))
			deltas := client.ObservedMailboxDeltas()
			require.Len(t, deltas, 1)
			assert.True(t, deltas[0].Reset)
			assert.NotContains(t, joinedCommands(server.commandsFor(1)), "CHANGEDSINCE")
		})
	}
}

func TestCondstoreIncompleteSearchCannotPublishVanishedUIDs(t *testing.T) {
	count := uint32(3)
	addr, server := startQresyncTestServer(t, qresyncServerConfig{
		capabilities: []string{"IMAP4rev1 ENABLE CONDSTORE"},
		uidValidity:  77, uidNext: 4, highestModSeq: 20,
		numMessages: &count, selectExists: &count,
		searchUIDs:   []imapv2.UID{1, 2}, // server claims three live messages
		fetchChanged: []imapv2.UID{3},
	})
	client := newQresyncTestClient(t, addr, map[string]FolderState{
		"INBOX": {UIDValidity: 77, UIDNext: 3, HighestModSeq: 10, KnownUIDs: []uint32{1, 2}},
	})
	_ = listQresyncMessages(t, client)
	assert.Nil(t, client.ObservedMailboxDeltas(),
		"a second incomplete enumeration cannot retire a stored membership")
	assert.Nil(t, client.ObservedFolderStates())
	assert.NotContains(t, joinedCommands(server.commandsFor(2)), "CHANGEDSINCE")
}
