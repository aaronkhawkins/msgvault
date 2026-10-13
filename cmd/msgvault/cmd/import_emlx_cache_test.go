package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
)

func TestImportEmlxDeferredCacheRetainsIngestionAndErrors(t *testing.T) {
	for _, tt := range []struct {
		name       string
		deferCache bool
		failIngest bool
	}{
		{name: "default refreshes cache"},
		{name: "deferred cache", deferCache: true},
		{name: "deferred import error", deferCache: true, failIngest: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			markDaemonCLISubprocessForTest(t)
			dataDir := t.TempDir()
			testCfg := lifecycleTestConfig(dataDir)
			withStoreResolverConfig(t, testCfg)
			oldIdentifier, oldResume, oldDeferred := importEmlxIdentifier, importEmlxNoResume, importEmlxDeferCache
			oldChanged := importEmlxCmd.Flags().Lookup("defer-cache").Changed
			oldOut, oldErr, oldLogger := importEmlxCmd.OutOrStdout(), importEmlxCmd.ErrOrStderr(), logger
			oldHook := buildCacheBeforeMessagesExportHook
			oldContext := importEmlxCmd.Context()
			importEmlxCmd.SetContext(context.Background())
			t.Cleanup(func() {
				importEmlxIdentifier, importEmlxNoResume, importEmlxDeferCache = oldIdentifier, oldResume, oldDeferred
				importEmlxCmd.Flags().Lookup("defer-cache").Changed = oldChanged
				importEmlxCmd.SetOut(oldOut)
				importEmlxCmd.SetErr(oldErr)
				logger = oldLogger
				buildCacheBeforeMessagesExportHook = oldHook
				importEmlxCmd.SetContext(oldContext)
			})
			importEmlxIdentifier, importEmlxNoResume = "archive@example.test", true
			require.NoError(t, importEmlxCmd.Flags().Set("defer-cache", strconv.FormatBool(tt.deferCache)))
			importEmlxCmd.SetOut(io.Discard)
			importEmlxCmd.SetErr(io.Discard)
			logger = slog.New(slog.NewTextHandler(io.Discard, nil))

			mail := filepath.Join(dataDir, "Mail", "Inbox.mbox", "Messages")
			require.NoError(t, os.MkdirAll(mail, 0o700))
			raw := "From: sender@example.test\r\nTo: archive@example.test\r\nSubject: synthetic deferred cache\r\nDate: Mon, 12 Oct 2026 12:00:00 +0000\r\n\r\nSynthetic body.\r\n"
			contents := []byte(fmt.Sprintf("%d\n%s", len(raw), raw))
			require.NoError(t, os.WriteFile(filepath.Join(mail, "1.emlx"), contents, 0o600))
			if tt.failIngest {
				st, err := store.Open(testCfg.DatabaseDSN())
				require.NoError(t, err)
				require.NoError(t, st.InitSchema())
				_, err = st.DB().Exec(`CREATE TRIGGER reject_synthetic_message BEFORE INSERT ON messages
					BEGIN SELECT RAISE(ABORT, 'synthetic ingestion failure'); END`)
				require.NoError(t, err)
				require.NoError(t, st.Close())
			}

			cacheError := errors.New("synthetic cache refresh failure")
			cacheVisited := false
			buildCacheBeforeMessagesExportHook = func() error {
				cacheVisited = true
				return cacheError
			}
			err := importEmlxCmd.RunE(importEmlxCmd, []string{filepath.Join(dataDir, "Mail")})
			switch {
			case tt.failIngest:
				require.Error(t, err, "deferring derived work must not hide an import error")
				assert.NotErrorIs(t, err, cacheError)
			case tt.deferCache:
				require.NoError(t, err)
			default:
				require.ErrorIs(t, err, cacheError, "default imports must still report cache failures")
			}
			assert.Equal(t, !tt.deferCache, cacheVisited)

			st, err := store.Open(testCfg.DatabaseDSN())
			require.NoError(t, err)
			defer func() { require.NoError(t, st.Close()) }()
			var count, errorsCount int
			var status string
			require.NoError(t, st.DB().QueryRow("SELECT count(*) FROM messages").Scan(&count))
			if tt.failIngest {
				assert.Zero(t, count)
			} else {
				assert.Equal(t, 1, count)
			}
			require.NoError(t, st.DB().QueryRow("SELECT status,errors_count FROM sync_runs ORDER BY id DESC LIMIT 1").Scan(&status, &errorsCount))
			if tt.failIngest {
				assert.Equal(t, "failed", status)
				assert.Positive(t, errorsCount)
				return
			}
			assert.Equal(t, "completed", status)
			assert.Zero(t, errorsCount)
			require.NoError(t, st.DB().QueryRow("SELECT count(*) FROM messages_fts WHERE messages_fts MATCH 'synthetic'").Scan(&count))
			assert.Equal(t, 1, count, "live full-text search must retain the imported message")
		})
	}
}
