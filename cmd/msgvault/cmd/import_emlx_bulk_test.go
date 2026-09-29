package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmlxBulkSQLiteOnlyChangesOptedInImportConnection(t *testing.T) {
	withStoreResolverConfig(t, lifecycleTestConfig(t.TempDir()))
	for _, tt := range []struct {
		name string
		bulk bool
		want int
	}{
		{name: "default full", bulk: false, want: 2},
		{name: "opt in normal", bulk: true, want: 1},
		{name: "default full after bulk", bulk: false, want: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st, cleanup, err := openWritableStoreAndInitForEmlxImport(tt.bulk)
			require.NoError(t, err)
			defer cleanup()

			var synchronous int
			require.NoError(t, st.DB().QueryRow("PRAGMA synchronous").Scan(&synchronous))
			require.Equal(t, tt.want, synchronous)
		})
	}
}
