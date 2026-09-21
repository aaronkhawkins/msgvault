package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.kenn.io/msgvault/internal/search"
)

func TestMessageSearchOrderByDropsPostgresRankArgumentForNewest(t *testing.T) {
	orderBy, orderArgCount := messageSearchOrderBy(
		search.ResultSortNewest,
		true,
		"ts_rank(m.search_vector, websearch_to_tsquery('simple', ?)) DESC",
		1,
	)

	assert.Equal(t, "COALESCE(m.sent_at, m.received_at, m.internal_date) DESC, m.id DESC", orderBy)
	assert.Zero(t, orderArgCount, "newest omits both the PostgreSQL rank expression and its bind argument")
}

func TestMessageSearchOrderByKeepsPostgresRankArgumentForRelevance(t *testing.T) {
	const rank = "ts_rank(m.search_vector, websearch_to_tsquery('simple', ?)) DESC"
	orderBy, orderArgCount := messageSearchOrderBy(search.ResultSortRelevance, true, rank, 1)

	assert.Equal(t,
		rank+", COALESCE(m.sent_at, m.received_at, m.internal_date) DESC, m.id DESC",
		orderBy,
	)
	assert.Equal(t, 1, orderArgCount)
}
