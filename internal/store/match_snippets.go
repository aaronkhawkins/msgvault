package store

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"
)

const matchSnippetLimit = 300

// MatchBodySnippets returns one body excerpt per message on the current
// result page. The lookup is restricted to those row ids and the same
// boolean match, so it does not scan the corpus again. An empty map means
// no body excerpt was available. Callers should still return the search
// result when this query fails.
func (s *Store) MatchBodySnippets(ctx context.Context, ids []int64, clauses [][]string) (map[int64]string, error) {
	if s == nil || len(ids) == 0 || len(clauses) == 0 || !s.fts5Available {
		return map[int64]string{}, nil
	}
	switch s.dialect.(type) {
	case *SQLiteDialect:
		return s.matchSQLiteBodySnippets(ctx, ids, clauses)
	case *PostgreSQLDialect:
		return s.matchPostgresBodySnippets(ctx, ids, clauses)
	default:
		return map[int64]string{}, nil
	}
}

func (s *Store) matchSQLiteBodySnippets(ctx context.Context, ids []int64, clauses [][]string) (map[int64]string, error) {
	match := (&SQLiteDialect{}).BuildFTSArgClauses(clauses)
	if match == "" {
		return map[int64]string{}, nil
	}
	query := fmt.Sprintf(`
		SELECT rowid, snippet(messages_fts, 2, '', '', ' … ', 16)
		FROM messages_fts
		WHERE rowid IN (%s) AND messages_fts MATCH ?
	`, inPlaceholders(len(ids)))
	args := append(idArgs(ids), match)
	return s.scanBodySnippets(ctx, query, args)
}

func (s *Store) matchPostgresBodySnippets(ctx context.Context, ids []int64, clauses [][]string) (map[int64]string, error) {
	match := (&PostgreSQLDialect{}).BuildFTSArgClauses(clauses)
	if match == "" {
		return map[int64]string{}, nil
	}
	query := fmt.Sprintf(`
		SELECT mb.message_id,
		       ts_headline('simple', COALESCE(mb.body_text, ''), to_tsquery('simple', ?),
		         'MaxFragments=1, MaxWords=20, MinWords=5, ShortWord=0')
		FROM message_bodies mb
		WHERE mb.message_id IN (%s)
		  AND to_tsvector('simple', COALESCE(mb.body_text, '')) @@ to_tsquery('simple', ?)
	`, inPlaceholders(len(ids)))
	args := append([]any{match}, idArgs(ids)...)
	args = append(args, match)
	return s.scanBodySnippets(ctx, query, args)
}

func (s *Store) scanBodySnippets(ctx context.Context, query string, args []any) (map[int64]string, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("match body snippets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	found := make(map[int64]string)
	for rows.Next() {
		var id int64
		var snippet string
		if err := rows.Scan(&id, &snippet); err != nil {
			return nil, fmt.Errorf("scan body snippet: %w", err)
		}
		snippet = boundSnippet(snippet)
		if snippet == "" {
			continue
		}
		found[id] = snippet
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate body snippets: %w", err)
	}
	return found, nil
}

func inPlaceholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func idArgs(ids []int64) []any {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return args
}

func boundSnippet(text string) string {
	text = strings.TrimSpace(text)
	text = strings.NewReplacer("<b>", "", "</b>", "").Replace(text)
	if len(text) <= matchSnippetLimit {
		return strings.TrimSpace(text)
	}
	cut := matchSnippetLimit
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return strings.TrimSpace(text[:cut])
}
