package qdrantindex

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Point is one authoritative message chunk mirrored into Qdrant.
type Point struct {
	ID         uint64
	MessageID  int64
	ChunkIndex int
	Vector     []float32
}

type Match struct {
	ID         uint64
	MessageID  int64
	ChunkIndex int
	Score      float64
}

type Client struct {
	base       string
	collection string
	http       *http.Client
}

func New(baseURL, collection string) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("qdrant endpoint must be an http URL without credentials or query")
	}
	if collection == "" || strings.ContainsAny(collection, "/?# ") {
		return nil, errors.New("invalid qdrant collection name")
	}
	return &Client{base: strings.TrimRight(baseURL, "/"), collection: collection, http: &http.Client{Timeout: 60 * time.Second}}, nil
}

func (c *Client) request(ctx context.Context, method, path string, body any, result any) error {
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader) //nolint:gosec // Endpoint is explicit operator configuration on a private service.
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req) //nolint:gosec // Only the validated operator-configured Qdrant endpoint is contacted.
	if err != nil {
		return fmt.Errorf("qdrant request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("qdrant %s: HTTP %d", method, resp.StatusCode)
	}
	var envelope struct {
		Status any             `json:"status"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&envelope); err != nil {
		return fmt.Errorf("decode qdrant response: %w", err)
	}
	if result != nil {
		return json.Unmarshal(envelope.Result, result)
	}
	return nil
}

func (c *Client) collectionPath() string { return "/collections/" + url.PathEscape(c.collection) }

type collectionInfo struct {
	Config struct {
		Metadata struct {
			SourceID   string `json:"source_id"`
			Generation int64  `json:"generation_id"`
			BootID     string `json:"boot_id"`
		} `json:"metadata"`
		Params struct {
			Vectors struct {
				Size     int    `json:"size"`
				Distance string `json:"distance"`
			} `json:"vectors"`
		} `json:"params"`
	} `json:"config"`
}

func (c *Client) collectionInfo(ctx context.Context) (collectionInfo, error) {
	var info collectionInfo
	err := c.request(ctx, http.MethodGet, c.collectionPath(), nil, &info)
	return info, err
}

func (c *Client) ProvenanceMatches(ctx context.Context, sourceID string, gen int64, bootID string) bool {
	info, err := c.collectionInfo(ctx)
	return err == nil && info.Config.Metadata.SourceID == sourceID && info.Config.Metadata.Generation == gen &&
		info.Config.Metadata.BootID == bootID && bootID != "" && strings.EqualFold(info.Config.Params.Vectors.Distance, "Euclid")
}

// RotateBootID changes the independent Qdrant marker before SQLite records
// the same value. A crash between those writes leaves search safely on SQLite.
func (c *Client) RotateBootID(ctx context.Context, sourceID string, gen int64) (string, error) {
	info, err := c.collectionInfo(ctx)
	if err != nil {
		return "", err
	}
	if info.Config.Metadata.SourceID != sourceID || info.Config.Metadata.Generation != gen ||
		!strings.EqualFold(info.Config.Params.Vectors.Distance, "Euclid") {
		return "", errors.New("qdrant collection provenance mismatch")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("generate Qdrant boot marker: %w", err)
	}
	bootID := hex.EncodeToString(nonce[:])
	if err := c.request(ctx, http.MethodPatch, c.collectionPath(), map[string]any{
		"metadata": map[string]any{"boot_id": bootID},
	}, nil); err != nil {
		return "", err
	}
	return bootID, nil
}

func (c *Client) EnsureCollection(ctx context.Context, dimension int, sourceID string, gen int64) error {
	if dimension <= 0 {
		return errors.New("invalid vector dimension")
	}
	if sourceID == "" || gen <= 0 {
		return errors.New("qdrant source identity and generation are required")
	}
	info, err := c.collectionInfo(ctx)
	if err == nil {
		if info.Config.Params.Vectors.Size != dimension || !strings.EqualFold(info.Config.Params.Vectors.Distance, "Euclid") ||
			info.Config.Metadata.SourceID != sourceID || info.Config.Metadata.Generation != gen {
			return errors.New("qdrant collection dimension, metric, or provenance mismatch")
		}
		return nil
	}
	if !strings.Contains(err.Error(), "HTTP 404") {
		return err
	}
	return c.request(ctx, http.MethodPut, c.collectionPath(), map[string]any{
		"vectors":         map[string]any{"size": dimension, "distance": "Euclid", "memory": "cached"},
		"on_disk_payload": true,
		"metadata":        map[string]any{"source_id": sourceID, "generation_id": gen},
	}, nil)
}

func (c *Client) Upsert(ctx context.Context, points []Point) error {
	if len(points) == 0 {
		return nil
	}
	type qpoint struct {
		ID      uint64         `json:"id"`
		Vector  []float32      `json:"vector"`
		Payload map[string]any `json:"payload"`
	}
	items := make([]qpoint, len(points))
	for i, p := range points {
		items[i] = qpoint{ID: p.ID, Vector: p.Vector, Payload: map[string]any{"message_id": p.MessageID, "chunk_index": p.ChunkIndex}}
	}
	return c.request(ctx, http.MethodPut, c.collectionPath()+"/points?wait=true", map[string]any{"points": items}, nil)
}

func (c *Client) Delete(ctx context.Context, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	err := c.request(ctx, http.MethodPost, c.collectionPath()+"/points/delete?wait=true", map[string]any{"points": ids}, nil)
	if err != nil && strings.Contains(err.Error(), "HTTP 404") {
		return nil // a retired generation may never have had a collection
	}
	return err
}

func (c *Client) Search(ctx context.Context, vector []float32, limit int) ([]Match, error) {
	if len(vector) == 0 || limit <= 0 {
		return nil, errors.New("invalid qdrant search request")
	}
	var result struct {
		Points []struct {
			ID      uint64  `json:"id"`
			Score   float64 `json:"score"`
			Payload struct {
				MessageID  int64 `json:"message_id"`
				ChunkIndex int   `json:"chunk_index"`
			} `json:"payload"`
		} `json:"points"`
	}
	err := c.request(ctx, http.MethodPost, c.collectionPath()+"/points/query", map[string]any{
		"query": vector, "limit": limit, "with_payload": []string{"message_id", "chunk_index"},
		"params": map[string]any{"hnsw_ef": 128},
	}, &result)
	if err != nil {
		return nil, err
	}
	matches := make([]Match, len(result.Points))
	for i, p := range result.Points {
		matches[i] = Match{ID: p.ID, MessageID: p.Payload.MessageID, ChunkIndex: p.Payload.ChunkIndex, Score: p.Score}
	}
	return matches, nil
}

func (c *Client) Count(ctx context.Context) (int64, error) {
	var result struct {
		Count int64 `json:"count"`
	}
	err := c.request(ctx, http.MethodPost, c.collectionPath()+"/points/count", map[string]any{"exact": true}, &result)
	return result.Count, err
}

// ScrollIDs returns one ordered page of point identities without vectors or
// payload. A nil next offset marks the end of the collection.
func (c *Client) ScrollIDs(ctx context.Context, offset *uint64, limit int) ([]uint64, *uint64, error) {
	body := map[string]any{"limit": limit, "with_payload": false, "with_vector": false}
	if offset != nil {
		body["offset"] = *offset
	}
	var result struct {
		Points []struct {
			ID uint64 `json:"id"`
		} `json:"points"`
		Next *uint64 `json:"next_page_offset"`
	}
	if err := c.request(ctx, http.MethodPost, c.collectionPath()+"/points/scroll", body, &result); err != nil {
		return nil, nil, err
	}
	ids := make([]uint64, len(result.Points))
	for i, p := range result.Points {
		ids[i] = p.ID
	}
	return ids, result.Next, nil
}

func (c *Client) CollectionName() string { return c.collection }

func CollectionForGeneration(prefix string, gen int64) string {
	return prefix + "_g" + strconv.FormatInt(gen, 10)
}
