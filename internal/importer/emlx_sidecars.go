package importer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"go.kenn.io/msgvault/internal/mime"
	"go.kenn.io/msgvault/internal/store"
)

// Apple Mail stores cached attachment bytes beside Messages/, under
// Attachments/<emlx number>/<part number>/<original filename>. The part
// number is not a stable flat MIME index, so an exact, unique filename match
// to an empty MIME attachment is required before the sidecar is accepted.
func ingestEmlxSidecars(
	ctx context.Context, st *store.Store, messageID int64,
	emlxPaths []string, raw []byte, attachmentsDir string, maxBytes int64,
) (stored, unmatched int64, err error) {
	if attachmentsDir == "" {
		return 0, 0, nil
	}
	var parsed *mime.Message
	type sidecarResolution struct {
		attachment   mime.Attachment
		path         string
		existingHash string
		copies       int
		conflict     bool
	}
	resolved := make(map[string]*sidecarResolution)
	var order []string
	for _, emlxPath := range emlxPaths {
		sidecarDir := emlxSidecarDir(emlxPath)
		if sidecarDir == "" {
			continue
		}
		partDirs, readErr := os.ReadDir(sidecarDir)
		if os.IsNotExist(readErr) {
			continue
		}
		if readErr != nil {
			return stored, unmatched, sidecarIOError("read Apple Mail sidecar directory", readErr)
		}
		if parsed == nil {
			parsed, err = mime.Parse(raw)
			if err != nil {
				return stored, unmatched, errors.New("parse MIME for Apple Mail sidecars")
			}
		}

		type candidate struct{ path, name string }
		var candidates []candidate
		nameCounts := make(map[string]int)
		for _, part := range partDirs {
			if !part.IsDir() {
				unmatched++
				continue
			}
			if _, parseErr := strconv.ParseUint(part.Name(), 10, 64); parseErr != nil {
				unmatched++
				continue
			}
			files, readErr := os.ReadDir(filepath.Join(sidecarDir, part.Name()))
			if readErr != nil {
				return stored, unmatched, sidecarIOError("read Apple Mail sidecar part", readErr)
			}
			for _, file := range files {
				if !file.Type().IsRegular() {
					unmatched++
					continue
				}
				candidates = append(candidates, candidate{
					path: filepath.Join(sidecarDir, part.Name(), file.Name()),
					name: file.Name(),
				})
				nameCounts[file.Name()]++
			}
		}

		for _, candidate := range candidates {
			if ctx.Err() != nil {
				return stored, unmatched, ctx.Err()
			}
			if nameCounts[candidate.name] != 1 {
				unmatched++
				continue
			}
			var match *mime.Attachment
			matches := 0
			for i := range parsed.Attachments {
				att := &parsed.Attachments[i]
				if att.Filename == candidate.name {
					matches++
					match = att
				}
			}
			if matches != 1 || len(match.Content) != 0 {
				unmatched++
				continue
			}
			occurrence := match.PartKey
			if occurrence == "" {
				occurrence = "filename:" + candidate.name
			}
			info, statErr := os.Stat(candidate.path)
			if statErr != nil {
				return stored, unmatched, sidecarIOError("stat Apple Mail sidecar", statErr)
			}
			if info.Size() == 0 || info.Size() > maxBytes {
				unmatched++
				continue
			}
			file, openErr := os.Open(candidate.path)
			if openErr != nil {
				return stored, unmatched, sidecarIOError("open Apple Mail sidecar", openErr)
			}
			hasher := sha256.New()
			size, readErr := io.Copy(hasher, io.LimitReader(file, sidecarReadLimit(maxBytes)))
			closeErr := file.Close()
			if readErr != nil {
				return stored, unmatched, sidecarIOError("read Apple Mail sidecar", readErr)
			}
			if closeErr != nil {
				return stored, unmatched, sidecarIOError("close Apple Mail sidecar", closeErr)
			}
			if size > maxBytes {
				unmatched++
				continue
			}
			att := *match
			att.Content = nil
			att.ContentHash = hex.EncodeToString(hasher.Sum(nil))
			if prior, ok := resolved[occurrence]; ok {
				prior.copies++
				if prior.attachment.ContentHash != att.ContentHash {
					prior.conflict = true
				}
				continue
			}
			resolved[occurrence] = &sidecarResolution{attachment: att, path: candidate.path, copies: 1}
			order = append(order, occurrence)
		}
	}
	for _, key := range order {
		if resolved[key].conflict {
			unmatched += int64(resolved[key].copies)
			return 0, unmatched, errors.New("conflicting Apple Mail sidecars for one MIME attachment")
		}
	}
	for _, key := range order {
		selection := resolved[key]
		previousHash, lookupErr := existingSidecarHash(ctx, st, messageID, &selection.attachment)
		if lookupErr != nil {
			return stored, unmatched, fmt.Errorf("check Apple Mail sidecar occurrence: %w", lookupErr)
		}
		if previousHash != "" && previousHash != selection.attachment.ContentHash {
			unmatched += int64(selection.copies)
			return stored, unmatched, errors.New("Apple Mail sidecar differs from stored MIME attachment")
		}
		selection.existingHash = previousHash
	}
	for _, key := range order {
		selection := resolved[key]
		file, openErr := os.Open(selection.path)
		if openErr != nil {
			return stored, unmatched, sidecarIOError("open selected Apple Mail sidecar", openErr)
		}
		content, readErr := io.ReadAll(io.LimitReader(file, sidecarReadLimit(maxBytes)))
		closeErr := file.Close()
		if readErr != nil {
			return stored, unmatched, sidecarIOError("read selected Apple Mail sidecar", readErr)
		}
		if closeErr != nil {
			return stored, unmatched, sidecarIOError("close selected Apple Mail sidecar", closeErr)
		}
		if int64(len(content)) > maxBytes {
			return stored, unmatched, errors.New("selected Apple Mail sidecar exceeds size limit")
		}
		hash := sha256.Sum256(content)
		if hex.EncodeToString(hash[:]) != selection.attachment.ContentHash {
			return stored, unmatched, errors.New("selected Apple Mail sidecar changed during import")
		}
		att := selection.attachment
		att.Content = content
		if storeErr := storeAttachment(st, attachmentsDir, messageID, &att); storeErr != nil {
			return stored, unmatched, errors.New("store Apple Mail sidecar")
		}
		if selection.existingHash == "" {
			stored++
		}
	}
	if stored > 0 {
		_, updateErr := st.DB().ExecContext(ctx, st.Rebind(`
			UPDATE messages SET
				has_attachments = (SELECT COUNT(*) > 0 FROM attachments WHERE message_id = ?),
				attachment_count = (SELECT COUNT(*) FROM attachments WHERE message_id = ?)
			WHERE id = ?`), messageID, messageID, messageID)
		if updateErr != nil {
			return stored, unmatched, fmt.Errorf("update Apple Mail attachment count: %w", updateErr)
		}
	}
	return stored, unmatched, nil
}

func sidecarReadLimit(maxBytes int64) int64 {
	if maxBytes == math.MaxInt64 {
		return maxBytes
	}
	return maxBytes + 1
}

func existingSidecarHash(ctx context.Context, st *store.Store, messageID int64, att *mime.Attachment) (string, error) {
	var hash string
	var row *sql.Row
	if att.PartKey != "" {
		row = st.DB().QueryRowContext(ctx, st.Rebind(`SELECT content_hash FROM attachments WHERE message_id = ? AND source_part_key = ? AND storage_path <> '' AND content_hash IS NOT NULL LIMIT 1`), messageID, att.PartKey)
	} else {
		row = st.DB().QueryRowContext(ctx, st.Rebind(`SELECT content_hash FROM attachments WHERE message_id = ? AND filename = ? AND mime_type = ? AND storage_path <> '' AND content_hash IS NOT NULL LIMIT 1`), messageID, att.Filename, att.ContentType)
	}
	err := row.Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return hash, err
}

func emlxSidecarDir(emlxPath string) string {
	if filepath.Base(filepath.Dir(emlxPath)) != "Messages" {
		return ""
	}
	number, _, ok := strings.Cut(filepath.Base(emlxPath), ".")
	if !ok {
		return ""
	}
	if _, err := strconv.ParseUint(number, 10, 64); err != nil {
		return ""
	}
	return filepath.Join(filepath.Dir(filepath.Dir(emlxPath)), "Attachments", number)
}

func sidecarIOError(operation string, err error) error {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return fmt.Errorf("%s: %w", operation, pathErr.Err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
