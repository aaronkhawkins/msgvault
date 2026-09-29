package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
	emlxPath string, raw []byte, attachmentsDir string, maxBytes int64,
) (stored, unmatched int64, err error) {
	if attachmentsDir == "" {
		return 0, 0, nil
	}
	sidecarDir := emlxSidecarDir(emlxPath)
	if sidecarDir == "" {
		return 0, 0, nil
	}
	partDirs, readErr := os.ReadDir(sidecarDir)
	if os.IsNotExist(readErr) {
		return 0, 0, nil
	}
	if readErr != nil {
		return 0, 0, sidecarIOError("read Apple Mail sidecar directory", readErr)
	}
	parsed, parseErr := mime.Parse(raw)
	if parseErr != nil {
		return 0, 0, errors.New("parse MIME for Apple Mail sidecars")
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
		info, statErr := os.Stat(candidate.path)
		if statErr != nil {
			return stored, unmatched, sidecarIOError("stat Apple Mail sidecar", statErr)
		}
		if info.Size() > maxBytes {
			unmatched++
			continue
		}
		if info.Size() == 0 {
			unmatched++
			continue
		}
		content, readErr := os.ReadFile(candidate.path)
		if readErr != nil {
			return stored, unmatched, sidecarIOError("read Apple Mail sidecar", readErr)
		}
		att := *match
		att.Content = content
		hash := sha256.Sum256(content)
		att.ContentHash = hex.EncodeToString(hash[:])
		if storeErr := storeAttachment(st, attachmentsDir, messageID, &att); storeErr != nil {
			return stored, unmatched, errors.New("store Apple Mail sidecar")
		}
		stored++
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

func hasEmlxSidecars(emlxPath string) bool {
	dir := emlxSidecarDir(emlxPath)
	if dir == "" {
		return false
	}
	info, err := os.Stat(dir)
	return err == nil && info.IsDir()
}

func sidecarIOError(operation string, err error) error {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return fmt.Errorf("%s: %w", operation, pathErr.Err)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
