package imap

import (
	"context"
	"fmt"
	"slices"

	imap "github.com/emersion/go-imap/v2"
)

// tryBuildCondstoreMessageList uses a complete saved membership baseline on
// servers with CONDSTORE but without QRESYNC. A changed mailbox still needs a
// UID SEARCH: CHANGEDSINCE reports flag changes, not expunges.
func (c *Client) tryBuildCondstoreMessageList(
	ctx context.Context, mailboxes []string, statuses map[string]FolderState,
) (bool, error) {
	if c.forceFullEnumeration || c.labelsSnapshotFilteredLocked() ||
		!c.conn.Caps().Has(imap.CapCondStore) ||
		!c.qresyncBaselinePresent(mailboxes) ||
		!folderStatusesCoverMailboxes(mailboxes, statuses) {
		return false, nil
	}
	for _, mailbox := range mailboxes {
		prior, current := c.priorFolderStates[mailbox], statuses[mailbox]
		if !c.qresyncEligible(prior, current) || current.NumMessages == nil {
			return false, nil
		}
		// A saved baseline must describe a possible mailbox epoch. A full scan
		// repairs an impossible cursor instead of building deltas from it.
		for _, uid := range prior.KnownUIDs {
			if uid == 0 || uid >= prior.UIDNext {
				return false, nil
			}
		}
	}
	return true, c.buildIncrementalMessageList(ctx, mailboxes, func(mailbox string) (MailboxDelta, error) {
		return c.collectCondstoreMailbox(ctx, mailbox, c.priorFolderStates[mailbox], statuses[mailbox])
	})
}

func (c *Client) collectCondstoreMailbox(
	ctx context.Context, mailbox string, prior, status FolderState,
) (MailboxDelta, error) {
	if err := ctx.Err(); err != nil {
		return MailboxDelta{}, err
	}
	if folderStateUnchanged(prior, status) && statusMessageCount(status, len(prior.KnownUIDs)) {
		status.KnownUIDs = cloneKnownUIDs(prior.KnownUIDs)
		return MailboxDelta{Mailbox: mailbox, State: status, Incremental: true}, nil
	}

	selected, err := c.conn.Select(mailbox, &imap.SelectOptions{CondStore: true}).Wait()
	if err != nil {
		return MailboxDelta{}, fmt.Errorf("CONDSTORE SELECT %q: %w", mailbox, err)
	}
	if selected.UIDValidity != prior.UIDValidity || selected.UIDNext == 0 ||
		uint32(selected.UIDNext) < status.UIDNext ||
		selected.HighestModSeq == 0 || selected.HighestModSeq < status.HighestModSeq {
		return MailboxDelta{}, fmt.Errorf("CONDSTORE SELECT %q changed or regressed cursor", mailbox)
	}
	c.selectedMailbox = mailbox
	c.selectedUIDValidity = selected.UIDValidity
	c.selectedNumMessages = selected.NumMessages

	// SEARCH UID n:* can return the last live UID even when it is below n.
	// Filter the server's answer before comparing it with the full UID set.
	tail, err := c.searchCondstoreUIDs(mailbox, imap.UID(prior.UIDNext), selected.UIDNext)
	if err != nil {
		return MailboxDelta{}, err
	}
	current, err := c.searchCondstoreUIDs(mailbox, 1, selected.UIDNext)
	if err != nil {
		return MailboxDelta{}, err
	}
	if len(current) != int(selected.NumMessages) {
		return MailboxDelta{}, fmt.Errorf("CONDSTORE %q UID SEARCH count differs from SELECT", mailbox)
	}
	added, vanished := diffKnownUIDs(prior.KnownUIDs, current)
	newUIDs := make([]imap.UID, 0, len(added))
	for _, uid := range added {
		if uint32(uid) < prior.UIDNext {
			return MailboxDelta{}, fmt.Errorf("CONDSTORE %q baseline omitted UID %d", mailbox, uid)
		}
		newUIDs = append(newUIDs, uid)
	}
	if !slices.Equal(tail, newUIDs) {
		return MailboxDelta{}, fmt.Errorf("CONDSTORE %q UID searches disagree", mailbox)
	}

	changed := make(map[imap.UID]struct{}, len(added))
	for _, uid := range added {
		changed[uid] = struct{}{}
	}
	if selected.UIDNext > 1 && selected.HighestModSeq > prior.HighestModSeq {
		var requested imap.UIDSet
		requested.AddRange(1, selected.UIDNext-1)
		msgs, fetchErr := c.conn.Fetch(requested, &imap.FetchOptions{
			UID: true, Flags: true, ModSeq: true, ChangedSince: prior.HighestModSeq,
		}).Collect()
		if fetchErr != nil {
			return MailboxDelta{}, fmt.Errorf("CONDSTORE CHANGEDSINCE in %q: %w", mailbox, fetchErr)
		}
		for _, msg := range msgs {
			if msg.UID == 0 || msg.ModSeq <= prior.HighestModSeq ||
				!slices.Contains(current, msg.UID) {
				return MailboxDelta{}, fmt.Errorf("CONDSTORE %q returned inconsistent changed UID", mailbox)
			}
			changed[msg.UID] = struct{}{}
		}
	}
	changedUIDs := make([]imap.UID, 0, len(changed))
	for uid := range changed {
		changedUIDs = append(changedUIDs, uid)
	}
	slices.Sort(changedUIDs)
	known := uidsToUint32(current)
	return MailboxDelta{
		Mailbox: mailbox,
		State: FolderState{
			UIDValidity: selected.UIDValidity, UIDNext: baselineUIDNext(uint32(selected.UIDNext), known),
			HighestModSeq: selected.HighestModSeq, KnownUIDs: known,
		},
		ChangedUIDs: changedUIDs, VanishedUIDs: vanished, Incremental: true,
	}, nil
}

func (c *Client) searchCondstoreUIDs(mailbox string, minUID, selectedUIDNext imap.UID) ([]imap.UID, error) {
	if c.selectedNumMessages == 0 {
		return []imap.UID{}, nil
	}
	data, err := c.conn.UIDSearch(enumerateMailboxSearchCriteria(c.since, c.before, minUID), nil).Wait()
	if err != nil {
		return nil, fmt.Errorf("CONDSTORE UID SEARCH in %q: %w", mailbox, err)
	}
	set, ok := data.All.(imap.UIDSet)
	if !ok {
		return nil, fmt.Errorf("CONDSTORE UID SEARCH in %q returned untyped UID set", mailbox)
	}
	uids, ok := set.Nums()
	if !ok {
		return nil, fmt.Errorf("CONDSTORE UID SEARCH in %q returned dynamic UID set", mailbox)
	}
	uids = slices.DeleteFunc(uids, func(uid imap.UID) bool { return uid < minUID })
	for _, uid := range uids {
		if uid == 0 || uid >= selectedUIDNext {
			return nil, fmt.Errorf("CONDSTORE UID SEARCH in %q returned out-of-range UID", mailbox)
		}
	}
	slices.Sort(uids)
	return uids, nil
}
