package remoteposix

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/google/uuid"
)

// cleanupUploads reclaims committed payloads and expired sessions. Session locks
// coordinate with writers and recovery in every provider sharing this state.
func (d *Driver) cleanupUploads(ctx context.Context) error {
	rows, err := d.s.reader().QueryContext(ctx, "SELECT id, body FROM uploads")
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		var b []byte
		if err = rows.Scan(&id, &b); err != nil {
			rows.Close()
			return err
		}
		var s session
		if err = json.Unmarshal(b, &s); err != nil {
			rows.Close()
			return err
		}
		if s.Result != "" || time.Since(time.Unix(0, s.Created)) > 24*time.Hour {
			ids = append(ids, id)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var result error
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		// A busy session can wait until the next pass; do not stall reconnects
		// behind an upload streaming a large body while holding its lock.
		attempt, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		err := d.cleanupUpload(attempt, id)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			continue
		}
		result = errors.Join(result, err)
	}
	return result
}

func (d *Driver) cleanupUpload(ctx context.Context, id string) error {
	if _, err := uuid.Parse(id); err != nil {
		return err
	}
	unlock, err := d.s.acquire(ctx, []resourceLock{{"session/" + id, true}})
	if err != nil {
		return err
	}
	defer unlock()
	// Never discard the session needed to finish an uncommitted operation,
	// including ambiguous recovery and remote outages. Recovery owns that work.
	ops, err := d.s.pending(ctx)
	if err != nil {
		return err
	}
	for _, op := range ops {
		if op.Session == id {
			return nil
		}
	}
	var b []byte
	err = d.s.reader().QueryRowContext(ctx, "SELECT body FROM uploads WHERE id=?", id).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var s session
	if err = json.Unmarshal(b, &s); err != nil {
		return err
	}
	expired := time.Since(time.Unix(0, s.Created)) > 24*time.Hour
	if s.Result == "" && !expired {
		return nil
	}
	if err = os.Remove(d.stage(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if expired {
		_, err = d.s.db.ExecContext(ctx, "DELETE FROM uploads WHERE id=?", id)
		return err
	}
	return nil
}
