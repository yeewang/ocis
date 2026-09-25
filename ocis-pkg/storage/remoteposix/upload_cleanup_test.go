package remoteposix

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCompletedUploadRemovesStageAndAllowsRetry(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	ctx := ownerContext()
	ids, err := d.InitiateUpload(ctx, at(d, "uploaded"), 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	u, err := d.GetUpload(ctx, ids["tus"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err = u.WriteChunk(ctx, 0, strings.NewReader("payload")); err != nil {
		t.Fatal(err)
	}
	if err = u.FinishUpload(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(d.stage(u.(*upload).id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stage remains: %v", err)
	}
	if err = u.FinishUpload(ctx); err != nil {
		t.Fatalf("completion retry: %v", err)
	}
	info, err := u.GetInfo(ctx)
	if err != nil || info.Offset != 7 {
		t.Fatalf("receipt lost: %+v %v", info, err)
	}
	if got := readContent(t, d, at(d, "uploaded")); got != "payload" {
		t.Fatal(got)
	}
}

func TestCleanupUploadStates(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	d.cancel()
	<-d.done
	for _, tc := range []struct {
		name                        string
		completed, expired, pending bool
	}{
		{"active", false, false, false},
		{"completed", true, false, false},
		{"expired", false, true, false},
		{"expired-completed", true, true, false},
		{"pending-recovery", false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids, err := d.InitiateUpload(ownerContext(), at(d, tc.name), 7, nil)
			if err != nil {
				t.Fatal(err)
			}
			u, err := d.GetUpload(ownerContext(), ids["tus"])
			if err != nil {
				t.Fatal(err)
			}
			id := u.(*upload).id
			s, err := d.loadSession(id)
			if err != nil {
				t.Fatal(err)
			}
			if tc.completed {
				s.Result = s.Info.Storage["NodeId"]
			}
			if tc.expired {
				s.Created = time.Now().Add(-25 * time.Hour).UnixNano()
			}
			if err = d.saveSession(s); err != nil {
				t.Fatal(err)
			}
			if tc.pending {
				op := operation{ID: uuid.NewString(), Session: id}
				b, _ := json.Marshal(op)
				if _, err = d.s.db.Exec("INSERT INTO operations VALUES (?,?)", op.ID, b); err != nil {
					t.Fatal(err)
				}
			}
			if err = d.cleanupUploads(context.Background()); err != nil {
				t.Fatal(err)
			}
			_, err = os.Stat(d.stage(id))
			wantStage := tc.pending || (!tc.completed && !tc.expired)
			if (err == nil) != wantStage {
				t.Fatalf("stage expected=%v err=%v", wantStage, err)
			}
			var count int
			if err = d.s.reader().QueryRow("SELECT count(*) FROM uploads WHERE id=?", id).Scan(&count); err != nil {
				t.Fatal(err)
			}
			wantRecord := !tc.expired || tc.pending
			if (count == 1) != wantRecord {
				t.Fatalf("record expected=%v count=%d", wantRecord, count)
			}
		})
	}
}

func TestCleanupWaitsForSessionLock(t *testing.T) {
	d := testDriver(t, t.TempDir(), t.TempDir())
	d.cancel()
	<-d.done
	ids, err := d.InitiateUpload(ownerContext(), at(d, "active"), 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	u, err := d.GetUpload(ownerContext(), ids["tus"])
	if err != nil {
		t.Fatal(err)
	}
	id := u.(*upload).id
	s, _ := d.loadSession(id)
	s.Created = time.Now().Add(-25 * time.Hour).UnixNano()
	if err = d.saveSession(s); err != nil {
		t.Fatal(err)
	}
	unlock, err := d.beginSession(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = d.cleanupUploads(ctx)
	unlock()
	if err == nil {
		t.Fatal("cleanup bypassed active upload lock")
	}
	if _, err = os.Stat(d.stage(id)); err != nil {
		t.Fatal(err)
	}
	if err = d.cleanupUploads(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStartupCleansPreviouslyCompletedStaging(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	d := testDriver(t, root, state)
	d.cancel()
	<-d.done
	uploadFile(t, d, "committed", "payload")
	var id string
	if err := d.s.reader().QueryRow("SELECT id FROM uploads").Scan(&id); err != nil {
		t.Fatal(err)
	}
	// Model a crash after commit but before unlinking the local copy.
	if err := os.WriteFile(d.stage(id), []byte("payload"), 0600); err != nil {
		t.Fatal(err)
	}
	other := testDriver(t, root, state)
	eventually(t, func() error {
		_, err := os.Stat(other.stage(id))
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return errors.New("completed staging still present")
	})
	if got := readContent(t, other, at(other, "committed")); got != "payload" {
		t.Fatal(got)
	}
	if s, err := other.loadSession(id); err != nil || s.Result == "" {
		t.Fatalf("receipt lost: %v", err)
	}
}
