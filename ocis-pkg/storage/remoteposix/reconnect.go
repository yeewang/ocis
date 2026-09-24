package remoteposix

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"
)

var errRemoteOffline = errors.New("remote filesystem unavailable; automatic reconnect pending")
var errMountBusy = errors.New("remote filesystem has active operations")

// This cross-process gate precedes every session, operation and resource lock.
// Never unlink it. Nonblocking acquisition also avoids nested-admission deadlocks.
func mountGate(state string, write bool) (func(), error) {
	f, err := os.OpenFile(filepath.Join(state, "mount.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	ok, err := tryFileLock(f, write)
	if err != nil || !ok {
		_ = f.Close()
		if err == nil {
			err = errMountBusy
		}
		return nil, err
	}
	return func() { _ = f.Close() }, nil
}

// admit protects shutdown, without taking the mount gate (also used by reopen).
func (s *store) admit() (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("remoteposix: store closed")
	}
	s.active.Add(1)
	return s.active.Done, nil
}

func (s *store) enter() (func(), error) {
	leave, err := s.admit()
	if err != nil {
		return nil, err
	}
	gate, err := mountGate(s.state, false)
	if err != nil {
		leave()
		return nil, fmt.Errorf("%w: %v", errRemoteOffline, err)
	}
	// The process-local lock makes the handle lifetime visible to Go's race
	// detector as well. An exclusive holder takes the OS gate first, so it
	// cannot queue on rootMu while nested shared admissions are active.
	s.rootMu.RLock()
	release := func() { s.rootMu.RUnlock(); gate(); leave() }
	var generation int64
	err = s.reader().QueryRow("SELECT value FROM settings WHERE key='mount_generation'").Scan(&generation)
	if err == nil && generation != s.generation {
		err = errors.New("mount generation changed")
	}
	if err == nil {
		err = s.healthy()
	}
	if err != nil {
		s.offline.Store(true)
		release()
		return nil, fmt.Errorf("%w: %v", errRemoteOffline, err)
	}
	return release, nil
}

func (s *store) validateRoot(r *os.Root) error {
	f, err := r.Open(path.Join(control, "identity"))
	if err != nil {
		return err
	}
	identity, err := io.ReadAll(io.LimitReader(f, 128))
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if string(identity) != s.spaceID {
		return errors.New("remote mount identity mismatch")
	}
	for _, name := range []string{"tmp", "trash", "versions"} {
		fi, err := r.Lstat(path.Join(control, name))
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			return fmt.Errorf("remote control path %s is not a directory", name)
		}
	}
	return nil
}

// Reopen only after ALL processes have released their old-generation leases.
// A stalled syscall keeps its lease; reconnect must never steal its locks.
func (s *store) reopen(ctx context.Context) error {
	leave, err := s.admit()
	if err != nil {
		return err
	}
	defer leave()
	gate, err := mountGate(s.state, true)
	if err != nil {
		return err
	}
	defer gate()
	s.rootMu.Lock()
	defer s.rootMu.Unlock()
	if err = ctx.Err(); err != nil {
		return err
	}
	candidate, err := os.OpenRoot(s.rootPath)
	if err != nil {
		return err
	}
	adopted := false
	defer func() {
		if !adopted {
			_ = candidate.Close()
		}
	}()
	if err = s.validateRoot(candidate); err != nil {
		return err
	}
	// Check the pathname again before adoption in case the mount changed while
	// its identity was being read. No missing control directories are recreated.
	fresh, err := os.OpenRoot(s.rootPath)
	if err != nil {
		return err
	}
	a, errA := candidate.Stat(".")
	b, errB := fresh.Stat(".")
	_ = fresh.Close()
	if errA != nil {
		return errA
	}
	if errB != nil {
		return errB
	}
	if !os.SameFile(a, b) {
		return errors.New("remote root changed during reconnect")
	}
	var generation int64
	if err = s.reader().QueryRowContext(ctx, "SELECT value FROM settings WHERE key='mount_generation'").Scan(&generation); err != nil {
		return err
	}
	if generation == s.generation {
		generation++
		if _, err = s.db.ExecContext(ctx, "UPDATE settings SET value=? WHERE key='mount_generation'", generation); err != nil {
			return err
		}
	}
	old := s.root
	s.root = candidate
	s.generation = generation
	adopted = true
	_ = old.Close()
	s.offline.Store(false)
	return nil
}

func (d *Driver) maintain(ctx context.Context) {
	defer close(d.done)
	probe := time.NewTimer(time.Second)
	defer probe.Stop()
	scan := time.NewTicker(d.c.ScanInterval)
	defer scan.Stop()
	delay := time.Second
	recovering := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-probe.C:
			wasOffline := d.s.offline.Load()
			leave, err := d.s.enter()
			if err == nil {
				leave()
			} else {
				err = d.s.reopen(ctx)
				wasOffline = true
			}
			if err != nil {
				// Busy means existing operations still own the old handle, not permission
				// to release their locks. Retry without spawning additional probe workers.
				d.s.offline.Store(true)
				if !errors.Is(err, errMountBusy) {
					d.log.Warn().Err(err).Msg("remote filesystem reconnect deferred")
				}
				delay = min(delay*2, 30*time.Second)
			} else {
				d.s.offline.Store(false)
				recovering = recovering || wasOffline
				if recovering {
					if err = d.s.recover(ctx); err != nil {
						d.log.Warn().Err(err).Msg("remote filesystem reconnected with pending recovery")
						delay = min(delay*2, 30*time.Second)
					} else {
						recovering = false
						delay = time.Second
						if d.c.Watch {
							if err = d.s.scan(ctx, d.c.MissingGrace); err != nil {
								d.log.Warn().Err(err).Msg("remote filesystem rescan deferred")
							}
						}
						d.log.Info().Msg("remote filesystem reconnected")
					}
				} else {
					delay = time.Second
				}
			}
			probe.Reset(delay)
		case <-scan.C:
			if d.c.Watch {
				if err := errors.Join(d.s.recover(ctx), d.s.scan(ctx, d.c.MissingGrace), d.publish(ctx)); err != nil {
					d.log.Error().Err(err).Msg("remote filesystem reconciliation suspended")
				}
			}
		}
	}
}
