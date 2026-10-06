/*
   Copyright The nydus Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package manager

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/containerd/nydus-snapshotter/config"
	"github.com/containerd/nydus-snapshotter/pkg/daemon"
	"github.com/containerd/nydus-snapshotter/pkg/daemon/types"
	"github.com/containerd/nydus-snapshotter/pkg/rafs"
	"github.com/containerd/nydus-snapshotter/pkg/store"
)

func newRecoverTestManager(t *testing.T) *Manager {
	db, err := store.NewDatabase(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	s, err := store.NewDaemonRafsStore(db)
	require.NoError(t, err)
	return &Manager{store: s, daemonCache: newDaemonCache(), FsDriver: config.FsDriverFusedev}
}

// addRecoverTestDaemon records a dedicated fusedev daemon whose config.json
// holds cfg, and a RAFS instance with snapshot ID "<id>-snapshot" served by it.
// The API socket's directory is created; the socket itself is not.
func addRecoverTestDaemon(t *testing.T, m *Manager, id, cfg string) *daemon.Daemon {
	// Not t.TempDir: its path can exceed the unix socket path limit on macOS.
	root, err := os.MkdirTemp("", "nydus-recover")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	d := &daemon.Daemon{States: daemon.ConfigState{
		ID:         id,
		APISocket:  filepath.Join(root, "socket", "api.sock"),
		DaemonMode: config.DaemonModeDedicated,
		FsDriver:   config.FsDriverFusedev,
		ConfigDir:  filepath.Join(root, "config"),
		LogDir:     filepath.Join(root, "logs"),
	}}
	for _, dir := range []string{filepath.Dir(d.GetAPISock()), d.States.ConfigDir, d.States.LogDir} {
		require.NoError(t, os.MkdirAll(dir, 0755))
	}
	require.NoError(t, os.WriteFile(d.ConfigFile(""), []byte(cfg), 0600))
	require.NoError(t, m.store.AddDaemon(d))

	snapshotID := id + "-snapshot"
	require.NoError(t, m.store.AddRafsInstance(&rafs.Rafs{
		DaemonID:   id,
		FsDriver:   config.FsDriverFusedev,
		SnapshotID: snapshotID,
	}))
	t.Cleanup(func() { rafs.RafsGlobalCache.Remove(snapshotID) })

	return d
}

// leaveStaleSocket creates a socket file at path with nothing listening on it,
// as a nydusd that died in an unclean reboot leaves behind.
func leaveStaleSocket(t *testing.T, path string) {
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	require.NoError(t, err)
	l.SetUnlinkOnClose(false)
	require.NoError(t, l.Close())
}

func storedDaemonIDs(t *testing.T, m *Manager) []string {
	var ids []string
	require.NoError(t, m.store.WalkDaemons(context.Background(), func(s *daemon.ConfigState) error {
		ids = append(ids, s.ID)
		return nil
	}))
	return ids
}

func storedSnapshotIDs(t *testing.T, m *Manager) []string {
	var ids []string
	require.NoError(t, m.store.WalkRafsInstances(context.Background(), func(r *rafs.Rafs) error {
		ids = append(ids, r.SnapshotID)
		return nil
	}))
	return ids
}

func TestRecoverDropsStoppedDaemonWithUnreadableConfig(t *testing.T) {
	m := newRecoverTestManager(t)
	torn := addRecoverTestDaemon(t, m, "recover-torn", "")
	intact := addRecoverTestDaemon(t, m, "recover-intact", `{"device":{}}`)
	leaveStaleSocket(t, torn.GetAPISock())
	leaveStaleSocket(t, intact.GetAPISock())

	recovering := map[string]*daemon.Daemon{}
	live := map[string]*daemon.Daemon{}
	require.NoError(t, m.Recover(context.Background(), &recovering, &live))

	require.Contains(t, recovering, intact.ID())
	require.NotNil(t, m.GetByDaemonID(intact.ID()))
	require.NotNil(t, rafs.RafsGlobalCache.Get(intact.ID()+"-snapshot"))
	require.DirExists(t, intact.States.ConfigDir)

	require.NotContains(t, recovering, torn.ID())
	require.Nil(t, m.GetByDaemonID(torn.ID()))
	require.Nil(t, rafs.RafsGlobalCache.Get(torn.ID()+"-snapshot"))
	require.NoDirExists(t, torn.States.ConfigDir)
	require.NoDirExists(t, torn.States.LogDir)
	require.NoDirExists(t, filepath.Dir(torn.GetAPISock()))

	require.Equal(t, []string{intact.ID()}, storedDaemonIDs(t, m))
	require.Equal(t, []string{intact.ID() + "-snapshot"}, storedSnapshotIDs(t, m))
}

func TestRecoverFailsOnRunningDaemonWithUnreadableConfig(t *testing.T) {
	m := newRecoverTestManager(t)
	d := addRecoverTestDaemon(t, m, "recover-running", "")

	l, err := net.Listen("unix", d.GetAPISock())
	require.NoError(t, err)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(types.DaemonInfo{ID: d.ID(), State: types.DaemonStateRunning})
	})}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })

	recovering := map[string]*daemon.Daemon{}
	live := map[string]*daemon.Daemon{}
	require.Error(t, m.Recover(context.Background(), &recovering, &live))

	require.Equal(t, []string{d.ID()}, storedDaemonIDs(t, m))
	require.Equal(t, []string{d.ID() + "-snapshot"}, storedSnapshotIDs(t, m))
	require.DirExists(t, d.States.ConfigDir)
}
