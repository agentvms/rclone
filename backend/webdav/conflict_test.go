package webdav_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/webdav"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDAV is a tiny Nextcloud-like server: files with ETags, conditional PUT,
// chunked upload (MKCOL, PUT chunks, MOVE .file) and a log of requests.
type fakeDAV struct {
	mu     sync.Mutex
	files  map[string]string // path under /remote.php/dav/files/alice/ -> content
	etags  map[string]string
	chunks map[string]string // upload dir -> content so far
	seq    int
	log    []string
	// rejectPut, when not 0, makes every PUT of a file fail with this status without touching the file
	rejectPut int
}

const davRoot = "/remote.php/dav/files/alice/"

func (d *fakeDAV) set(name, content string) {
	d.seq++
	d.files[name] = content
	d.etags[name] = fmt.Sprintf("\"e%d\"", d.seq)
}

func (d *fakeDAV) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.log = append(d.log, r.Method+" "+r.URL.Path)
	name := strings.TrimPrefix(r.URL.Path, davRoot)
	switch {
	case r.Method == "PUT" && strings.Contains(r.URL.Path, "/dav/uploads/alice/"):
		dir := strings.TrimPrefix(r.URL.Path, "/remote.php/dav/uploads/alice/")
		dir = dir[:strings.Index(dir, "/")]
		b, _ := io.ReadAll(r.Body)
		d.chunks[dir] += string(b)
		w.WriteHeader(http.StatusCreated)
	case r.Method == "PUT" && d.rejectPut != 0:
		w.WriteHeader(d.rejectPut)
	case r.Method == "PUT":
		cur, exists := d.files[name]
		_ = cur
		if m := r.Header.Get("If-Match"); m != "" && (!exists || d.etags[name] != m) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		if r.Header.Get("If-None-Match") == "*" && exists {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		b, _ := io.ReadAll(r.Body)
		d.set(name, string(b))
		w.WriteHeader(http.StatusCreated)
	case r.Method == "MKCOL":
		w.WriteHeader(http.StatusCreated)
	case r.Method == "MOVE" && strings.HasPrefix(r.URL.Path, davRoot):
		// a plain file MOVE (rename); Overwrite: T replaces the destination
		dest, _ := url.Parse(r.Header.Get("Destination"))
		d.set(strings.TrimPrefix(dest.Path, davRoot), d.files[name])
		delete(d.files, name)
		w.WriteHeader(http.StatusCreated)
	case r.Method == "MOVE":
		dir := strings.TrimPrefix(r.URL.Path, "/remote.php/dav/uploads/alice/")
		dir = dir[:strings.Index(dir, "/")]
		dest := strings.TrimPrefix(r.Header.Get("Destination"), "http://"+r.Host+davRoot)
		dest = strings.ReplaceAll(dest, "%20", " ")
		dest = strings.ReplaceAll(dest, "%28", "(")
		dest = strings.ReplaceAll(dest, "%29", ")")
		if _, exists := d.files[dest]; exists && r.Header.Get("Overwrite") == "F" {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		d.set(dest, d.chunks[dir])
		w.WriteHeader(http.StatusCreated)
	case r.Method == "DELETE":
		if strings.Contains(r.URL.Path, "/dav/uploads/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		delete(d.files, name)
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "PROPFIND":
		content, ok := d.files[name]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusMultiStatus)
		fmt.Fprintf(w, `<d:multistatus xmlns:d="DAV:"><d:response><d:href>%s</d:href><d:propstat><d:prop><d:getcontentlength>%d</d:getcontentlength><d:getetag>&quot;%s&quot;</d:getetag><d:resourcetype/></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response></d:multistatus>`,
			r.URL.Path, len(content), strings.Trim(d.etags[name], "\""))
	case r.Method == "PATCH":
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func (d *fakeDAV) deletes() (n int) {
	for _, l := range d.log {
		if strings.HasPrefix(l, "DELETE "+davRoot) {
			n++
		}
	}
	return
}

func newConflictFs(t *testing.T, conflict bool, chunk string) (*fakeDAV, fs.Fs, context.Context) {
	d := &fakeDAV{files: map[string]string{}, etags: map[string]string{}, chunks: map[string]string{}}
	ts := httptest.NewServer(d)
	t.Cleanup(ts.Close)
	configfile.Install()
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 1
	m := configmap.Simple{
		"type":                 "webdav",
		"url":                  ts.URL + "/remote.php/dav/files/alice",
		"vendor":               "nextcloud",
		"nextcloud_chunk_size": chunk,
	}
	if conflict {
		m["conflict_copies"] = "true"
	}
	f, err := webdav.NewFs(ctx, "TestConflict", "", m)
	require.NoError(t, err)
	return d, f, ctx
}

func upload(ctx context.Context, f fs.Fs, name, content string) error {
	src := &reopenable{object.NewStaticObjectInfo(name, time.Now(), int64(len(content)), true, nil, f), content}
	_, err := f.Put(ctx, bytes.NewReader([]byte(content)), src)
	return err
}

// update replaces an object that rclone listed earlier, like the VFS does
func update(ctx context.Context, o fs.Object, content string) error {
	src := &reopenable{object.NewStaticObjectInfo(o.Remote(), time.Now(), int64(len(content)), true, nil, o.Fs()), content}
	return o.Update(ctx, bytes.NewReader([]byte(content)), src)
}

// reopenable is a local source the backend can open a second time
type reopenable struct {
	fs.ObjectInfo
	content string
}

func (r *reopenable) Open(ctx context.Context, _ ...fs.OpenOption) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(r.content)), nil
}
func (r *reopenable) Update(context.Context, io.Reader, fs.ObjectInfo, ...fs.OpenOption) error {
	return fmt.Errorf("unused")
}
func (r *reopenable) Remove(context.Context) error { return fmt.Errorf("unused") }
func (r *reopenable) SetModTime(context.Context, time.Time) error {
	return fmt.Errorf("unused")
}
func (r *reopenable) Storable() bool                                  { return true }
func (r *reopenable) Fs() fs.Info                                     { return r.ObjectInfo.Fs() }
func (r *reopenable) String() string                                  { return r.Remote() }
func (r *reopenable) Hash(context.Context, hash.Type) (string, error) { return "", nil }

func conflictNames(d *fakeDAV, base string) (names []string) {
	for n := range d.files {
		if strings.HasPrefix(n, base+" (conflicted copy ") {
			names = append(names, n)
		}
	}
	return
}

func TestConflictNewFileUsesIfNoneMatch(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "10Mi")
	d.set("a.txt", "agent")
	require.NoError(t, upload(ctx, f, "a.txt", "computer"))
	assert.Equal(t, "agent", d.files["a.txt"], "the server's file keeps its name and content")
	names := conflictNames(d, "a")
	require.Len(t, names, 1)
	assert.True(t, strings.HasSuffix(names[0], ").txt"), names[0])
	assert.Equal(t, "computer", d.files[names[0]])
	assert.Zero(t, d.deletes(), "no DELETE of the server file")
}

func TestConflictStaleETagSavesCopyAndKeepsServerFile(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "10Mi")
	d.set("doc.txt", "v1")
	o, err := f.NewObject(ctx, "doc.txt")
	require.NoError(t, err)
	d.mu.Lock()
	d.set("doc.txt", "agent edit") // the agent changes it before the computer uploads
	d.mu.Unlock()
	require.NoError(t, update(ctx, o, "computer edit"))
	assert.Equal(t, "agent edit", d.files["doc.txt"])
	names := conflictNames(d, "doc")
	require.Len(t, names, 1)
	assert.Equal(t, "computer edit", d.files[names[0]])
	assert.Zero(t, d.deletes(), "a failed conditional upload must not delete the server file")
}

func TestConflictNormalEditsMakeNoCopy(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "10Mi")
	d.set("doc.txt", "v1")
	o, err := f.NewObject(ctx, "doc.txt")
	require.NoError(t, err)
	require.NoError(t, update(ctx, o, "v2"))
	require.NoError(t, update(ctx, o, "v3"))
	assert.Equal(t, "v3", d.files["doc.txt"])
	assert.Empty(t, conflictNames(d, "doc"))
}

func TestConflictNewFileIsPlainUpload(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "10Mi")
	require.NoError(t, upload(ctx, f, "new.txt", "hello"))
	assert.Equal(t, "hello", d.files["new.txt"])
	assert.Empty(t, conflictNames(d, "new"))
}

func TestConflictChunkedChecksDestinationETag(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "4B")
	d.set("big.bin", "v1v1v1v1v1")
	o, err := f.NewObject(ctx, "big.bin")
	require.NoError(t, err)

	// unchanged destination: the merge replaces it
	require.NoError(t, update(ctx, o, "second-ver"))
	assert.Equal(t, "second-ver", d.files["big.bin"])
	assert.Empty(t, conflictNames(d, "big"))

	// the agent changes it first: the merge becomes a conflicted copy
	d.mu.Lock()
	d.set("big.bin", "agent-made")
	d.mu.Unlock()
	o2, err := f.NewObject(ctx, "big.bin")
	require.NoError(t, err)
	d.mu.Lock()
	d.set("big.bin", "agent-again")
	d.mu.Unlock()
	require.NoError(t, update(ctx, o2, "computer-12"))
	assert.Equal(t, "agent-again", d.files["big.bin"])
	names := conflictNames(d, "big")
	require.Len(t, names, 1)
	assert.Equal(t, "computer-12", d.files[names[0]])
	assert.Zero(t, d.deletes())
}

func TestConflictChunkedNewFileHasNoFalseConflict(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "4B")
	require.NoError(t, upload(ctx, f, "fresh.bin", "0123456789"))
	assert.Equal(t, "0123456789", d.files["fresh.bin"])
	assert.Empty(t, conflictNames(d, "fresh"))
}

func TestConflictChunkedNewFileAppearedOnServer(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "4B")
	d.set("same.bin", "agent-file!")
	require.NoError(t, upload(ctx, f, "same.bin", "0123456789"))
	assert.Equal(t, "agent-file!", d.files["same.bin"])
	assert.Len(t, conflictNames(d, "same"), 1)
}

func TestConflictOffByDefault(t *testing.T) {
	d, f, ctx := newConflictFs(t, false, "10Mi")
	d.set("a.txt", "agent")
	o, err := f.NewObject(ctx, "a.txt")
	require.NoError(t, err)
	require.NoError(t, update(ctx, o, "computer"))
	assert.Equal(t, "computer", d.files["a.txt"], "stock behaviour: last writer wins")
	for _, l := range d.log {
		assert.NotContains(t, l, "conflicted")
	}
}

// A failed upload for any reason other than a 412 (507, a 5xx) must not delete
// the file that is on the server: the server did not change it.
func TestConflictFailedUploadKeepsServerFile(t *testing.T) {
	for _, status := range []int{http.StatusInsufficientStorage, http.StatusInternalServerError, http.StatusBadGateway} {
		d, f, ctx := newConflictFs(t, true, "10Mi")
		d.set("doc.txt", "server version")
		o, err := f.NewObject(ctx, "doc.txt")
		require.NoError(t, err)
		d.rejectPut = status
		require.Error(t, update(ctx, o, "computer edit"), "status %d", status)
		assert.Equal(t, "server version", d.files["doc.txt"], "status %d", status)
		assert.Zero(t, d.deletes(), "status %d", status)
	}
}

// With conflict copies off the base behaviour stays: a failed upload is cleaned up.
func TestConflictOffFailedUploadStillRemoves(t *testing.T) {
	d, f, ctx := newConflictFs(t, false, "10Mi")
	d.set("doc.txt", "server version")
	o, err := f.NewObject(ctx, "doc.txt")
	require.NoError(t, err)
	d.rejectPut = http.StatusInsufficientStorage
	require.Error(t, update(ctx, o, "computer edit"))
	assert.Equal(t, 1, d.deletes())
}

// moveOver does what vfs/file.go does for a rename over an existing file:
// look the destination up, then operations.Move.
func moveOver(ctx context.Context, f fs.Fs, from, to string) error {
	src, err := f.NewObject(ctx, from)
	if err != nil {
		return err
	}
	dst, err := f.NewObject(ctx, to)
	if err != nil && !errors.Is(err, fs.ErrorObjectNotFound) {
		return err
	}
	_, err = operations.Move(ctx, f, dst, to, src)
	return err
}

// A temporary file that was already uploaded, renamed over a file an agent changed
// since the computer saw it: the agent's version stays, the computer's becomes a copy.
func TestConflictUploadedTempRenameKeepsAgentVersion(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "10Mi")
	d.set("doc.txt", "old version")
	_, err := f.NewObject(ctx, "doc.txt")
	require.NoError(t, err)
	require.NoError(t, upload(ctx, f, "temp.txt", "computer edit"))
	d.set("doc.txt", "agent edit")
	require.NoError(t, moveOver(ctx, f, "temp.txt", "doc.txt"))
	assert.Equal(t, "agent edit", d.files["doc.txt"])
	names := conflictNames(d, "doc")
	require.Len(t, names, 1)
	assert.Equal(t, "computer edit", d.files[names[0]])
	_, still := d.files["temp.txt"]
	assert.False(t, still)
	assert.Zero(t, d.deletes())
}

// Nothing changed on the server since the computer saw doc.txt: the rename replaces it, no copy,
// and a second save the same way is no conflict either.
func TestConflictUploadedTempRenameReplacesUnchangedFile(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "10Mi")
	d.set("doc.txt", "old version")
	_, err := f.NewObject(ctx, "doc.txt")
	require.NoError(t, err)
	for _, content := range []string{"edit one", "edit two"} {
		require.NoError(t, upload(ctx, f, "temp.txt", content))
		require.NoError(t, moveOver(ctx, f, "temp.txt", "doc.txt"))
		assert.Equal(t, content, d.files["doc.txt"])
	}
	assert.Empty(t, conflictNames(d, "doc"))
}

// Renaming to a name that does not exist is a plain move.
func TestConflictUploadedTempRenameToNewName(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "10Mi")
	require.NoError(t, upload(ctx, f, "temp.txt", "content"))
	require.NoError(t, moveOver(ctx, f, "temp.txt", "new.txt"))
	assert.Equal(t, "content", d.files["new.txt"])
	assert.Empty(t, conflictNames(d, "new"))
}
