package webdav_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// putNew writes remote as a new object (what the VFS does after a rename of a
// temporary file over an existing one) and returns the error.
func putNew(ctx context.Context, f fs.Fs, name, content string) error {
	src := &reopenable{object.NewStaticObjectInfo(name, time.Now(), int64(len(content)), true, nil, f), content}
	_, err := f.Put(ctx, newReader(content), src)
	return err
}

func TestConflictRenameOverSeenFileReplacesIt(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "10Mi")
	d.set("doc.txt", "v1")
	_, err := f.NewObject(ctx, "doc.txt") // rclone has seen the file's ETag
	require.NoError(t, err)
	require.NoError(t, putNew(ctx, f, "doc.txt", "saved by an editor"))
	assert.Equal(t, "saved by an editor", d.files["doc.txt"])
	assert.Empty(t, conflictNames(d, "doc"), "replacing a file rclone has seen, unchanged, is not a conflict")
}

func TestConflictRenameOverSeenFileChangedOnServer(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "10Mi")
	d.set("doc.txt", "v1")
	_, err := f.NewObject(ctx, "doc.txt")
	require.NoError(t, err)
	d.mu.Lock()
	d.set("doc.txt", "agent edit")
	d.mu.Unlock()
	require.NoError(t, putNew(ctx, f, "doc.txt", "saved by an editor"))
	assert.Equal(t, "agent edit", d.files["doc.txt"])
	require.Len(t, conflictNames(d, "doc"), 1)
}

func TestConflictRenameOverFileDeletedOnServerIsPlainCreate(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "10Mi")
	d.set("doc.txt", "v1")
	_, err := f.NewObject(ctx, "doc.txt")
	require.NoError(t, err)
	d.mu.Lock()
	delete(d.files, "doc.txt")
	d.mu.Unlock()
	require.NoError(t, putNew(ctx, f, "doc.txt", "created again"))
	assert.Equal(t, "created again", d.files["doc.txt"])
	assert.Empty(t, conflictNames(d, "doc"))
}

func TestConflictRenameOverChunkedFile(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "4B")
	d.set("big.bin", "0123456789")
	_, err := f.NewObject(ctx, "big.bin")
	require.NoError(t, err)
	require.NoError(t, putNew(ctx, f, "big.bin", "abcdefghijkl"))
	assert.Equal(t, "abcdefghijkl", d.files["big.bin"])
	assert.Empty(t, conflictNames(d, "big"))
}

func TestConflictNeverSeenFileStillConflicts(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "10Mi")
	d.set("same.txt", "agent")
	require.NoError(t, putNew(ctx, f, "same.txt", "computer"))
	assert.Equal(t, "agent", d.files["same.txt"])
	require.Len(t, conflictNames(d, "same"), 1)
}

func newReader(s string) *strings.Reader { return strings.NewReader(s) }

// A mount that restarted with an unsaved edit finds the file changed on the server and forgets the
// ETag it was based on. A listing before the upload (the refresher's first full refresh) must not
// make the server's new version the base again (vml-zrx9): the upload is a creation, so it conflicts.
func TestConflictListingAfterForgetDoesNotRebaseTheEdit(t *testing.T) {
	d, f, ctx := newConflictFs(t, true, "10Mi")
	d.set("doc.txt", "v1")
	_, err := f.NewObject(ctx, "doc.txt")
	require.NoError(t, err)
	d.mu.Lock()
	d.set("doc.txt", "machine text")
	d.mu.Unlock()
	f.(interface{ ForgetSeenETag(string) }).ForgetSeenETag("doc.txt")
	_, err = f.List(ctx, "") // a listing before the upload
	require.NoError(t, err)
	_, err = f.NewObject(ctx, "doc.txt") // and the single read the upload path does
	require.NoError(t, err)
	require.NoError(t, putNew(ctx, f, "doc.txt", "computer text"))
	assert.Equal(t, "machine text", d.files["doc.txt"], "the machine's version stays")
	require.Len(t, conflictNames(d, "doc"), 1, "the computer's text is saved as a conflict copy")
}
