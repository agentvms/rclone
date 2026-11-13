package webdav

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
)

// rememberETag records the ETag the server reported for remote.
func (f *Fs) rememberETag(remote, etag string) {
	if f.opt.ConflictCopies && etag != "" {
		f.seenETags.Store(remote, etag)
	}
}

// rememberETagIfUnseen is rememberETag for a single-object read (NewObject): it
// keeps the ETag from the last listing, so that what the computer saw is not
// replaced by a version that appeared on the server since.
func (f *Fs) rememberETagIfUnseen(remote, etag string) {
	if f.opt.ConflictCopies && etag != "" {
		f.seenETags.LoadOrStore(remote, etag)
	}
}

// MoveReplaces tells operations.Move not to delete the destination before a
// move: Move decides what happens to a file that is replaced.
func (f *Fs) MoveReplaces() bool { return f.opt.ConflictCopies }

// moveKeepingChanges moves src to remote (a rename over an existing file: an
// editor saving a temporary file over the original, after the temporary file
// was already uploaded). When remote exists and is not the version rclone last
// saw (an agent changed it, or rclone never saw it), the server's file is kept
// and src is moved to a conflicted copy name instead.
func (f *Fs) moveKeepingChanges(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}
	cur, err := f.readMetaDataForPath(ctx, remote)
	if err != nil && !errors.Is(err, fs.ErrorObjectNotFound) {
		return nil, err
	}
	if err == nil {
		seen, ok := f.seenETags.Load(remote)
		if !ok || seen.(string) != cur.ETag {
			ext := path.Ext(remote)
			conflictRemote := fmt.Sprintf("%s (conflicted copy %s)%s", strings.TrimSuffix(remote, ext), time.Now().Format("2006-01-02 150405"), ext)
			if _, err := f.copyOrMove(ctx, src, conflictRemote, "MOVE"); err != nil {
				return nil, fmt.Errorf("conflict copy %q: %w", conflictRemote, err)
			}
			fs.Logf(remote, "Changed on the server since it was read: kept the server's version, saved the local version as %q", conflictRemote)
			// Describe the local version so the VFS fetches the server's next time
			o := f.createObject(remote, srcObj.modTime, srcObj.size)
			o.hasMetaData = true
			o.sha1, o.md5 = srcObj.sha1, srcObj.md5
			o.etag = conflictPendingETag
			return o, nil
		}
	}
	dst, err := f.copyOrMove(ctx, src, remote, "MOVE")
	if err != nil {
		return nil, err
	}
	f.seenETags.Delete(srcObj.remote)
	if d, ok := dst.(*Object); ok {
		f.rememberETag(remote, d.etag)
	}
	return dst, nil
}

// etagToReplace returns the ETag to send as If-Match when remote is written as
// a new object (a rename over an existing file, an editor saving a temporary
// file over the original). It is the ETag rclone last read for that path, if
// the file is still on the server. Empty means "a file rclone has not seen",
// which is uploaded with If-None-Match: *.
func (f *Fs) etagToReplace(ctx context.Context, remote string) string {
	if !f.opt.ConflictCopies {
		return ""
	}
	v, ok := f.seenETags.Load(remote)
	if !ok {
		return ""
	}
	if _, err := f.readMetaDataForPath(ctx, remote); errors.Is(err, fs.ErrorObjectNotFound) {
		f.seenETags.Delete(remote) // deleted on the server since: this is a new file again
		return ""
	}
	return v.(string)
}
