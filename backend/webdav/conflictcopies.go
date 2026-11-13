package webdav

import (
	"context"
	"errors"

	"github.com/rclone/rclone/fs"
)

// rememberETag records the ETag the server reported for remote.
func (f *Fs) rememberETag(remote, etag string) {
	if f.opt.ConflictCopies && etag != "" {
		f.seenETags.Store(remote, etag)
	}
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
