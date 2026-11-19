package webdav

import (
	"context"
	"io"
	"net"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Reading a file: a failure to connect is not retried when conflict copies are on (the mount fails an offline open after one connect
// timeout); a listing keeps its retry.
func TestShouldRetryDialError(t *testing.T) {
	dial := &url.Error{Op: "Get", URL: "http://x/f", Err: &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}}
	read := &url.Error{Op: "Get", URL: "http://x/f", Err: &net.OpError{Op: "read", Net: "tcp", Err: io.ErrUnexpectedEOF}}
	on := &Fs{opt: Options{ConflictCopies: true}}
	off := &Fs{}
	retry, _ := on.shouldRetryOpen(context.Background(), nil, dial)
	assert.False(t, retry, "dial error, conflict copies on")
	retry, _ = on.shouldRetryOpen(context.Background(), nil, read)
	assert.True(t, retry, "a dropped connection is still retried")
	retry, _ = off.shouldRetryOpen(context.Background(), nil, dial)
	assert.True(t, retry, "conflict copies off: unchanged")
}

func TestShouldRetryListingDialError(t *testing.T) {
	dial := &url.Error{Op: "Propfind", URL: "http://x/d/", Err: &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}}
	on := &Fs{opt: Options{ConflictCopies: true}}
	retry, _ := on.shouldRetry(context.Background(), nil, dial)
	assert.True(t, retry, "a listing request is still retried after a failed connect")
}
