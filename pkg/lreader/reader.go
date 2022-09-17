package lreader

import (
	"bytes"
	"errors"
	"io"
)

var ErrorReadOverflow = errors.New("try read with buffer overflow")

// Reader is a buffered line reader (zero-alocation during read)
type Reader struct {
	reader  io.Reader
	lastErr error
	buf     []byte
	pos     int
	end     int
}

func New(reader io.Reader, bufSize int) *Reader {
	return &Reader{
		reader: reader,
		buf:    make([]byte, bufSize),
	}
}

func (r *Reader) Reset(reader io.Reader) {
	r.reader = reader
	r.pos = 0
	r.end = 0
}

func (r *Reader) Grow(newSize int) {
	if len(r.buf) < newSize {
		buf := make([]byte, newSize)
		copy(buf, r.buf[r.pos:r.end])
		end := r.Len()
		r.pos = 0
		r.end = end
		r.buf = buf
	}
}

func (r *Reader) Empty() bool {
	return r.end == r.pos
}

func (r *Reader) Full() bool {
	return r.end == len(r.buf)
}

func (r *Reader) Len() int {
	return r.end - r.pos
}

func (r *Reader) Unreaded() []byte {
	return r.buf[r.pos:r.end]
}

func (r *Reader) Cap() int {
	return len(r.buf)
}

// Readline return next line bytes (from buffer, copy if need for future use).
//
// for overflow detect, compare error with ErrorReadOverflow, and use Grow and call next ReadLine if needed.
//
// If enf of file, io.EOF returned, may be try read later, if tail mode needed.
func (r *Reader) ReadUntil(delim byte) (b []byte, err error) {
	if r.end != r.pos && r.end != 0 {
		end := bytes.IndexByte(r.buf[r.pos:r.end], delim)
		if end > -1 {
			end += r.pos + 1
			b = r.buf[r.pos:end]
			if end == r.end {
				r.pos = 0
				r.end = 0
			} else {
				r.pos = end
			}
			return
		}
	}
	if r.pos != 0 {
		copy(r.buf, r.buf[r.pos:r.end])
		r.end = r.Len()
		r.pos = 0
	}
	if r.lastErr == nil || r.lastErr == io.EOF {
		var n int
		for {
			if r.Full() {
				// not reset pos and end, Reader can be enlarged with Grow
				err = ErrorReadOverflow
				return
			}
			n, r.lastErr = r.reader.Read(r.buf[r.end:])
			if n > 0 {
				r.end += n
				end := bytes.IndexByte(r.buf[r.pos:r.end], delim)
				if end > -1 {
					end += r.pos + 1
					b = r.buf[r.pos:end]
					if end == r.end {
						r.pos = 0
						r.end = 0
					} else {
						r.pos = end
					}
					return
				}
			}
			if r.lastErr != nil {
				break
			}
		}
	}
	err = r.lastErr
	return
}
