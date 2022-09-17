package lreader

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestReader_LineReader(t *testing.T) {
	in := []byte("string 1\nline 2\nincomplete")
	r := bytes.NewReader(in)

	reader := New(r, 11)

	tests := []struct {
		want         []byte
		wantErr      bool
		wantEOF      bool
		wantOverflow bool
		wantBuf      []byte
	}{
		{
			want: []byte("string 1\n"),
		},
		{
			want: []byte("line 2\n"),
		},
		{
			want:    nil,
			wantBuf: []byte("incomplete"),
			wantEOF: true,
		},
		{
			want:    nil,
			wantEOF: true,
		},
	}

	for i, tt := range tests {
		got, err := reader.ReadUntil('\n')
		if !bytes.Equal(tt.want, got) {
			t.Errorf("[%d] ReadUntil('\\n') want '%s', got '%s'", i,
				strings.ReplaceAll(string(tt.want), "\n", "\\n"),
				strings.ReplaceAll(string(got), "\n", "\\n"),
			)
		}
		if tt.wantBuf != nil {
			unreaded := reader.Unreaded()
			if !bytes.Equal(tt.wantBuf, unreaded) {
				t.Errorf("[%d] Unreader() want '%s', got '%s'", i,
					strings.ReplaceAll(string(tt.want), "\n", "\\n"),
					strings.ReplaceAll(string(got), "\n", "\\n"),
				)
			}
		}
		if tt.wantOverflow {
			if err == ErrorReadOverflow {
				reader.Grow(reader.Cap() + 10)
				continue
			}
			t.Fatalf("[%d] ReadUntil('\\n') error = %#v, wantOverflow %v", i, err, tt.wantOverflow)
		}
		if tt.wantEOF {
			if err != io.EOF {
				t.Fatalf("[%d] ReadUntil('\\n') error = %#v, wantEOF %v", i, err, tt.wantEOF)
			}
		} else if (err != nil) != tt.wantErr {
			t.Fatalf("[%d] ReadUntil('\\n') error = %#v, wantErr %v", i, err, tt.wantErr)
		}
	}
}

func TestReader_LineReaderOverflow(t *testing.T) {
	in := []byte("string 1\nline 2\n")
	r := bytes.NewReader(in)

	reader := New(r, 8)

	tests := []struct {
		want         []byte
		wantErr      bool
		wantEOF      bool
		wantOverflow bool
		wantBuf      []byte
	}{
		{
			wantBuf:      []byte("string 1"),
			wantErr:      true,
			wantOverflow: true,
		},
		{
			want: []byte("string 1\n"),
		},
		{
			want: []byte("line 2\n"),
		},
		{
			want:    nil,
			wantEOF: true,
		},
	}

	for i, tt := range tests {
		got, err := reader.ReadUntil('\n')
		if !bytes.Equal(tt.want, got) {
			t.Errorf("[%d] ReadUntil('\\n') want '%s', got '%s'", i,
				strings.ReplaceAll(string(tt.want), "\n", "\\n"),
				strings.ReplaceAll(string(got), "\n", "\\n"),
			)
		}
		if tt.wantBuf != nil {
			unreaded := reader.Unreaded()
			if !bytes.Equal(tt.wantBuf, unreaded) {
				t.Errorf("[%d] Unreader() want '%s', got '%s'", i,
					strings.ReplaceAll(string(tt.want), "\n", "\\n"),
					strings.ReplaceAll(string(got), "\n", "\\n"),
				)
			}
		}
		if tt.wantOverflow {
			if err == ErrorReadOverflow {
				reader.Grow(reader.Cap() + 10)
				continue
			}
			t.Fatalf("[%d] ReadUntil('\\n') error = %#v, wantOverflow %v", i, err, tt.wantOverflow)
		}
		if tt.wantEOF {
			if err != io.EOF {
				t.Fatalf("[%d] ReadUntil('\\n') error = %#v, wantEOF %v", i, err, tt.wantEOF)
			}
		} else if (err != nil) != tt.wantErr {
			t.Fatalf("[%d] ReadUntil('\\n') error = %#v, wantErr %v", i, err, tt.wantErr)
		}
	}
}

func TestReader_LineReaderFile(t *testing.T) {
	ifp, err := os.CreateTemp("", "log-exporter")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ifp.Close()
		os.Remove(ifp.Name())
	}()

	ofp, err := os.Open(ifp.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer ofp.Close()

	reader := New(ofp, 10)

	tests := []struct {
		truncate     bool
		in           string
		want         []byte
		wantErr      bool
		wantEOF      bool
		wantOverflow bool
		wantBuf      []byte
	}{
		{
			in:   "string 1\n",
			want: []byte("string 1\n"),
		},
		{
			in:      "line",
			wantBuf: []byte("line"),
			wantEOF: true,
		},
		{
			in:   " 2\n",
			want: []byte("line 2\n"),
		},
		{
			truncate: true,
			in:       "string 3\n",
			want:     []byte("string 3\n"),
		},
		{
			want:    nil,
			wantEOF: true,
		},
	}

	for i, tt := range tests {
		if tt.truncate {
			ifp.WriteString(tt.in)
			ifp.Truncate(0)
			ifp.Seek(0, 0)
		}
		_, err := ifp.WriteString(tt.in)
		if err != nil {
			t.Fatalf("[%d] fp.WriteString() error = %#v", i, err)
		}
		ifp.Sync()
		if tt.truncate {
			got, err := reader.ReadUntil('\n')
			if !bytes.Equal(nil, got) {
				t.Errorf("[%d] ReadUntil('\\n') want be empty, got '%s'", i,
					strings.ReplaceAll(string(got), "\n", "\\n"),
				)
			}
			if err != io.EOF {
				t.Fatalf("[%d] ReadUntil('\\n') error = %#v, wantEOF %v before seek", i, err, tt.wantEOF)
			}
			ofp.Seek(0, 0)
		}
		got, err := reader.ReadUntil('\n')
		if !bytes.Equal(tt.want, got) {
			t.Errorf("[%d] ReadUntil('\\n') want '%s', got '%s'", i,
				strings.ReplaceAll(string(tt.want), "\n", "\\n"),
				strings.ReplaceAll(string(got), "\n", "\\n"),
			)
		}
		if tt.wantBuf != nil {
			unreaded := reader.Unreaded()
			if !bytes.Equal(tt.wantBuf, unreaded) {
				t.Errorf("[%d] Unreader() want '%s', got '%s'", i,
					strings.ReplaceAll(string(tt.want), "\n", "\\n"),
					strings.ReplaceAll(string(got), "\n", "\\n"),
				)
			}
		}
		if tt.wantOverflow {
			if err == ErrorReadOverflow {
				reader.Grow(reader.Cap() + 10)
				continue
			}
			t.Fatalf("[%d] ReadUntil('\\n') error = %#v, wantOverflow %v", i, err, tt.wantOverflow)
		}
		if tt.wantEOF {
			if err != io.EOF {
				t.Fatalf("[%d] ReadUntil('\\n') error = %#v, wantEOF %v", i, err, tt.wantEOF)
			}
		} else if (err != nil) != tt.wantErr {
			t.Fatalf("[%d] ReadUntil('\\n') error = %#v, wantErr %v", i, err, tt.wantErr)
		}
	}
}

func generate(lines []string, size int) ([]byte, int, int) {
	var b bytes.Buffer
	b.Grow(size)

	i := 0
	n := 0
	maxLen := 0
	for b.Len() < size {
		b.WriteString(lines[i])
		if maxLen < len(lines[i]) {
			maxLen = len(lines[i])
		}
		i++
		n++
		if i == len(lines) {
			i = 0
		}
	}

	return b.Bytes(), n, maxLen
}

var benchLines []string = []string{
	"line 1\n", "string 2\n", "long long string with some symbols\n", "string 3\n", "line 4\n",
	"string 5\n", "another long long string with some another symbols\n", "string 6\n", "line 7\n",
	"string 8\n", "happy new year\n", "string 9\n", "line 10\n",
	"very long string very long string very long string very long string very long string very long string very long string very long string very long string\n",
}

func benchmarkReader(b *testing.B, name string, benchData []byte, bufSize, maxLen, n int) {
	b.Run(fmt.Sprintf("Reader(%s) size=%d, buf_size=%d, max_len=%d", name, len(benchData), bufSize, maxLen), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var (
				err    error
				data   []byte
				readed int
			)
			b.StopTimer()
			r := bytes.NewReader(benchData)
			b.StartTimer()
			reader := New(r, bufSize)
			for err == nil {
				data, err = reader.ReadUntil('\n')
				if len(data) > 0 {
					readed++
				}
			}
			if n != readed {
				b.Fatalf("lines read %d, want %d, err %#v", readed, n, err)
			}
		}
	})
}

func benchmarkBufioReader(b *testing.B, name string, benchData []byte, bufSize, maxLen, n int) {
	b.Run(fmt.Sprintf("bufio.Reader(%s) size=%d, buf_size=%d, max_len=%d", name, len(benchData), bufSize, maxLen), func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var (
				err    error
				data   []byte
				readed int
			)
			b.StopTimer()
			r := bytes.NewReader(benchData)
			b.StartTimer()
			reader := bufio.NewReaderSize(r, bufSize)
			for err == nil {
				data, _, err = reader.ReadLine()
				if len(data) > 0 {
					readed++
				}
			}
			if n != readed {
				b.Fatalf("lines read %d, want %d, err %#v", readed, n, err)
			}
		}
	})
}

func benchmarkReaders(b *testing.B, size int) {
	benchData, n, maxLen := generate(benchLines, size)

	bufSize := len(benchData)
	benchmarkReader(b, "All", benchData, bufSize, maxLen, n)
	benchmarkBufioReader(b, "All", benchData, bufSize, maxLen, n)

	bufSize = maxLen
	benchmarkReader(b, "maxLine", benchData, bufSize, maxLen, n)
	benchmarkBufioReader(b, "maxLine", benchData, bufSize, maxLen, n)
}

func BenchmarkReader1024M(b *testing.B) {
	benchmarkReaders(b, 1024*1024)
}
