package chunk

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

func Hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func Valid(id string) bool {
	b, e := hex.DecodeString(id)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == id
}

// Walk holds at most size bytes, reusing the buffer after the callback returns.
func Walk(r io.Reader, size int, fn func(int, []byte) error) (string, int64, error) {
	h := sha256.New()
	var total int64
	buf := make([]byte, size)
	for seq := 0; ; seq++ {
		n, e := io.ReadFull(r, buf)
		if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
			return "", total, e
		}
		if n > 0 {
			h.Write(buf[:n])
			total += int64(n)
			if err := fn(seq, buf[:n]); err != nil {
				return "", total, err
			}
		}
		if e != nil {
			break
		}
	}
	return hex.EncodeToString(h.Sum(nil)), total, nil
}
