package chunk

import (
	"bytes"
	"io"
	"testing"
)

type endless struct{}

func (endless) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = byte(i % 251)
	}
	return len(b), nil
}
func TestStreaming(t *testing.T) {
	for _, size := range []int{0, 1, 1024, 1025, 4097} {
		t.Run(string(rune(size+65)), func(t *testing.T) {
			data := bytes.Repeat([]byte{42}, size)
			got := []byte{}
			sum, n, e := Walk(bytes.NewReader(data), 1024, func(seq int, b []byte) error {
				if len(b) > 1024 {
					t.Fatal("unbounded chunk")
				}
				got = append(got, b...)
				return nil
			})
			if e != nil || n != int64(size) || !bytes.Equal(data, got) || sum != Hash(data) {
				t.Fatal(n, e)
			}
		})
	}
	var count int
	_, n, e := Walk(io.LimitReader(endless{}, 64<<20), 1<<20, func(_ int, b []byte) error { count++; return nil })
	if e != nil || n != 64<<20 || count != 64 {
		t.Fatal(count, n, e)
	}
}
func TestHash(t *testing.T) {
	if Hash([]byte("abc")) != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatal("SHA-256 vector")
	}
	if !Valid(Hash(nil)) || Valid("../bad") || Valid("AB"+Hash(nil)[2:]) {
		t.Fatal("validation")
	}
}
func BenchmarkHash(b *testing.B) {
	data := make([]byte, 4<<20)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		Hash(data)
	}
}
func BenchmarkStreaming(b *testing.B) {
	b.SetBytes(64 << 20)
	for b.Loop() {
		_, _, _ = Walk(io.LimitReader(endless{}, 64<<20), 4<<20, func(int, []byte) error { return nil })
	}
}
