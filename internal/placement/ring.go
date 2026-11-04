package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
)

type point struct {
	hash uint64
	node string
}
type Ring struct{ points []point }

func hash(s string) uint64 { h := sha256.Sum256([]byte(s)); return binary.BigEndian.Uint64(h[:8]) }
func New(nodes []string, virtual int) *Ring {
	r := &Ring{}
	for _, n := range nodes {
		for v := 0; v < virtual; v++ {
			r.points = append(r.points, point{hash(fmt.Sprintf("%s:%d", n, v)), n})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		a, b := r.points[i], r.points[j]
		if a.hash == b.hash {
			return a.node < b.node
		}
		return a.hash < b.hash
	})
	return r
}
func (r *Ring) Select(key string, n int) []string {
	out := []string{}
	if len(r.points) == 0 {
		return out
	}
	at := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= hash(key) })
	seen := map[string]bool{}
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		p := r.points[(at+i)%len(r.points)]
		if !seen[p.node] {
			seen[p.node] = true
			out = append(out, p.node)
		}
	}
	return out
}
