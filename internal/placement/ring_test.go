package placement

import (
	"fmt"
	"reflect"
	"testing"
)

func TestRing(t *testing.T) {
	a := New([]string{"a", "b", "c"}, 128)
	b := New([]string{"c", "b", "a"}, 128)
	if len(a.points) != 384 {
		t.Fatal("virtual nodes")
	}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprint(i)
		x, y := a.Select(key, 3), b.Select(key, 3)
		if !reflect.DeepEqual(x, y) || len(x) != 3 || x[0] == x[1] || x[1] == x[2] || x[0] == x[2] {
			t.Fatal(x, y)
		}
	}
	if len(New(nil, 128).Select("x", 3)) != 0 || len(New([]string{"a"}, 128).Select("x", 3)) != 1 {
		t.Fatal("insufficient nodes")
	}
	add := New([]string{"a", "b", "c", "d"}, 128)
	remove := New([]string{"a", "b"}, 128)
	moved := 0
	for i := 0; i < 1000; i++ {
		k := fmt.Sprint(i)
		old := a.Select(k, 1)[0]
		next := add.Select(k, 1)[0]
		if old != next {
			moved++
			if next != "d" {
				t.Fatal("addition remapped between existing nodes")
			}
		}
		r := remove.Select(k, 1)[0]
		if old != "c" && r != old {
			t.Fatal("removal remapped unrelated key")
		}
	}
	if moved < 100 || moved > 500 {
		t.Fatal("unexpected ring distribution", moved)
	}
}
