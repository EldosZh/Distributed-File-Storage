package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidation(t *testing.T) {
	t.Setenv("DFS_INTERNAL_TOKEN", "")
	for _, tc := range []struct {
		body  string
		valid bool
	}{{"internal_token: test-internal-token\n", true}, {"internal_token: short\n", false}, {"internal_token: test-internal-token\nchunk_size: 17\n", false}, {"internal_token: test-internal-token\nrepair_workers: 0\n", false}, {"internal_token: test-internal-token\nunknown_option: true\n", false}} {
		p := filepath.Join(t.TempDir(), "config.yaml")
		if e := os.WriteFile(p, []byte(tc.body), 0600); e != nil {
			t.Fatal(e)
		}
		_, e := Load(p, false)
		if (e == nil) != tc.valid {
			t.Fatal(tc.body, e)
		}
	}
}
