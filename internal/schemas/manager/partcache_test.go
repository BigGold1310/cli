/*
Copyright 2026 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package manager

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/afero"
)

func TestPartCache_RoundTrip(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		reason string
		files  map[string]string
	}{
		"NestedFiles": {
			reason: "A part's files and layout must come back exactly, models/ prefix included.",
			files:  map[string]string{"models/index.ts": "I", "models/io/k8s/v1.ts": "K"},
		},
		"EmptyPart": {
			reason: "A source that produced nothing is cached as an empty part, so it is not regenerated.",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c := &partCache{fs: afero.NewMemMapFs(), now: time.Now}
			var part afero.Fs
			if tc.files != nil {
				part = afero.NewMemMapFs()
				for p, content := range tc.files {
					if err := afero.WriteFile(part, p, []byte(content), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			c.put("ts", "k", part)

			got, ok := c.get("ts", "k")
			if !ok {
				t.Fatalf("%s\nget: miss after put", tc.reason)
			}
			if diff := cmp.Diff(tc.files, readFiles(t, got)); diff != "" {
				t.Errorf("%s\n-want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestPartCache_Miss(t *testing.T) {
	t.Parallel()

	var nilCache *partCache
	if _, ok := nilCache.get("ts", "k"); ok {
		t.Error("a nil cache must always miss")
	}
	nilCache.put("ts", "k", nil) // Must not panic.

	c := &partCache{fs: afero.NewMemMapFs(), now: time.Now}
	if _, ok := c.get("ts", "absent"); ok {
		t.Error("get of an absent key must miss")
	}
}

func TestPartCache_RejectsEscapingPaths(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "../escape", Mode: 0o600, Size: 1, Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := untarPart(&buf); err == nil {
		t.Error("untarPart accepted an entry outside the part")
	}
}

func TestPartCache_Prune(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	cacheFS := afero.NewMemMapFs()
	c := &partCache{fs: cacheFS, now: func() time.Time { return now }}
	c.put("ts", "old", nil)
	c.put("ts", "used", nil)
	c.put("ts", "fresh", nil)
	if err := afero.WriteFile(cacheFS, "ts/.tmp-crashed", []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := now.Add(-partCacheMaxAge - time.Hour)
	for _, p := range []string{"ts/old.tar.gz", "ts/used.tar.gz", "ts/.tmp-crashed"} {
		if err := cacheFS.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}
	// Reading an entry marks it used.
	if _, ok := c.get("ts", "used"); !ok {
		t.Fatal("get: miss")
	}

	c.prune("ts")

	entries, err := afero.ReadDir(cacheFS, "ts")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(entries))
	for _, e := range entries {
		got = append(got, e.Name())
	}
	if diff := cmp.Diff([]string{"fresh.tar.gz", "used.tar.gz"}, got); diff != "" {
		t.Errorf("entries after prune: -want, +got:\n%s", diff)
	}
}
