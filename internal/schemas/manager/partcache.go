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
	"compress/gzip"
	"crypto/rand"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/afero"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
)

const (
	// partCacheVersion is part of every part cache key. Bump it when the
	// format of an entry changes.
	partCacheVersion = "1"

	// partCacheMaxAge is how long a part cache entry is kept after it was
	// last used.
	partCacheMaxAge = 30 * 24 * time.Hour

	partCacheExt = ".tar.gz"
)

// DefaultPartCacheDir returns the default per-user cache directory for the
// per-source parts of run-level languages.
func DefaultPartCacheDir() string {
	base, err := os.UserCacheDir()
	if err != nil {
		base = os.TempDir()
	}
	return filepath.Join(base, "crossplane", "schemas")
}

// partCache stores the part a run-level generator produced for one source, so
// a run-level language is only regenerated for the sources that changed, and
// can be repaired without fetching the sources whose parts are cached.
//
// An entry is keyed by everything that determines the part: the language, the
// generator's fingerprint, the flags and the source's key and content hash.
// It never needs invalidating; entries nobody used for partCacheMaxAge are
// pruned. Each entry is one gzipped tarball, written to a temporary file and
// renamed into place, so readers never see a partial entry.
//
// The cache is an optimization only: an entry that cannot be read is a miss,
// and an entry that cannot be written is skipped.
type partCache struct {
	fs  afero.Fs
	now func() time.Time
}

// partKey returns the cache key of a source's part.
func partKey(lang, fingerprint string, flags Flags, sourceKey, sourceHash string) string {
	return hashPartsFull(append([]string{partCacheVersion, lang, fingerprint, sourceKey, sourceHash}, flags.parts()...))
}

// get returns the cached part, if there is one.
func (c *partCache) get(lang, key string) (afero.Fs, bool) {
	if c == nil {
		return nil, false
	}
	p := path.Join(lang, key+partCacheExt)
	f, err := c.fs.Open(p)
	if err != nil {
		return nil, false
	}
	defer func() { _ = f.Close() }()

	part, err := untarPart(f)
	if err != nil {
		return nil, false
	}
	// Mark the entry used, so prune keeps it.
	now := c.now()
	_ = c.fs.Chtimes(p, now, now)
	return part, true
}

// put stores a part. A nil part is stored as an empty one.
func (c *partCache) put(lang, key string, part afero.Fs) {
	if c == nil {
		return
	}
	if err := c.fs.MkdirAll(lang, 0o750); err != nil {
		return
	}
	var suffix [8]byte
	_, _ = rand.Read(suffix[:])
	tmp := path.Join(lang, ".tmp-"+key+"-"+hex.EncodeToString(suffix[:]))
	f, err := c.fs.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	err = tarPart(f, part)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = c.fs.Rename(tmp, path.Join(lang, key+partCacheExt))
	}
	if err != nil {
		_ = c.fs.Remove(tmp)
	}
}

// prune removes the language's entries, and leftover temporary files, that
// were last used longer than partCacheMaxAge ago.
func (c *partCache) prune(lang string) {
	if c == nil {
		return
	}
	entries, err := afero.ReadDir(c.fs, lang)
	if err != nil {
		return
	}
	cutoff := c.now().Add(-partCacheMaxAge)
	for _, e := range entries {
		if e.IsDir() || e.ModTime().After(cutoff) {
			continue
		}
		if strings.HasSuffix(e.Name(), partCacheExt) || strings.HasPrefix(e.Name(), ".tmp-") {
			_ = c.fs.Remove(path.Join(lang, e.Name()))
		}
	}
}

// tarPart writes the files of part to w as a gzipped tarball, in path order.
func tarPart(w io.Writer, part afero.Fs) error {
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	if part != nil {
		// afero.Walk visits paths in lexical order, so the tarball is stable.
		err := afero.Walk(part, ".", func(p string, info fs.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			content, err := afero.ReadFile(part, p)
			if err != nil {
				return err
			}
			hdr := &tar.Header{Name: filepath.ToSlash(p), Mode: 0o600, Size: int64(len(content)), Typeflag: tar.TypeReg}
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			_, err = tw.Write(content)
			return err
		})
		if err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

// untarPart reads a gzipped tarball written by tarPart into a new filesystem.
func untarPart(r io.Reader) (afero.Fs, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer func() { _ = gz.Close() }()

	part := afero.NewMemMapFs()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return part, nil
		}
		if err != nil {
			return nil, err
		}
		name := path.Clean(hdr.Name)
		if hdr.Typeflag != tar.TypeReg || path.IsAbs(name) || name == ".." || strings.HasPrefix(name, "../") {
			return nil, errors.Errorf("unexpected entry %q in part cache", hdr.Name)
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		if err := part.MkdirAll(path.Dir(name), 0o750); err != nil {
			return nil, err
		}
		if err := afero.WriteFile(part, name, content, 0o600); err != nil {
			return nil, err
		}
	}
}
