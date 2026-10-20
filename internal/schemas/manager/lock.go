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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"slices"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	crversion "github.com/crossplane/crossplane-runtime/v2/pkg/version"
)

const (
	lockFileName = ".lock.json"

	// lockVersion is the current lock file format. A lock with any other
	// version is ignored and overwritten on the next change.
	lockVersion = 1
)

// Flags are the generation options, beyond languages, that change the content
// of the generated files. They are recorded per source so that changing one
// regenerates the schemas it affects.
type Flags struct {
	GoRuntimeObjects bool `json:"goRuntimeObjects"`
	// APIGroups are the globs resources are filtered by, sorted. Empty means
	// no filtering.
	APIGroups []string `json:"apiGroups,omitempty"`
}

func (f Flags) equal(o Flags) bool {
	return f.GoRuntimeObjects == o.GoRuntimeObjects && slices.Equal(f.APIGroups, o.APIGroups)
}

// lock tracks the sources whose schemas are present in the manager and the
// files generated from each. It is persisted to the manager's filesystem.
type lock struct {
	LockVersion int `json:"lockVersion"`
	// DependenciesHash is the hash of the project's dependencies the lock was
	// last updated for. It lets Verify notice changed dependencies without the
	// network.
	DependenciesHash string                `json:"dependenciesHash,omitempty"`
	Sources          map[string]lockSource `json:"sources"`
	// Run holds the lock entry of every run-level language: one whose
	// generator describes the whole source set in a single generation run
	// (see schemaMerger), so its files belong to the run rather than to any
	// single source.
	Run map[string]lockRun `json:"run,omitempty"`
}

// lockSource is the lock entry of a single source.
type lockSource struct {
	SourceHash string `json:"sourceHash"`
	// Pin is an immutable reference to fetch the source's content by, if the
	// source has one.
	Pin string `json:"pin,omitempty"`
	// Version is the resolved tag or ref, shown to people. It is optional.
	Version string `json:"version,omitempty"`
	// Generator is a fingerprint of everything outside the source that
	// determines what the generators produced from it: the CLI build version
	// and each generator's own fingerprint (its pinned toolchain image and
	// options). It is recorded per source, not once for the lock, because a
	// pass that fails partway leaves some sources generated under the new
	// fingerprint and others under the old one. Files made under a different
	// fingerprint are stale, so an upgraded CLI that changes generator output
	// forces `schema update` instead of mysteriously failing `schema
	// generate`.
	Generator string `json:"generator,omitempty"`
	Flags     Flags  `json:"flags"`
	// Languages are the sorted languages the source was generated for. They
	// are recorded because a language may produce no files for a source.
	Languages []string   `json:"languages"`
	Files     []lockFile `json:"files"`
}

// lockRun is the lock entry of a run-level language. Its files are generated
// from the union of all sources in one merged pass, so the entry records the
// sources it was generated from instead of claiming the files for any one of
// them.
type lockRun struct {
	// Sources maps each source key to the content hash its part was
	// generated from. The run is fresh only when this exactly matches the
	// live sources.
	Sources map[string]string `json:"sources"`
	// Generator is the generator fingerprint the run files were made under.
	Generator string `json:"generator,omitempty"`
	// Flags are the generation flags the run files were made with.
	Flags Flags      `json:"flags"`
	Files []lockFile `json:"files"`
}

// LockedSource is the read-only view of a source's lock entry.
type LockedSource struct {
	Pin        string
	Version    string
	SourceHash string
}

// lockFile is a generated file, identified by its slash-separated path
// relative to the managed directory, and the hash of its content.
type lockFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func newLock() *lock {
	return &lock{
		LockVersion: lockVersion,
		Sources:     make(map[string]lockSource),
	}
}

// owned returns the set of file paths recorded by any source or run entry.
func (l *lock) owned() map[string]struct{} {
	owned := make(map[string]struct{})
	for _, src := range l.Sources {
		for _, f := range src.Files {
			owned[f.Path] = struct{}{}
		}
	}
	for _, run := range l.Run {
		for _, f := range run.Files {
			owned[f.Path] = struct{}{}
		}
	}
	return owned
}

// ownedFile is a file a lock source records, with the source that owns it.
type ownedFile struct {
	Source string
	SHA256 string
}

// ownedBy returns every file path recorded by one of the given sources other
// than key, with its hash and its owner. Only sources that are settled in the
// current pass are passed in: the entry of a source that has not been Added
// yet may be about to change, and a source that was removed is about to be
// reconciled away, so neither can conflict with anything.
func (l *lock) ownedBy(key string, settled map[string]struct{}) map[string]ownedFile {
	owned := make(map[string]ownedFile)
	for k := range settled {
		if k == key {
			continue
		}
		for _, f := range l.Sources[k].Files {
			owned[f.Path] = ownedFile{Source: k, SHA256: f.SHA256}
		}
	}
	return owned
}

// conflict returns an error naming the first path, in sorted order, that two
// sources generate differently. files maps each path of key to its hash.
func (l *lock) conflict(key string, files map[string]string, settled map[string]struct{}) error {
	owned := l.ownedBy(key, settled)
	for _, p := range sortedKeys(files) {
		if other, ok := owned[p]; ok && other.SHA256 != files[p] {
			return errors.Errorf("cannot generate schemas for %s: %s was also generated differently by %s; the sources disagree about this file", key, p, other.Source)
		}
	}
	return nil
}

func hashContent(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// generatorFingerprint hashes everything outside the sources that determines
// what the configured generators produce. Each generator contributes its
// language and its own Fingerprint() -- its pinned toolchain image and
// options -- so any change that alters what a generator produces also
// changes the fingerprint and marks locked files stale. m.languages() is
// sorted, so the parts are stable for the same generator set.
func (m *Manager) generatorFingerprint() string {
	parts := make([]string, 0, len(m.generators)+1)
	parts = append(parts, "cli="+crversion.New().GetVersionString())
	for _, g := range m.generators {
		parts = append(parts, g.Language()+"="+g.Fingerprint())
	}
	return hashParts(parts)
}

// hashParts returns a short hex digest of the given strings, each length-
// prefixed so no two different part lists can hash the same.
func hashParts(parts []string) string {
	h := sha256.New()
	var lenBuf [8]byte
	for _, p := range parts {
		binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(p)))
		_, _ = h.Write(lenBuf[:])
		_, _ = h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}
