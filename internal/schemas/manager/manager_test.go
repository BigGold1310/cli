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
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/spf13/afero"

	devv1alpha1 "github.com/crossplane/cli/v2/apis/dev/v1alpha1"
	"github.com/crossplane/cli/v2/internal/schemas/generator"
	"github.com/crossplane/cli/v2/internal/schemas/runner"
)

// lf builds the expected lock entry for a generated file.
func lf(path, content string) lockFile {
	return lockFile{Path: path, SHA256: hashContent([]byte(content))}
}

func TestManager_Add(t *testing.T) {
	t.Parallel()

	// run is one Add call. Every run gets a fresh manager over the same
	// filesystem, like consecutive invocations of the CLI.
	type run struct {
		gens  []generator.Interface
		flags Flags
		src   *mockSource
		// tamper changes the managed directory before the call.
		tamper func(t *testing.T, fs afero.Fs)
	}
	type args struct {
		// lockJSON is written to the lock file before the first run.
		lockJSON string
		runs     []run
	}
	type want struct {
		lock  *lock
		files map[string]string
		err   error
	}

	cr := func(files map[string]string) []generator.Interface {
		return []generator.Interface{&mockGenerator{lang: "mock", files: files}}
	}
	src := func(hash string) *mockSource { return &mockSource{key: "xpkg://pkg", hash: hash} }

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"EmptyLock": {
			reason: "A source that is not locked is generated and recorded with its hash and files.",
			args: args{runs: []run{
				{gens: cr(map[string]string{"a.json": "A"}), src: src("h1")},
			}},
			want: want{
				lock: &lock{LockVersion: 1, Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h1", Languages: []string{"mock"}, Files: []lockFile{lf("mock/a.json", "A")}},
				}},
				files: map[string]string{"mock/a.json": "A"},
			},
		},
		"UnchangedHashIsNoOp": {
			reason: "A source whose hash matches the lock is not regenerated, even if the generator would now produce different files.",
			args: args{runs: []run{
				{gens: cr(map[string]string{"a.json": "A"}), src: src("h1")},
				{gens: cr(map[string]string{"a.json": "CHANGED"}), src: src("h1")},
			}},
			want: want{
				lock: &lock{LockVersion: 1, Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h1", Languages: []string{"mock"}, Files: []lockFile{lf("mock/a.json", "A")}},
				}},
				files: map[string]string{"mock/a.json": "A"},
			},
		},
		"ChangedHashRegeneratesAndPrunes": {
			reason: "A new hash regenerates the source, and files it no longer produces are removed and reported.",
			args: args{runs: []run{
				{gens: cr(map[string]string{"a.json": "A", "b.json": "B"}), src: src("h1")},
				{gens: cr(map[string]string{"a.json": "A2"}), src: src("h2")},
			}},
			want: want{
				lock: &lock{LockVersion: 1, Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h2", Languages: []string{"mock"}, Files: []lockFile{lf("mock/a.json", "A2")}},
				}},
				files: map[string]string{"mock/a.json": "A2"},
			},
		},
		"ChangedFlagsRegenerate": {
			reason: "Flags are part of what a source was generated with, so changing one regenerates it.",
			args: args{runs: []run{
				{gens: cr(map[string]string{"a.json": "A"}), src: src("h1")},
				{gens: cr(map[string]string{"a.json": "A2"}), src: src("h1"), flags: Flags{GoRuntimeObjects: true}},
			}},
			want: want{
				lock: &lock{LockVersion: 1, Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h1", Flags: Flags{GoRuntimeObjects: true}, Languages: []string{"mock"}, Files: []lockFile{lf("mock/a.json", "A2")}},
				}},
				files: map[string]string{"mock/a.json": "A2"},
			},
		},
		"ChangedAPIGroupsRegenerate": {
			reason: "Changing the API group filter changes what a source generates, so it is regenerated and recorded sorted.",
			args: args{runs: []run{
				{gens: cr(map[string]string{"a.json": "A", "b.json": "B"}), src: src("h1")},
				{gens: cr(map[string]string{"a.json": "A"}), src: src("h1"), flags: Flags{APIGroups: []string{"b.*", "a.*"}}},
			}},
			want: want{
				lock: &lock{LockVersion: 1, Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h1", Flags: Flags{APIGroups: []string{"a.*", "b.*"}}, Languages: []string{"mock"}, Files: []lockFile{lf("mock/a.json", "A")}},
				}},
				files: map[string]string{"mock/a.json": "A"},
			},
		},
		"APIGroupOrderIsIgnored": {
			reason: "The same globs in another order are the same filter and must not regenerate.",
			args: args{runs: []run{
				{gens: cr(map[string]string{"a.json": "A"}), src: src("h1"), flags: Flags{APIGroups: []string{"a.*", "b.*"}}},
				{gens: cr(map[string]string{"a.json": "CHANGED"}), src: src("h1"), flags: Flags{APIGroups: []string{"b.*", "a.*"}}},
			}},
			want: want{
				lock: &lock{LockVersion: 1, Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h1", Flags: Flags{APIGroups: []string{"a.*", "b.*"}}, Languages: []string{"mock"}, Files: []lockFile{lf("mock/a.json", "A")}},
				}},
				files: map[string]string{"mock/a.json": "A"},
			},
		},
		"AddedLanguageRegenerates": {
			reason: "A language that has no files for the source yet is detected from the file paths and generated.",
			args: args{runs: []run{
				{gens: cr(map[string]string{"a.json": "A"}), src: src("h1")},
				{
					gens: []generator.Interface{
						&mockGenerator{lang: "mock", files: map[string]string{"a.json": "A"}},
						&mockGenerator{lang: "other", files: map[string]string{"a.txt": "T"}},
					},
					src: src("h1"),
				},
			}},
			want: want{
				lock: &lock{LockVersion: 1, Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h1", Languages: []string{"mock", "other"}, Files: []lockFile{lf("mock/a.json", "A"), lf("other/a.txt", "T")}},
				}},
				files: map[string]string{"mock/a.json": "A", "other/a.txt": "T"},
			},
		},
		"RemovedLanguageRegeneratesAndPrunes": {
			reason: "A language that is no longer generated regenerates the source, and its files are removed.",
			args: args{runs: []run{
				{
					gens: []generator.Interface{
						&mockGenerator{lang: "mock", files: map[string]string{"a.json": "A"}},
						&mockGenerator{lang: "other", files: map[string]string{"a.txt": "T"}},
					},
					src: src("h1"),
				},
				{gens: cr(map[string]string{"a.json": "A"}), src: src("h1")},
			}},
			want: want{
				lock: &lock{LockVersion: 1, Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h1", Languages: []string{"mock"}, Files: []lockFile{lf("mock/a.json", "A")}},
				}},
				files: map[string]string{"mock/a.json": "A"},
			},
		},
		"DriftIsReportedNotResolved": {
			reason: "Add reports edited and deleted files but leaves them alone; repairing is the job of Generate.",
			args: args{runs: []run{
				{gens: cr(map[string]string{"a.json": "A", "b.json": "B"}), src: src("h1")},
				{
					gens: cr(map[string]string{"a.json": "A", "b.json": "B"}),
					src:  src("h1"),
					tamper: func(t *testing.T, fs afero.Fs) {
						t.Helper()
						if err := afero.WriteFile(fs, "mock/a.json", []byte("edited"), 0o600); err != nil {
							t.Fatal(err)
						}
						if err := fs.Remove("mock/b.json"); err != nil {
							t.Fatal(err)
						}
					},
				},
			}},
			want: want{
				lock: &lock{LockVersion: 1, Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h1", Languages: []string{"mock"}, Files: []lockFile{lf("mock/a.json", "A"), lf("mock/b.json", "B")}},
				}},
				files: map[string]string{"mock/a.json": "edited"},
			},
		},
		"OldLockIsOverwritten": {
			reason: "A lock without lockVersion 1 is ignored and replaced.",
			args: args{
				lockJSON: `{"packages":{"xpkg://pkg":"h1"}}`,
				runs:     []run{{gens: cr(map[string]string{"a.json": "A"}), src: src("h1")}},
			},
			want: want{
				lock: &lock{LockVersion: 1, Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h1", Languages: []string{"mock"}, Files: []lockFile{lf("mock/a.json", "A")}},
				}},
				files: map[string]string{"mock/a.json": "A"},
			},
		},
		"LanguageWithoutOutputIsRecorded": {
			reason: "A language that produces no files for a source is still recorded, so the source is up to date on the next Add.",
			args: args{runs: []run{
				{gens: []generator.Interface{&mockGenerator{lang: "mock", files: map[string]string{"a.json": "A"}}, &mockGenerator{lang: "empty"}}, src: src("h1")},
				{gens: []generator.Interface{&mockGenerator{lang: "mock", files: map[string]string{"a.json": "CHANGED"}}, &mockGenerator{lang: "empty"}}, src: src("h1")},
			}},
			want: want{
				lock: &lock{LockVersion: 1, Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h1", Languages: []string{"empty", "mock"}, Files: []lockFile{lf("mock/a.json", "A")}},
				}},
				files: map[string]string{"mock/a.json": "A"},
			},
		},
		"SourceWithoutOutputIsUpToDate": {
			reason: "A source that generates nothing is recorded once and not regenerated on every Add.",
			args: args{runs: []run{
				{gens: cr(nil), src: src("h1")},
				{gens: cr(nil), src: src("h1")},
			}},
			want: want{
				lock: &lock{LockVersion: 1, Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h1", Languages: []string{"mock"}, Files: []lockFile{}},
				}},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memFS := afero.NewMemMapFs()
			if tc.args.lockJSON != "" {
				if err := afero.WriteFile(memFS, lockFileName, []byte(tc.args.lockJSON), 0o600); err != nil {
					t.Fatal(err)
				}
			}

			for _, r := range tc.args.runs {
				if r.tamper != nil {
					r.tamper(t, memFS)
				}
				err := New(memFS, r.gens, nil, WithFlags(r.flags)).Add(t.Context(), r.src)
				if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
					t.Fatalf("%s\nAdd(...): -want err, +got err:\n%s", tc.reason, diff)
				}
			}

			gotLock, err := New(memFS, nil, nil).getLock()
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want.lock, gotLock, cmpopts.IgnoreFields(lockSource{}, "Generator")); diff != "" {
				t.Errorf("%s\nlock: -want, +got:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.files, readFiles(t, memFS)); diff != "" {
				t.Errorf("%s\nfiles: -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestManager_Reconcile(t *testing.T) {
	t.Parallel()

	type args struct {
		// added are the sources added before reconciling, each with one file.
		added map[string]string // source key -> file name
		live  []string
	}
	type want struct {
		result *ReconcileResult
		keys   []string
		files  map[string]string
		err    error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"AllLive": {
			reason: "Nothing is removed while every locked source is live.",
			args:   args{added: map[string]string{"a": "a.json", "b": "b.json"}, live: []string{"a", "b"}},
			want: want{
				result: &ReconcileResult{},
				keys:   []string{"a", "b"},
				files:  map[string]string{"mock/a.json": "a.json", "mock/b.json": "b.json"},
			},
		},
		"OrphanRemoved": {
			reason: "A locked source that is no longer live loses its lock entry and files.",
			args:   args{added: map[string]string{"a": "a.json", "b": "b.json"}, live: []string{"a"}},
			want: want{
				result: &ReconcileResult{Sources: []string{"b"}, Files: []string{"mock/b.json"}},
				keys:   []string{"a"},
				files:  map[string]string{"mock/a.json": "a.json"},
			},
		},
		"SharedFileKept": {
			reason: "A file that another live source also records must survive its orphaned co-owner.",
			args:   args{added: map[string]string{"a": "shared.json", "b": "shared.json"}, live: []string{"a"}},
			want: want{
				result: &ReconcileResult{Sources: []string{"b"}},
				keys:   []string{"a"},
				files:  map[string]string{"mock/shared.json": "shared.json"},
			},
		},
		"NoLiveSources": {
			reason: "With no live sources everything is removed.",
			args:   args{added: map[string]string{"a": "a.json"}},
			want: want{
				result: &ReconcileResult{Sources: []string{"a"}, Files: []string{"mock/a.json"}},
				keys:   []string{},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memFS := afero.NewMemMapFs()
			live := make([]Source, 0, len(tc.args.live))
			for key, file := range tc.args.added {
				m := New(memFS, []generator.Interface{&mockGenerator{lang: "mock", files: map[string]string{file: file}}}, nil)
				if err := m.Add(t.Context(), &mockSource{key: key, hash: "h"}); err != nil {
					t.Fatal(err)
				}
			}
			for _, key := range tc.args.live {
				live = append(live, &mockSource{key: key})
			}

			got, err := New(memFS, nil, nil).Reconcile(t.Context(), live)
			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("%s\nReconcile(...): -want err, +got err:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.result, got); diff != "" {
				t.Errorf("%s\nReconcile(...): -want, +got:\n%s", tc.reason, diff)
			}
			l, _ := New(memFS, nil, nil).getLock()
			if diff := cmp.Diff(tc.want.keys, sortedKeys(l.Sources)); diff != "" {
				t.Errorf("%s\nlocked sources: -want, +got:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.files, readFiles(t, memFS)); diff != "" {
				t.Errorf("%s\nfiles: -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestManager_Status(t *testing.T) {
	t.Parallel()

	type args struct {
		tamper func(t *testing.T, fs afero.Fs)
	}
	type want struct {
		statuses []SourceStatus
		err      error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"Clean": {
			reason: "Untouched files report no drift.",
			args:   args{tamper: func(*testing.T, afero.Fs) {}},
			want:   want{statuses: []SourceStatus{{Key: "k"}}},
		},
		"EditedAndMissing": {
			reason: "Edited and deleted files are named with their kind.",
			args: args{tamper: func(t *testing.T, fs afero.Fs) {
				t.Helper()
				_ = afero.WriteFile(fs, "mock/a.json", []byte("edited"), 0o600)
				_ = fs.Remove("mock/b.json")
			}},
			want: want{statuses: []SourceStatus{{Key: "k", Drift: []DriftedFile{
				{Path: "mock/a.json", Kind: DriftModified},
				{Path: "mock/b.json", Kind: DriftMissing},
			}}}},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memFS := afero.NewMemMapFs()
			m := New(memFS, []generator.Interface{&mockGenerator{lang: "mock", files: map[string]string{"a.json": "A", "b.json": "B"}}}, nil)
			if err := m.Add(t.Context(), &mockSource{key: "k", hash: "h"}); err != nil {
				t.Fatal(err)
			}
			tc.args.tamper(t, memFS)

			got, err := m.Status(t.Context())
			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("%s\nStatus(...): -want err, +got err:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.statuses, got); diff != "" {
				t.Errorf("%s\nStatus(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestManager_VerifyAndGenerate(t *testing.T) {
	t.Parallel()

	// The lock has a local source "fs://api" (mock/l.json, hash "h-api") and a
	// dependency "xpkg://dep" (mock/d.json, hash "h-dep", pinned), written for
	// dependencies "deps1".
	type args struct {
		// generate selects Generate (repairs modified files) over Verify.
		generate bool
		tamper   func(t *testing.T, fs afero.Fs)
		flags    Flags
		// extraLanguage adds a generator the lock was not written with.
		extraLanguage bool
		depsHash      string
		// localHash is the hash of the local source passed in. Empty means the
		// local source is not passed at all.
		localHash string
		// localKey is the key of the local source. It defaults to the locked one.
		localKey string
		// fetchHash is the hash of the source the fetcher returns.
		fetchHash string
	}
	type want struct {
		// err is the expected error; cmpopts.AnyError when only its presence
		// and message matter.
		err error
		// errMsg is a substring of the expected error message.
		errMsg string
		// fetched are the keys the fetcher was called with.
		fetched []string
		files   map[string]string
	}

	remove := func(path string) func(*testing.T, afero.Fs) {
		return func(t *testing.T, fs afero.Fs) {
			t.Helper()
			_ = fs.Remove(path)
		}
	}
	edit := func(path string) func(*testing.T, afero.Fs) {
		return func(t *testing.T, fs afero.Fs) {
			t.Helper()
			_ = afero.WriteFile(fs, path, []byte("edited"), 0o600)
		}
	}
	none := func(*testing.T, afero.Fs) {}
	clean := map[string]string{"mock/l.json": "L", "mock/d.json": "D"}
	with := func(path, content string) map[string]string {
		files := map[string]string{"mock/l.json": "L", "mock/d.json": "D"}
		if content == "" {
			delete(files, path)
		} else {
			files[path] = content
		}
		return files
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"VerifyCleanProject": {
			reason: "Nothing to do when disk and lock agree, and nothing is fetched.",
			args:   args{tamper: none, depsHash: "deps1", localHash: "h-api"},
			want:   want{files: clean},
		},
		"VerifyRecreatesMissingLocalFile": {
			reason: "A missing file of a local source is recreated from the local source, without fetching.",
			args:   args{tamper: remove("mock/l.json"), depsHash: "deps1", localHash: "h-api"},
			want:   want{files: clean},
		},
		"VerifyFetchesMissingDependencyFile": {
			reason: "A missing file of a dependency is recreated from the source fetched at its pin.",
			args:   args{tamper: remove("mock/d.json"), depsHash: "deps1", localHash: "h-api", fetchHash: "h-dep"},
			want:   want{fetched: []string{"xpkg://dep"}, files: clean},
		},
		"VerifyRejectsFetchedSourceWithOtherHash": {
			reason: "A source fetched at its pin whose content differs from the lock must not be used.",
			args:   args{tamper: remove("mock/d.json"), depsHash: "deps1", localHash: "h-api", fetchHash: "other"},
			want:   want{err: cmpopts.AnyError, errMsg: "does not match the schema lock", fetched: []string{"xpkg://dep"}, files: with("mock/d.json", "")},
		},
		"VerifyRejectsModifiedFile": {
			reason: "Build must not silently overwrite a hand-edited generated file, and needs no fetch to say so.",
			args:   args{tamper: edit("mock/d.json"), depsHash: "deps1", localHash: "h-api"},
			want:   want{err: cmpopts.AnyError, errMsg: "crossplane schema generate", files: with("mock/d.json", "edited")},
		},
		"VerifyRejectsChangedDependencies": {
			reason: "Other dependencies than the ones locked mean the lock is stale; this is known without the network.",
			args:   args{tamper: remove("mock/d.json"), depsHash: "deps2", localHash: "h-api"},
			want:   want{err: cmpopts.AnyError, errMsg: "dependencies changed", files: with("mock/d.json", "")},
		},
		"VerifyRejectsChangedFlags": {
			reason: "Sources generated with other flags need an update first.",
			args:   args{tamper: none, depsHash: "deps1", localHash: "h-api", flags: Flags{GoRuntimeObjects: true}},
			want:   want{err: cmpopts.AnyError, errMsg: "other flags or languages", files: clean},
		},
		"VerifyRejectsChangedAPIGroups": {
			reason: "Changing the API group filter needs an update first.",
			args:   args{tamper: none, depsHash: "deps1", localHash: "h-api", flags: Flags{APIGroups: []string{"a.*"}}},
			want:   want{err: cmpopts.AnyError, errMsg: "other flags or languages", files: clean},
		},
		"VerifyRejectsChangedLanguages": {
			reason: "A language the lock was not written with needs an update first.",
			args:   args{tamper: none, depsHash: "deps1", localHash: "h-api", extraLanguage: true},
			want:   want{err: cmpopts.AnyError, errMsg: "other flags or languages", files: clean},
		},
		"VerifyRejectsChangedLocalSource": {
			reason: "A local source hash that differs from the lock means the lock is stale.",
			args:   args{tamper: remove("mock/l.json"), depsHash: "deps1", localHash: "other"},
			want:   want{err: cmpopts.AnyError, errMsg: "source fs://api changed", files: with("mock/l.json", "")},
		},
		"VerifyRejectsUnlockedLocalSource": {
			reason: "A local source with no lock entry needs an update first.",
			args:   args{tamper: none, depsHash: "deps1", localHash: "h-new", localKey: "fs://new"},
			want:   want{err: cmpopts.AnyError, errMsg: "source fs://new is not in the schema lock", files: clean},
		},
		"GenerateRepairsModifiedLocalFile": {
			reason: "`schema generate` is the explicit repair for edited files.",
			args:   args{generate: true, tamper: edit("mock/l.json"), depsHash: "deps1", localHash: "h-api"},
			want:   want{files: clean},
		},
		"GenerateRepairsModifiedDependencyFile": {
			reason: "`schema generate` restores an edited dependency file from the source fetched at its pin.",
			args:   args{generate: true, tamper: edit("mock/d.json"), depsHash: "deps1", localHash: "h-api", fetchHash: "h-dep"},
			want:   want{fetched: []string{"xpkg://dep"}, files: clean},
		},
		"GenerateRejectsChangedDependencies": {
			reason: "Generate never re-resolves what the lock pinned; changed dependencies fail.",
			args:   args{generate: true, tamper: edit("mock/d.json"), depsHash: "deps2", localHash: "h-api"},
			want:   want{err: cmpopts.AnyError, errMsg: "dependencies changed", files: with("mock/d.json", "edited")},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memFS := afero.NewMemMapFs()
			for key, src := range map[string]*pinnedSource{
				"l": {mockSource: &mockSource{key: "fs://api", hash: "h-api"}},
				"d": {mockSource: &mockSource{key: "xpkg://dep", hash: "h-dep"}, pin: "dep@sha256:x", version: "v1"},
			} {
				file := key + ".json"
				m := New(memFS, []generator.Interface{&mockGenerator{lang: "mock", files: map[string]string{file: strings.ToUpper(key)}}}, nil)
				if err := m.Add(t.Context(), src); err != nil {
					t.Fatal(err)
				}
			}
			if err := New(memFS, nil, nil).SetDependenciesHash("deps1"); err != nil {
				t.Fatal(err)
			}
			lockBefore, _ := afero.ReadFile(memFS, lockFileName)
			tc.args.tamper(t, memFS)

			// The generator renders every file, but only the ones the lock records
			// for the source being restored are written.
			gens := []generator.Interface{&mockGenerator{lang: "mock", files: map[string]string{"l.json": "L", "d.json": "D"}}}
			if tc.args.extraLanguage {
				gens = append(gens, &mockGenerator{lang: "other", files: map[string]string{"a.txt": "T"}})
			}
			m := New(memFS, gens, nil, WithFlags(tc.args.flags))

			var local []Source
			if tc.args.localHash != "" {
				key := tc.args.localKey
				if key == "" {
					key = "fs://api"
				}
				local = append(local, &mockSource{key: key, hash: tc.args.localHash})
			}
			var fetched []string
			fetch := func(_ context.Context, key string, entry LockedSource) (Source, error) {
				fetched = append(fetched, key)
				if entry.Pin != "dep@sha256:x" || entry.Version != "v1" {
					t.Errorf("%s\nfetch(...): entry = %+v, want the locked pin and version", tc.reason, entry)
				}
				return &mockSource{key: key, hash: tc.args.fetchHash}, nil
			}

			var err error
			if tc.args.generate {
				err = m.Generate(t.Context(), local, tc.args.depsHash, fetch)
			} else {
				err = m.Verify(t.Context(), local, tc.args.depsHash, fetch)
			}

			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("%s\nVerify/Generate(...): -want err, +got err:\n%s", tc.reason, diff)
			}
			if err != nil && !strings.Contains(err.Error(), tc.want.errMsg) {
				t.Errorf("%s\nVerify/Generate(...): err = %q, want it to contain %q", tc.reason, err, tc.want.errMsg)
			}
			if diff := cmp.Diff(tc.want.fetched, fetched); diff != "" {
				t.Errorf("%s\nfetched: -want, +got:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.files, readFiles(t, memFS)); diff != "" {
				t.Errorf("%s\nfiles: -want, +got:\n%s", tc.reason, diff)
			}
			lockAfter, _ := afero.ReadFile(memFS, lockFileName)
			if diff := cmp.Diff(string(lockBefore), string(lockAfter)); diff != "" {
				t.Errorf("%s\nlock must not change: -before, +after:\n%s", tc.reason, diff)
			}
		})
	}
}

const localCRD = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.local.example.org
spec:
  group: local.example.org
`

// echoGenerator generates a file with the content of each input file, so tests
// can see what the manager handed to the generators.
type echoGenerator struct{}

func (echoGenerator) Language() string { return "echo" }

func (echoGenerator) GenerateFromCRD(_ context.Context, from afero.Fs, _ runner.SchemaRunner) (afero.Fs, error) {
	out := afero.NewMemMapFs()
	err := afero.Walk(from, ".", func(p string, info fs.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		content, err := afero.ReadFile(from, p)
		if err != nil {
			return err
		}
		return afero.WriteFile(out, p, content, 0o600)
	})
	return out, err
}

func (echoGenerator) GenerateFromOpenAPI(_ context.Context, _ afero.Fs, _ runner.SchemaRunner) (afero.Fs, error) {
	return nil, nil
}

func (echoGenerator) Fingerprint() string { return "echo" }

func TestManager_LocalSourcesIgnoreAPIGroups(t *testing.T) {
	t.Parallel()

	const depCRD = "kind: CustomResourceDefinition\nspec:\n  group: dep.example.org\n"
	local := func() Source {
		fs := afero.NewMemMapFs()
		_ = afero.WriteFile(fs, "widget.yaml", []byte(localCRD), 0o600)
		return NewFSSource("api", fs)
	}
	dep := func() Source {
		fs := afero.NewMemMapFs()
		_ = afero.WriteFile(fs, "dep.yaml", []byte(depCRD), 0o600)
		return &mockSource{key: "xpkg://dep", hash: "h-dep", fs: fs}
	}
	gens := []generator.Interface{echoGenerator{}}
	groups := func(g ...string) []Option { return []Option{WithFlags(Flags{APIGroups: g})} }

	localHash, _ := local().Hash(t.Context())
	localLock := lockSource{SourceHash: localHash, Languages: []string{"echo"}, Files: []lockFile{lf("echo/widget.yaml", localCRD)}}

	type args struct {
		// rounds are the API group globs of consecutive Add rounds. Each round
		// adds the local source and the dependency.
		rounds [][]string
		// verify makes the test call Verify with verifyGlobs after the last round.
		verify      bool
		verifyGlobs []string
	}
	type want struct {
		files map[string]string
		lock  map[string]lockSource
		err   error
	}

	// An unrelated glob filters the dependency's CRD away but not the local one.
	afterFirst := want{
		files: map[string]string{"echo/widget.yaml": localCRD},
		lock: map[string]lockSource{
			"fs://api":   localLock,
			"xpkg://dep": {SourceHash: "h-dep", Flags: Flags{APIGroups: []string{"other.*"}}, Languages: []string{"echo"}, Files: []lockFile{}},
		},
	}
	// Changing the globs regenerates the dependency but leaves the local source
	// alone.
	afterSecond := want{
		files: map[string]string{"echo/widget.yaml": localCRD, "echo/dep.yaml": depCRD},
		lock: map[string]lockSource{
			"fs://api":   localLock,
			"xpkg://dep": {SourceHash: "h-dep", Flags: Flags{APIGroups: []string{"dep.*"}}, Languages: []string{"echo"}, Files: []lockFile{lf("echo/dep.yaml", depCRD)}},
		},
	}
	withErr := func(w want, err error) want {
		w.err = err
		return w
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"LocalSourceIgnoresAPIGroups": {
			reason: "A local source must ignore apiGroups, a dependency must not.",
			args:   args{rounds: [][]string{{"other.*"}}},
			want:   afterFirst,
		},
		"ChangedAPIGroupsRegenerateOnlyDependency": {
			reason: "Changed apiGroups must regenerate only the dependency.",
			args:   args{rounds: [][]string{{"other.*"}, {"dep.*"}}},
			want:   afterSecond,
		},
		// Verify accepts the local source after its globs changed, but not the
		// dependency.
		"VerifyUnchangedGlobs": {
			reason: "The lock matches the globs.",
			args:   args{rounds: [][]string{{"other.*"}, {"dep.*"}}, verify: true, verifyGlobs: []string{"dep.*"}},
			want:   afterSecond,
		},
		"VerifyChangedGlobs": {
			reason: "Changed globs make the dependency stale.",
			args:   args{rounds: [][]string{{"other.*"}, {"dep.*"}}, verify: true, verifyGlobs: []string{"x.*"}},
			want:   withErr(afterSecond, cmpopts.AnyError),
		},
		"VerifyChangedGlobsLocals": {
			reason: "Changed globs make the dependency stale but never the local source.",
			args:   args{rounds: [][]string{{"other.*"}, {"dep.*"}}, verify: true, verifyGlobs: []string{"other.*"}},
			want:   withErr(afterSecond, cmpopts.AnyError),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memFS := afero.NewMemMapFs()
			for _, globs := range tc.args.rounds {
				m := New(memFS, gens, nil, groups(globs...)...)
				for _, s := range []Source{local(), dep()} {
					if err := m.Add(t.Context(), s); err != nil {
						t.Fatal(err)
					}
				}
			}

			var err error
			if tc.args.verify {
				if err := New(memFS, nil, nil).SetDependenciesHash("deps"); err != nil {
					t.Fatal(err)
				}
				err = New(memFS, gens, nil, groups(tc.args.verifyGlobs...)...).Verify(t.Context(), []Source{local()}, "deps", nil)
			}
			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("%s\nVerify(...): -want err, +got err:\n%s", tc.reason, diff)
			}
			if err != nil && strings.Contains(err.Error(), "fs://api") {
				t.Errorf("%s\nVerify(...): err = %v, must not mention the local source", tc.reason, err)
			}

			l, lerr := New(memFS, nil, nil).getLock()
			if lerr != nil {
				t.Fatal(lerr)
			}
			if diff := cmp.Diff(tc.want.files, readFiles(t, memFS)); diff != "" {
				t.Errorf("%s\nfiles: -want, +got:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.lock, l.Sources, cmpopts.IgnoreFields(lockSource{}, "Generator")); diff != "" {
				t.Errorf("%s\nlock sources: -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestManager_Clean(t *testing.T) {
	t.Parallel()

	type args struct{ add bool }
	type want struct {
		files map[string]string
		err   error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"RemovesSchemasAndLock": {
			reason: "Clean wipes generated files and the lock.",
			args:   args{add: true},
		},
		"EmptyDirectory": {
			reason: "Cleaning a project without schemas is not an error.",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memFS := afero.NewMemMapFs()
			m := New(memFS, []generator.Interface{&mockGenerator{lang: "mock", files: map[string]string{"a.json": "A"}}}, nil)
			if tc.args.add {
				if err := m.Add(t.Context(), &mockSource{key: "k", hash: "h"}); err != nil {
					t.Fatal(err)
				}
			}
			if diff := cmp.Diff(tc.want.err, m.Clean(), cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("%s\nClean(): -want err, +got err:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.files, readFiles(t, memFS)); diff != "" {
				t.Errorf("%s\nfiles: -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestUpdateLockIsIndented(t *testing.T) {
	type args struct {
		sources map[string]lockSource
	}
	type want struct {
		content string
		err     error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"OneEntryPerLine": {
			// See issue #168.
			reason: "The lock file must be written with one entry per line (indented) so it is human-readable and avoids spurious merge conflicts.",
			args: args{sources: map[string]lockSource{
				"xpkg://pkg.example/foo": {
					SourceHash: "sha256:aaa",
					Languages:  []string{"go"},
					Files:      []lockFile{{Path: "go/foo.go", SHA256: "bbb"}},
				},
			}},
			want: want{content: `{
  "lockVersion": 1,
  "sources": {
    "xpkg://pkg.example/foo": {
      "sourceHash": "sha256:aaa",
      "flags": {
        "goRuntimeObjects": false
      },
      "languages": [
        "go"
      ],
      "files": [
        {
          "path": "go/foo.go",
          "sha256": "bbb"
        }
      ]
    }
  }
}
`},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			m := New(fs, nil, nil)

			l := newLock()
			l.Sources = tc.args.sources

			err := m.updateLock(l)
			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("%s\nupdateLock(...): -want err, +got err:\n%s", tc.reason, diff)
			}

			got, err := afero.ReadFile(fs, lockFileName)
			if err != nil {
				t.Fatalf("read lock: %v", err)
			}
			if diff := cmp.Diff(tc.want.content, string(got)); diff != "" {
				t.Errorf("%s\nupdateLock(...): lock file: -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

// readFiles returns all files except the lock, keyed by path.
func readFiles(t *testing.T, f afero.Fs) map[string]string {
	t.Helper()
	var files map[string]string
	err := afero.Walk(f, ".", func(path string, info fs.FileInfo, err error) error {
		if err != nil || info.IsDir() || info.Name() == lockFileName || info.Name() == "index.schema.json" {
			return err
		}
		content, err := afero.ReadFile(f, path)
		if files == nil {
			files = map[string]string{}
		}
		files[path] = string(content)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

type mockGenerator struct {
	lang  string
	files map[string]string
}

func (g *mockGenerator) Language() string {
	return g.lang
}

func (g *mockGenerator) GenerateFromCRD(_ context.Context, _ afero.Fs, _ runner.SchemaRunner) (afero.Fs, error) {
	if len(g.files) == 0 {
		return nil, nil
	}
	fs := afero.NewMemMapFs()
	for path, contents := range g.files {
		if err := afero.WriteFile(fs, path, []byte(contents), 0o600); err != nil {
			return nil, err
		}
	}
	return fs, nil
}

func (g *mockGenerator) GenerateFromOpenAPI(_ context.Context, _ afero.Fs, _ runner.SchemaRunner) (afero.Fs, error) {
	return nil, nil
}

func (g *mockGenerator) Fingerprint() string {
	return "mock"
}

// mockRunGenerator is a run-level generator (see schemaMerger): it writes a
// root file describing every source it saw, and merges parts by
// concatenating their root files. Like the TypeScript generator, its merge
// walks the models/ directory, so it fails if a part reaches it in any layout
// other than the generator's own.
type mockRunGenerator struct {
	lang  string
	files map[string]string
}

func (g *mockRunGenerator) Language() string { return g.lang }

func (g *mockRunGenerator) GenerateFromCRD(_ context.Context, _ afero.Fs, _ runner.SchemaRunner) (afero.Fs, error) {
	if len(g.files) == 0 {
		return nil, nil
	}
	fs := afero.NewMemMapFs()
	for path, contents := range g.files {
		if err := afero.WriteFile(fs, path, []byte(contents), 0o600); err != nil {
			return nil, err
		}
	}
	return fs, nil
}

func (g *mockRunGenerator) GenerateFromOpenAPI(_ context.Context, _ afero.Fs, _ runner.SchemaRunner) (afero.Fs, error) {
	return nil, nil
}

func (g *mockRunGenerator) Fingerprint() string { return "mock-run" }

func (g *mockRunGenerator) MergeGeneratedSchemas(parts []afero.Fs) (afero.Fs, error) {
	out := afero.NewMemMapFs()
	for i, part := range parts {
		err := afero.Walk(part, "models", func(p string, info fs.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			content, err := afero.ReadFile(part, p)
			if err != nil {
				return err
			}
			// Concatenate: the merged root file names every part, unlike a
			// file-copy merge where the last part would silently win.
			prev, _ := afero.ReadFile(out, p)
			return afero.WriteFile(out, p, append(append(prev, []byte(fmt.Sprintf("part%d\n", i))...), content...), 0o600)
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

type mockSource struct {
	key  string
	hash string
	fs   afero.Fs
}

func (s *mockSource) Key() string {
	return s.key
}

func (s *mockSource) Hash(_ context.Context) (string, error) {
	return s.hash, nil
}

func (s *mockSource) Resources(_ context.Context) (afero.Fs, error) {
	return s.fs, nil
}

func (s *mockSource) Type() SourceType {
	return SourceTypeCRD
}

// pinnedSource is a mockSource that also implements Pinned.
type pinnedSource struct {
	*mockSource
	pin     string
	version string
}

func (s *pinnedSource) Pin(_ context.Context) (string, error) {
	return s.pin, nil
}

func (s *pinnedSource) Version() string {
	return s.version
}

func TestManager_AddRecordsPin(t *testing.T) {
	t.Parallel()

	base := func() *mockSource { return &mockSource{key: "xpkg://pkg", hash: "h1"} }

	type args struct {
		src Source
	}
	type want struct {
		locked map[string]LockedSource
		err    error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"PinnedSource": {
			reason: "The pin and version of a source that implements Pinned are recorded in the lock.",
			args:   args{src: &pinnedSource{mockSource: base(), pin: "pkg@sha256:abc", version: "v1"}},
			want:   want{locked: map[string]LockedSource{"xpkg://pkg": {Pin: "pkg@sha256:abc", Version: "v1", SourceHash: "h1"}}},
		},
		"UnpinnedSource": {
			reason: "A source that does not implement Pinned records neither.",
			args:   args{src: NewFSSource("apis", afero.NewMemMapFs())},
			want:   want{locked: map[string]LockedSource{"fs://apis": {SourceHash: mustFSHash(t)}}},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			m := New(afero.NewMemMapFs(), []generator.Interface{&mockGenerator{lang: "mock", files: map[string]string{"a.json": "A"}}}, nil)
			if diff := cmp.Diff(tc.want.err, m.Add(t.Context(), tc.args.src), cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("%s\nAdd(...): -want err, +got err:\n%s", tc.reason, diff)
			}
			got, err := m.Locked()
			if err != nil {
				t.Fatalf("%s\nLocked(): %v", tc.reason, err)
			}
			if diff := cmp.Diff(tc.want.locked, got); diff != "" {
				t.Errorf("%s\nLocked(): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

// mustFSHash returns the hash of an empty filesystem source.
func mustFSHash(t *testing.T) string {
	t.Helper()
	h, err := NewFSSource("apis", afero.NewMemMapFs()).Hash(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// echoSource returns a source whose echoGenerator output is the given files.
func echoSource(key, hash string, files map[string]string) *mockSource {
	fs := afero.NewMemMapFs()
	for p, content := range files {
		_ = afero.WriteFile(fs, p, []byte(content), 0o600)
	}
	return &mockSource{key: key, hash: hash, fs: fs}
}

func TestManager_AddConflictingSources(t *testing.T) {
	t.Parallel()

	// Each pass is one `schema update`: a new manager that Adds every live
	// source in order, then Reconciles.
	type pass struct {
		sources []*mockSource
		errMsg  string
	}
	cases := map[string]struct {
		reason string
		passes []pass
		want   map[string]string
	}{
		"IdenticalSharedFilesAreAllowed": {
			reason: "Two sources producing the same shared model byte-identically is normal, not a conflict.",
			passes: []pass{{sources: []*mockSource{
				echoSource("xpkg://a", "h1", map[string]string{"a.json": "A", "shared.json": "S"}),
				echoSource("xpkg://b", "h1", map[string]string{"b.json": "B", "shared.json": "S"}),
			}}},
			want: map[string]string{"echo/a.json": "A", "echo/b.json": "B", "echo/shared.json": "S"},
		},
		"ChangedSourceAfterSettledSourceIsRejected": {
			reason: "A changed source that disagrees with an already settled source must fail without overwriting its file.",
			passes: []pass{
				{sources: []*mockSource{echoSource("xpkg://a", "h1", map[string]string{"shared.json": "S"})}},
				{sources: []*mockSource{
					echoSource("xpkg://a", "h1", map[string]string{"shared.json": "S"}),
					echoSource("xpkg://b", "h1", map[string]string{"shared.json": "DIFFERENT"}),
				}, errMsg: "shared.json was also generated differently by xpkg://a"},
			},
			want: map[string]string{"echo/shared.json": "S"},
		},
		"ChangedSourceBeforeSettledSourceIsRejected": {
			reason: "The conflict must be found whichever order the sources are Added in.",
			passes: []pass{
				{sources: []*mockSource{echoSource("xpkg://a", "h1", map[string]string{"shared.json": "S"})}},
				{sources: []*mockSource{
					echoSource("xpkg://b", "h1", map[string]string{"shared.json": "DIFFERENT"}),
					echoSource("xpkg://a", "h1", map[string]string{"shared.json": "S"}),
				}, errMsg: "shared.json was also generated differently by xpkg://b"},
			},
		},
		"ReplacedSourceDoesNotConflict": {
			reason: "A removed source's lock entry must not block the source replacing it; it is reconciled away.",
			passes: []pass{
				{sources: []*mockSource{echoSource("xpkg://old", "h1", map[string]string{"s3.json": "OLD"})}},
				{sources: []*mockSource{echoSource("xpkg://new", "h1", map[string]string{"s3.json": "NEW"})}},
			},
			want: map[string]string{"echo/s3.json": "NEW"},
		},
		"SourcesBumpedTogetherDoNotConflict": {
			reason: "Two sources whose shared model changes in the same pass must not conflict with each other's stale entries.",
			passes: []pass{
				{sources: []*mockSource{
					echoSource("xpkg://a", "h1", map[string]string{"shared.json": "V1"}),
					echoSource("xpkg://b", "h1", map[string]string{"shared.json": "V1"}),
				}},
				{sources: []*mockSource{
					echoSource("xpkg://a", "h2", map[string]string{"shared.json": "V2"}),
					echoSource("xpkg://b", "h2", map[string]string{"shared.json": "V2"}),
				}},
			},
			want: map[string]string{"echo/shared.json": "V2"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memFS := afero.NewMemMapFs()
			for i, p := range tc.passes {
				m := New(memFS, []generator.Interface{echoGenerator{}}, nil)
				var err error
				live := make([]Source, 0, len(p.sources))
				for _, src := range p.sources {
					live = append(live, src)
					if err = m.Add(t.Context(), src); err != nil {
						break
					}
				}
				if err == nil {
					_, err = m.Reconcile(t.Context(), live)
				}
				if p.errMsg == "" {
					if err != nil {
						t.Fatalf("%s\npass %d: %v", tc.reason, i, err)
					}
					continue
				}
				if err == nil || !strings.Contains(err.Error(), p.errMsg) {
					t.Fatalf("%s\npass %d: want error containing %q, got %v", tc.reason, i, p.errMsg, err)
				}
			}
			if tc.want == nil {
				return
			}
			if diff := cmp.Diff(tc.want, readFiles(t, memFS)); diff != "" {
				t.Errorf("%s\nfiles: -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestManager_Finalize(t *testing.T) {
	t.Parallel()

	runGen := func(files map[string]string) *mockRunGenerator {
		return &mockRunGenerator{lang: "ts", files: files}
	}
	dep := func(hash string) *mockSource { return &mockSource{key: "xpkg://dep", hash: hash} }
	api := func(hash string) *mockSource { return &mockSource{key: "fs://apis", hash: hash} }

	t.Run("MergesAllSources", func(t *testing.T) {
		t.Parallel()

		memFS := afero.NewMemMapFs()
		gens := []generator.Interface{runGen(map[string]string{"models/index.txt": "S"})}
		sources := []Source{dep("h1"), api("h2")}

		m := New(memFS, gens, nil)
		for _, src := range sources {
			if err := m.Add(t.Context(), src); err != nil {
				t.Fatal(err)
			}
		}
		if err := m.Finalize(t.Context(), sources); err != nil {
			t.Fatal(err)
		}

		// The merged root file must describe both sources, not just the one
		// whose copy landed last.
		got, err := afero.ReadFile(memFS, "ts/index.txt")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(got), "part0") || !strings.Contains(string(got), "part1") {
			t.Errorf("merged root file lost a source's part; got %q", got)
		}

		l, err := New(memFS, nil, nil).getLock()
		if err != nil {
			t.Fatal(err)
		}
		entry, ok := l.Run["ts"]
		if !ok {
			t.Fatalf("no run entry for ts in lock %+v", l)
		}
		if diff := cmp.Diff(map[string]string{"xpkg://dep": "h1", "fs://apis": "h2"}, entry.Sources); diff != "" {
			t.Errorf("run entry sources: -want, +got:\n%s", diff)
		}
	})

	t.Run("FreshRunIsNoOp", func(t *testing.T) {
		t.Parallel()

		memFS := afero.NewMemMapFs()
		sources := []Source{dep("h1"), api("h2")}

		m := New(memFS, []generator.Interface{runGen(map[string]string{"models/index.txt": "MERGED"})}, nil)
		for _, src := range sources {
			if err := m.Add(t.Context(), src); err != nil {
				t.Fatal(err)
			}
		}
		if err := m.Finalize(t.Context(), sources); err != nil {
			t.Fatal(err)
		}
		before, err := afero.ReadFile(memFS, "ts/index.txt")
		if err != nil {
			t.Fatal(err)
		}

		// A second Finalize with a generator whose output differs must not
		// regenerate: the run entry is fresh for the same sources.
		m = New(memFS, []generator.Interface{runGen(map[string]string{"models/index.txt": "CHANGED"})}, nil)
		if err := m.Finalize(t.Context(), sources); err != nil {
			t.Fatal(err)
		}
		after, err := afero.ReadFile(memFS, "ts/index.txt")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, after) {
			t.Errorf("fresh run entry was regenerated: %q became %q", before, after)
		}
	})

	t.Run("ChangedSourceRegenerates", func(t *testing.T) {
		t.Parallel()

		memFS := afero.NewMemMapFs()
		gens := []generator.Interface{runGen(map[string]string{"models/index.txt": "S"})}

		m := New(memFS, gens, nil)
		if err := m.Add(t.Context(), dep("h1")); err != nil {
			t.Fatal(err)
		}
		if err := m.Finalize(t.Context(), []Source{dep("h1")}); err != nil {
			t.Fatal(err)
		}

		// The source set changes; the run entry is stale and regenerates.
		sources := []Source{dep("h1"), api("h2")}
		m = New(memFS, gens, nil)
		if err := m.Add(t.Context(), api("h2")); err != nil {
			t.Fatal(err)
		}
		if err := m.Finalize(t.Context(), sources); err != nil {
			t.Fatal(err)
		}

		l, err := New(memFS, nil, nil).getLock()
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(map[string]string{"xpkg://dep": "h1", "fs://apis": "h2"}, l.Run["ts"].Sources); diff != "" {
			t.Errorf("run entry did not regenerate: -want, +got:\n%s", diff)
		}
	})

	t.Run("RunFileDriftIsRepaired", func(t *testing.T) {
		t.Parallel()

		memFS := afero.NewMemMapFs()
		apiSrc := NewFSSource("apis", afero.NewMemMapFs())
		gens := []generator.Interface{runGen(map[string]string{"models/index.txt": "S"})}
		sources := []Source{apiSrc}

		m := New(memFS, gens, nil)
		if err := m.Add(t.Context(), apiSrc); err != nil {
			t.Fatal(err)
		}
		if err := m.Finalize(t.Context(), sources); err != nil {
			t.Fatal(err)
		}
		want, err := afero.ReadFile(memFS, "ts/index.txt")
		if err != nil {
			t.Fatal(err)
		}
		if err := memFS.Remove("ts/index.txt"); err != nil {
			t.Fatal(err)
		}

		// Verify recreates a missing run file from the lock, byte-identically,
		// offline: local sources are at hand and the run entry records them.
		m = New(memFS, gens, nil)
		if err := m.Verify(t.Context(), sources, "", nil); err != nil {
			t.Fatalf("Verify did not recreate the deleted run file: %v", err)
		}
		got, err := afero.ReadFile(memFS, "ts/index.txt")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("restored run file differs: want %q, got %q", want, got)
		}

		// A modified run file is reported by Verify and restored by Generate.
		if err := afero.WriteFile(memFS, "ts/index.txt", []byte("edited"), 0o644); err != nil {
			t.Fatal(err)
		}
		m = New(memFS, gens, nil)
		if err := m.Verify(t.Context(), sources, "", nil); err == nil {
			t.Fatal("Verify did not report the modified run file")
		}
		m = New(memFS, gens, nil)
		if err := m.Generate(t.Context(), sources, "", nil); err != nil {
			t.Fatal(err)
		}
		got, err = afero.ReadFile(memFS, "ts/index.txt")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("restored run file differs: want %q, got %q", want, got)
		}

		m = New(memFS, gens, nil)
		if err := m.Verify(t.Context(), sources, "", nil); err != nil {
			t.Errorf("Verify failed after repair: %v", err)
		}
	})

	t.Run("RunFileOfFetchedSourceIsRepaired", func(t *testing.T) {
		t.Parallel()

		// Real builds pass only the project's own APIs as local, so repairing
		// a run file fetches every dependency at its locked pin. This
		// deadlocked when the fetch path read the lock under lockMu again.
		memFS := afero.NewMemMapFs()
		gens := []generator.Interface{runGen(map[string]string{"models/index.txt": "S"})}
		depSrc, apiSrc := dep("h1"), api("h2")
		sources := []Source{depSrc, apiSrc}

		m := New(memFS, gens, nil)
		for _, src := range sources {
			if err := m.Add(t.Context(), src); err != nil {
				t.Fatal(err)
			}
		}
		if err := m.Finalize(t.Context(), sources); err != nil {
			t.Fatal(err)
		}
		want, err := afero.ReadFile(memFS, "ts/index.txt")
		if err != nil {
			t.Fatal(err)
		}
		if err := memFS.Remove("ts/index.txt"); err != nil {
			t.Fatal(err)
		}

		fetched := 0
		fetch := func(_ context.Context, key string, _ LockedSource) (Source, error) {
			fetched++
			if key != depSrc.Key() {
				return nil, fmt.Errorf("unexpected fetch of %s", key)
			}
			return depSrc, nil
		}
		done := make(chan error, 1)
		go func() { done <- New(memFS, gens, nil).Verify(t.Context(), []Source{apiSrc}, "", fetch) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Verify did not recreate the deleted run file: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Verify deadlocked repairing a run file of a fetched source")
		}
		if fetched != 1 {
			t.Errorf("fetched %d sources, want 1", fetched)
		}
		got, err := afero.ReadFile(memFS, "ts/index.txt")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("restored run file differs: want %q, got %q", want, got)
		}
	})

	t.Run("RemovedRunLanguageIsPruned", func(t *testing.T) {
		t.Parallel()

		memFS := afero.NewMemMapFs()
		perSource := &mockGenerator{lang: "mock", files: map[string]string{"a.json": "A"}}
		sources := []Source{api("h2")}

		m := New(memFS, []generator.Interface{perSource, runGen(map[string]string{"models/index.txt": "S"})}, nil)
		if err := m.Add(t.Context(), sources[0]); err != nil {
			t.Fatal(err)
		}
		if err := m.Finalize(t.Context(), sources); err != nil {
			t.Fatal(err)
		}

		// The run-level language is dropped from the project. Until the next
		// update Verify says so; the update removes the run entry and files.
		m = New(memFS, []generator.Interface{perSource}, nil)
		if err := m.Verify(t.Context(), sources, "", nil); err == nil || !strings.Contains(err.Error(), "not configured any more") {
			t.Fatalf("Verify before update: want error about the removed language, got %v", err)
		}
		if err := m.Add(t.Context(), sources[0]); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Reconcile(t.Context(), sources); err != nil {
			t.Fatal(err)
		}
		if err := m.Finalize(t.Context(), sources); err != nil {
			t.Fatal(err)
		}

		m = New(memFS, []generator.Interface{perSource}, nil)
		if err := m.Verify(t.Context(), sources, "", nil); err != nil {
			t.Errorf("Verify after update: %v", err)
		}
		if diff := cmp.Diff(map[string]string{"mock/a.json": "A"}, readFiles(t, memFS)); diff != "" {
			t.Errorf("files of the removed run-level language were left behind: -want, +got:\n%s", diff)
		}
		l, err := m.getLock()
		if err != nil {
			t.Fatal(err)
		}
		if l.Run != nil {
			t.Errorf("run entry of the removed language was kept: %+v", l.Run)
		}
	})

	t.Run("NoRunLevelGeneratorIsNoOp", func(t *testing.T) {
		t.Parallel()

		memFS := afero.NewMemMapFs()
		m := New(memFS, []generator.Interface{&mockGenerator{lang: "mock", files: map[string]string{"a.json": "A"}}}, nil)
		if err := m.Add(t.Context(), dep("h1")); err != nil {
			t.Fatal(err)
		}
		if err := m.Finalize(t.Context(), []Source{dep("h1")}); err != nil {
			t.Fatal(err)
		}

		l, err := New(memFS, nil, nil).getLock()
		if err != nil {
			t.Fatal(err)
		}
		if l.Run != nil {
			t.Errorf("run entries written without a run-level generator: %+v", l.Run)
		}
	})
}

// fingerprintGenerator is a mockGenerator with a configurable fingerprint that
// counts its runs.
type fingerprintGenerator struct {
	mockGenerator
	fingerprint string
	runs        *atomic.Int32
}

func (g *fingerprintGenerator) Fingerprint() string { return g.fingerprint }

func (g *fingerprintGenerator) GenerateFromCRD(ctx context.Context, from afero.Fs, r runner.SchemaRunner) (afero.Fs, error) {
	g.runs.Add(1)
	return g.mockGenerator.GenerateFromCRD(ctx, from, r)
}

func TestManager_FingerprintIsPerSource(t *testing.T) {
	t.Parallel()

	memFS := afero.NewMemMapFs()
	var runs atomic.Int32
	gen := func(fingerprint string) []generator.Interface {
		return []generator.Interface{&fingerprintGenerator{
			mockGenerator: mockGenerator{lang: "mock", files: map[string]string{"a.json": "A"}},
			fingerprint:   fingerprint,
			runs:          &runs,
		}}
	}
	a := &mockSource{key: "xpkg://a", hash: "h1"}
	b := &mockSource{key: "xpkg://b", hash: "h1"}

	m := New(memFS, gen("v1"), nil)
	for _, src := range []Source{a, b} {
		if err := m.Add(t.Context(), src); err != nil {
			t.Fatal(err)
		}
	}

	// The generator changes, and an update regenerates only a before it
	// fails. b's files were still made under the old fingerprint.
	m = New(memFS, gen("v2"), nil)
	if err := m.Add(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	// b's files still describe it, so builds pass, but b is reported stale.
	if err := New(memFS, gen("v2"), nil).Verify(t.Context(), nil, "", nil); err != nil {
		t.Errorf("Verify after a partial update: %v", err)
	}
	stale, err := New(memFS, gen("v2"), nil).StaleSources()
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]string{"xpkg://b"}, stale); diff != "" {
		t.Errorf("StaleSources after a partial update: -want, +got:\n%s", diff)
	}

	// The next update must regenerate b, not skip it as up to date.
	runs.Store(0)
	m = New(memFS, gen("v2"), nil)
	for _, src := range []Source{a, b} {
		if err := m.Add(t.Context(), src); err != nil {
			t.Fatal(err)
		}
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("update after a partial update ran the generator %d times, want 1 (for b only)", got)
	}
	stale, err = New(memFS, gen("v2"), nil).StaleSources()
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 0 {
		t.Errorf("StaleSources after the update: %v", stale)
	}
}

// echoRunGenerator is a run-level generator whose part for a source is the
// source's resources, so parts differ between sources. It counts its runs.
type echoRunGenerator struct {
	mockRunGenerator
	runs *atomic.Int32
}

func (g *echoRunGenerator) GenerateFromCRD(_ context.Context, from afero.Fs, _ runner.SchemaRunner) (afero.Fs, error) {
	g.runs.Add(1)
	var content []byte
	err := afero.Walk(from, ".", func(p string, info fs.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, err := afero.ReadFile(from, p)
		content = append(content, b...)
		return err
	})
	if err != nil {
		return nil, err
	}
	out := afero.NewMemMapFs()
	return out, afero.WriteFile(out, "models/index.txt", content, 0o600)
}

func TestManager_FingerprintScope(t *testing.T) {
	t.Parallel()

	t.Run("AddingRunLevelLanguageKeepsSourcesFresh", func(t *testing.T) {
		t.Parallel()

		memFS := afero.NewMemMapFs()
		var runs atomic.Int32
		perSource := &fingerprintGenerator{mockGenerator: mockGenerator{lang: "mock", files: map[string]string{"a.json": "A"}}, fingerprint: "p", runs: &runs}
		src := &mockSource{key: "xpkg://a", hash: "h1"}

		if err := New(memFS, []generator.Interface{perSource}, nil).Add(t.Context(), src); err != nil {
			t.Fatal(err)
		}
		runs.Store(0)
		m := New(memFS, []generator.Interface{perSource, &mockRunGenerator{lang: "ts", files: map[string]string{"models/index.txt": "S"}}}, nil)
		if err := m.Add(t.Context(), src); err != nil {
			t.Fatal(err)
		}
		if got := runs.Load(); got != 0 {
			t.Errorf("adding a run-level language regenerated a per-source entry %d times", got)
		}
	})

	t.Run("AddingLanguageKeepsRunFresh", func(t *testing.T) {
		t.Parallel()

		memFS := afero.NewMemMapFs()
		var runs atomic.Int32
		run := &echoRunGenerator{mockRunGenerator: mockRunGenerator{lang: "ts"}, runs: &runs}
		sources := []Source{echoSource("fs://apis", "h1", map[string]string{"x": "X"})}

		m := New(memFS, []generator.Interface{run}, nil)
		if err := m.Add(t.Context(), sources[0]); err != nil {
			t.Fatal(err)
		}
		if err := m.Finalize(t.Context(), sources); err != nil {
			t.Fatal(err)
		}
		runs.Store(0)
		m = New(memFS, []generator.Interface{run, &mockGenerator{lang: "mock", files: map[string]string{"a.json": "A"}}}, nil)
		if err := m.Add(t.Context(), sources[0]); err != nil {
			t.Fatal(err)
		}
		if err := m.Finalize(t.Context(), sources); err != nil {
			t.Fatal(err)
		}
		if got := runs.Load(); got != 0 {
			t.Errorf("adding a per-source language regenerated the run-level one %d times", got)
		}
	})
}

func TestManager_StaleGeneratorRepair(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		reason  string
		newGen  map[string]string
		wantErr string
	}{
		"SameOutputIsRepaired": {
			reason: "A file locked under an old fingerprint is repaired if the new generator still makes it byte-identically.",
			newGen: map[string]string{"a.json": "A"},
		},
		"ChangedOutputNamesTheCause": {
			reason:  "If the new generator makes the file differently, the error must say the CLI or generator changed.",
			newGen:  map[string]string{"a.json": "CHANGED"},
			wantErr: "generated by another CLI version or generator configuration",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memFS := afero.NewMemMapFs()
			var runs atomic.Int32
			src := &mockSource{key: "fs://apis", hash: "h1"}
			old := &fingerprintGenerator{mockGenerator: mockGenerator{lang: "mock", files: map[string]string{"a.json": "A"}}, fingerprint: "v1", runs: &runs}
			if err := New(memFS, []generator.Interface{old}, nil).Add(t.Context(), src); err != nil {
				t.Fatal(err)
			}
			if err := memFS.Remove("mock/a.json"); err != nil {
				t.Fatal(err)
			}

			upgraded := &fingerprintGenerator{mockGenerator: mockGenerator{lang: "mock", files: tc.newGen}, fingerprint: "v2", runs: &runs}
			err := New(memFS, []generator.Interface{upgraded}, nil).Verify(t.Context(), []Source{src}, "", nil)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("%s\nVerify: %v", tc.reason, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("%s\nVerify: want error containing %q, got %v", tc.reason, tc.wantErr, err)
			}
		})
	}
}

func TestManager_RunPartCache(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) (afero.Fs, afero.Fs, *atomic.Int32, *echoRunGenerator) {
		t.Helper()
		var runs atomic.Int32
		return afero.NewMemMapFs(), afero.NewMemMapFs(), &runs, &echoRunGenerator{mockRunGenerator: mockRunGenerator{lang: "ts"}, runs: &runs}
	}
	update := func(t *testing.T, m *Manager, sources []Source) {
		t.Helper()
		for _, src := range sources {
			if err := m.Add(t.Context(), src); err != nil {
				t.Fatal(err)
			}
		}
		if err := m.Finalize(t.Context(), sources); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("OnlyChangedSourcesAreRegenerated", func(t *testing.T) {
		t.Parallel()

		memFS, cacheFS, runs, gen := setup(t)
		dep := echoSource("xpkg://dep", "h1", map[string]string{"dep": "DEP;"})
		update(t, New(memFS, []generator.Interface{gen}, nil, WithPartCache(cacheFS)), []Source{dep, echoSource("fs://apis", "h1", map[string]string{"api": "API1;"})})
		if got := runs.Load(); got != 2 {
			t.Fatalf("cold run generated %d parts, want 2", got)
		}

		runs.Store(0)
		update(t, New(memFS, []generator.Interface{gen}, nil, WithPartCache(cacheFS)), []Source{dep, echoSource("fs://apis", "h2", map[string]string{"api": "API2;"})})
		if got := runs.Load(); got != 1 {
			t.Errorf("after one source changed, generated %d parts, want 1", got)
		}
		got, err := afero.ReadFile(memFS, "ts/index.txt")
		if err != nil {
			t.Fatal(err)
		}
		if want := "part0\nAPI2;part1\nDEP;"; string(got) != want {
			t.Errorf("merged run file: want %q, got %q", want, got)
		}
	})

	t.Run("RepairUsesCachedPartsOffline", func(t *testing.T) {
		t.Parallel()

		memFS, cacheFS, runs, gen := setup(t)
		api := echoSource("fs://apis", "h1", map[string]string{"api": "API;"})
		update(t, New(memFS, []generator.Interface{gen}, nil, WithPartCache(cacheFS)), []Source{echoSource("xpkg://dep", "h1", map[string]string{"dep": "DEP;"}), api})
		want, err := afero.ReadFile(memFS, "ts/index.txt")
		if err != nil {
			t.Fatal(err)
		}
		if err := memFS.Remove("ts/index.txt"); err != nil {
			t.Fatal(err)
		}

		runs.Store(0)
		fetch := func(_ context.Context, key string, _ LockedSource) (Source, error) {
			return nil, fmt.Errorf("fetched %s although its part is cached", key)
		}
		if err := New(memFS, []generator.Interface{gen}, nil, WithPartCache(cacheFS)).Verify(t.Context(), []Source{api}, "", fetch); err != nil {
			t.Fatalf("Verify: %v", err)
		}
		if got := runs.Load(); got != 0 {
			t.Errorf("repair generated %d parts, want 0", got)
		}
		got, err := afero.ReadFile(memFS, "ts/index.txt")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("restored run file differs: want %q, got %q", want, got)
		}
	})

	t.Run("MergeOrderIsIndependentOfSourceOrder", func(t *testing.T) {
		t.Parallel()

		// Finalize gets the live sources in resolution order, repair in key
		// order. Both must merge the same way, or repair can never recreate
		// the file exactly.
		memFS, _, _, gen := setup(t)
		dep := echoSource("xpkg://dep", "h1", map[string]string{"dep": "DEP;"})
		api := echoSource("fs://apis", "h1", map[string]string{"api": "API;"})
		update(t, New(memFS, []generator.Interface{gen}, nil), []Source{dep, api})
		if err := memFS.Remove("ts/index.txt"); err != nil {
			t.Fatal(err)
		}
		fetch := func(context.Context, string, LockedSource) (Source, error) { return dep, nil }
		if err := New(memFS, []generator.Interface{gen}, nil).Verify(t.Context(), []Source{api}, "", fetch); err != nil {
			t.Fatalf("Verify: %v", err)
		}
	})

	t.Run("CorruptEntryIsAMiss", func(t *testing.T) {
		t.Parallel()

		memFS, cacheFS, runs, gen := setup(t)
		sources := []Source{echoSource("fs://apis", "h1", map[string]string{"api": "API;"})}
		update(t, New(memFS, []generator.Interface{gen}, nil, WithPartCache(cacheFS)), sources)
		entries, err := afero.ReadDir(cacheFS, "ts")
		if err != nil || len(entries) != 1 {
			t.Fatalf("want one cache entry, got %v (%v)", entries, err)
		}
		if err := afero.WriteFile(cacheFS, "ts/"+entries[0].Name(), []byte("not a tarball"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := memFS.Remove("ts/index.txt"); err != nil {
			t.Fatal(err)
		}

		runs.Store(0)
		if err := New(memFS, []generator.Interface{gen}, nil, WithPartCache(cacheFS)).Verify(t.Context(), sources, "", nil); err != nil {
			t.Fatalf("Verify: %v", err)
		}
		if got := runs.Load(); got != 1 {
			t.Errorf("a corrupt entry must be regenerated; generated %d parts, want 1", got)
		}
	})
}

func TestManager_SetDependenciesHash(t *testing.T) {
	t.Parallel()

	type args struct {
		// src is added before the hash is set.
		src  Source
		hash string
	}
	type want struct {
		lock *lock
		err  error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"HashIsRecordedNextToSources": {
			reason: "Setting the dependencies hash keeps the locked sources and records the hash in the lock.",
			args: args{
				src:  &pinnedSource{mockSource: &mockSource{key: "xpkg://pkg", hash: "h1"}, pin: "pkg@sha256:abc", version: "v1"},
				hash: "deps",
			},
			want: want{lock: &lock{
				LockVersion:      1,
				DependenciesHash: "deps",
				Sources: map[string]lockSource{
					"xpkg://pkg": {SourceHash: "h1", Pin: "pkg@sha256:abc", Version: "v1", Languages: []string{"mock"}, Files: []lockFile{lf("mock/a.json", "A")}},
				},
			}},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memFS := afero.NewMemMapFs()
			gens := []generator.Interface{&mockGenerator{lang: "mock", files: map[string]string{"a.json": "A"}}}

			if err := New(memFS, gens, nil).Add(t.Context(), tc.args.src); err != nil {
				t.Fatal(err)
			}
			err := New(memFS, nil, nil).SetDependenciesHash(tc.args.hash)
			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("%s\nSetDependenciesHash(...): -want err, +got err:\n%s", tc.reason, diff)
			}

			got, err := New(memFS, nil, nil).getLock()
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want.lock, got, cmpopts.IgnoreFields(lockSource{}, "Generator")); diff != "" {
				t.Errorf("%s\nSetDependenciesHash(...): -want lock, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestManager_OldLockVersionIsIgnored(t *testing.T) {
	t.Parallel()

	type args struct {
		lockJSON string
	}
	type want struct {
		locked map[string]LockedSource
		err    error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"UnversionedLock": {
			reason: "A lock without a lockVersion should be ignored.",
			args:   args{lockJSON: `{"sources":{"xpkg://pkg":{"sourceHash":"h1","flags":{"goRuntimeObjects":false},"files":[]}}}`},
			want:   want{locked: map[string]LockedSource{}},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memFS := afero.NewMemMapFs()
			if err := afero.WriteFile(memFS, lockFileName, []byte(tc.args.lockJSON), 0o600); err != nil {
				t.Fatal(err)
			}

			got, err := New(memFS, nil, nil).Locked()
			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("%s\nLocked(): -want err, +got err:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.locked, got); diff != "" {
				t.Errorf("%s\nLocked(): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestFlagsFor(t *testing.T) {
	t.Parallel()

	type args struct {
		schemas          *devv1alpha1.ProjectSchemas
		goRuntimeObjects bool
	}
	type want struct {
		flags Flags
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"NilSchemas": {
			reason: "A project without a schemas section has no filter.",
			want:   want{flags: Flags{}},
		},
		"APIGroups": {
			reason: "The project's apiGroups become part of the flags recorded in the lock.",
			args:   args{schemas: &devv1alpha1.ProjectSchemas{APIGroups: []string{"*.m.upbound.io"}}, goRuntimeObjects: true},
			want:   want{flags: Flags{GoRuntimeObjects: true, APIGroups: []string{"*.m.upbound.io"}}},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if diff := cmp.Diff(tc.want.flags, FlagsFor(tc.args.schemas, tc.args.goRuntimeObjects)); diff != "" {
				t.Errorf("%s\nFlagsFor(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestManager_HasLock(t *testing.T) {
	t.Parallel()

	type args struct {
		lockJSON string
	}
	type want struct {
		has bool
		err error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"NoFile": {
			reason: "A project that never ran an update has no lock.",
		},
		"CurrentLock": {
			reason: "A lock in the current format is usable.",
			args:   args{lockJSON: `{"lockVersion":1,"sources":{}}`},
			want:   want{has: true},
		},
		"OldLock": {
			reason: "A lock from an earlier version is ignored, so it counts as no lock.",
			args:   args{lockJSON: `{"sources":{}}`},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memFS := afero.NewMemMapFs()
			if tc.args.lockJSON != "" {
				if err := afero.WriteFile(memFS, lockFileName, []byte(tc.args.lockJSON), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := New(memFS, nil, nil).HasLock()
			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("%s\nHasLock(): -want err, +got err:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.has, got); diff != "" {
				t.Errorf("%s\nHasLock(): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}

func TestManager_removeUnowned(t *testing.T) {
	t.Parallel()

	type args struct {
		readOnly bool
		exists   bool
	}
	type want struct {
		removed []string
		err     error
	}

	cases := map[string]struct {
		reason string
		args   args
		want   want
	}{
		"AlreadyGone": {
			reason: "A file that is already gone is not an error.",
		},
		"Removed": {
			reason: "An unowned file is removed and reported.",
			args:   args{exists: true},
			want:   want{removed: []string{"mock/a.json"}},
		},
		"CannotRemove": {
			reason: "Any other failure to remove a file is returned.",
			args:   args{exists: true, readOnly: true},
			want:   want{err: cmpopts.AnyError},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			memFS := afero.NewMemMapFs()
			if tc.args.exists {
				if err := afero.WriteFile(memFS, "mock/a.json", []byte("A"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			f := memFS
			if tc.args.readOnly {
				f = afero.NewReadOnlyFs(memFS)
			}

			got, err := New(f, nil, nil).removeUnowned(newLock(), []lockFile{lf("mock/a.json", "A")})
			if diff := cmp.Diff(tc.want.err, err, cmpopts.EquateErrors()); diff != "" {
				t.Fatalf("%s\nremoveUnowned(...): -want err, +got err:\n%s", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.removed, got); diff != "" {
				t.Errorf("%s\nremoveUnowned(...): -want, +got:\n%s", tc.reason, diff)
			}
		})
	}
}
