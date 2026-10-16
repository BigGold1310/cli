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

package diffserver

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	diffv1alpha1 "github.com/crossplane/upjet/v2/proto/diff/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	runtimexpkg "github.com/crossplane/crossplane-runtime/v2/pkg/xpkg"
	"github.com/crossplane/crossplane-runtime/v2/pkg/xpkg/parser"

	"github.com/crossplane/cli/v2/internal/docker"
)

const (
	pkgWithCapability = `---
apiVersion: meta.pkg.crossplane.io/v1
kind: Provider
metadata:
  name: provider-aws-ec2
spec:
  capabilities:
  - SafeStart
  - DiffServer
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: vpcs.ec2.aws.upbound.io
spec:
  group: ec2.aws.upbound.io
  names:
    kind: VPC
    plural: vpcs
  scope: Cluster
  versions:
  - name: v1beta1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: vpcs.ec2.aws.m.upbound.io
spec:
  group: ec2.aws.m.upbound.io
  names:
    kind: VPC
    plural: vpcs
  scope: Namespaced
  versions:
  - name: v1beta1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
`

	pkgWithoutCapability = `---
apiVersion: meta.pkg.crossplane.io/v1
kind: Provider
metadata:
  name: provider-aws-s3
spec:
  capabilities:
  - SafeStart
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: buckets.s3.aws.upbound.io
spec:
  group: s3.aws.upbound.io
  names:
    kind: Bucket
    plural: buckets
  scope: Cluster
  versions:
  - name: v1beta1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
`
)

func parsePackage(t *testing.T, yamlDoc, digest string) *runtimexpkg.Package {
	t.Helper()

	metaScheme, err := runtimexpkg.BuildMetaScheme()
	if err != nil {
		t.Fatalf("cannot build meta scheme: %v", err)
	}
	objScheme, err := runtimexpkg.BuildObjectScheme()
	if err != nil {
		t.Fatalf("cannot build object scheme: %v", err)
	}

	pkg, err := parser.New(metaScheme, objScheme).Parse(context.Background(), io.NopCloser(strings.NewReader(yamlDoc)))
	if err != nil {
		t.Fatalf("cannot parse test package: %v", err)
	}
	return &runtimexpkg.Package{Package: pkg, Digest: digest}
}

// fakePkgClient implements the runtime xpkg.Client interface with canned
// packages.
type fakePkgClient struct {
	pkgs map[string]*runtimexpkg.Package
}

func (f *fakePkgClient) Get(_ context.Context, ref string, _ ...runtimexpkg.GetOption) (*runtimexpkg.Package, error) {
	p, ok := f.pkgs[ref]
	if !ok {
		return nil, status.Error(codes.NotFound, "no such package")
	}
	return p, nil
}

func (f *fakePkgClient) ListVersions(_ context.Context, _ string, _ ...runtimexpkg.GetOption) ([]string, error) {
	return nil, nil
}

// fakeRuntime fakes container lifecycle, pointing every container at addr.
type fakeRuntime struct {
	mu       sync.Mutex
	addr     string
	existing map[string]fakeContainer // name -> container
	started  []string                 // names passed to StartContainer
	stopped  []string                 // ids passed to StopContainerByID
}

type fakeContainer struct {
	id      string
	running bool
}

func (f *fakeRuntime) StartContainer(_ context.Context, name, _ string, _ ...docker.StartContainerOption) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, name)
	return "new-" + name, nil
}

func (f *fakeRuntime) StopContainerByID(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, id)
	return nil
}

func (f *fakeRuntime) StartContainerByID(_ context.Context, _ string) error { return nil }

func (f *fakeRuntime) GetRunningContainerIDByName(_ context.Context, name string) (string, bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.existing[name]
	if !ok {
		return "", false, false, nil
	}
	return c.id, true, c.running, nil
}

func (f *fakeRuntime) GetHostPort(_ context.Context, _ string, _ int) (string, error) {
	return f.addr, nil
}

// readyServer serves a real PlanService on a loopback listener that answers
// every request with INVALID_ARGUMENT, which is all readiness needs.
type readyServer struct {
	diffv1alpha1.UnimplementedPlanServiceServer
}

func (readyServer) Plan(context.Context, *diffv1alpha1.PlanRequest) (*diffv1alpha1.PlanResponse, error) {
	return nil, status.Error(codes.InvalidArgument, "empty request")
}

func servePlanService(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}
	s := grpc.NewServer()
	diffv1alpha1.RegisterPlanServiceServer(s, readyServer{})
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func TestManagerStart(t *testing.T) {
	ctx := context.Background()
	log := logging.NewNopLogger()

	pkgs := &fakePkgClient{pkgs: map[string]*runtimexpkg.Package{
		"xpkg.example.org/provider-aws-ec2:v2.9.0": parsePackage(t, pkgWithCapability, "sha256:0123456789abcdef"),
		"xpkg.example.org/provider-aws-s3:v2.9.0":  parsePackage(t, pkgWithoutCapability, "sha256:fedcba9876543210"),
	}}

	t.Run("NoCapabilitySkipsWithoutStarting", func(t *testing.T) {
		rt := &fakeRuntime{}
		m := NewManager(pkgs, log, WithContainerRuntime(rt))

		skipped, err := m.Start(ctx, Images([]string{"xpkg.example.org/provider-aws-s3:v2.9.0"}))
		if err != nil {
			t.Fatalf("Start(...): unexpected error: %v", err)
		}
		if len(skipped) != 1 || !strings.Contains(skipped[0].Reason, "DiffServer capability") {
			t.Errorf("Start(...): want one capability skip, got %+v", skipped)
		}
		if len(rt.started) != 0 {
			t.Errorf("Start(...): no container should start for a package without the capability")
		}
	})

	t.Run("RoutesBuiltFromPackageCRDs", func(t *testing.T) {
		rt := &fakeRuntime{addr: servePlanService(t)}
		m := NewManager(pkgs, log, WithContainerRuntime(rt))

		skipped, err := m.Start(ctx, Images([]string{"xpkg.example.org/provider-aws-ec2:v2.9.0"}))
		if err != nil {
			t.Fatalf("Start(...): unexpected error: %v", err)
		}
		if len(skipped) != 0 {
			t.Fatalf("Start(...): unexpected skips: %+v", skipped)
		}

		for _, gk := range []schema.GroupKind{
			{Group: "ec2.aws.upbound.io", Kind: "VPC"},
			{Group: "ec2.aws.m.upbound.io", Kind: "VPC"},
		} {
			if _, ok := m.Route(gk); !ok {
				t.Errorf("Route(%s): want a route from the package CRDs", gk)
			}
		}
		if _, ok := m.Route(schema.GroupKind{Group: "s3.aws.upbound.io", Kind: "Bucket"}); ok {
			t.Errorf("Route(Bucket.s3): want no route for a kind the package does not ship")
		}

		wantName := containerPrefix + "0123456789ab"
		if len(rt.started) != 1 || rt.started[0] != wantName {
			t.Errorf("Start(...): want container %q started, got %v", wantName, rt.started)
		}

		if err := m.Stop(ctx); err != nil {
			t.Fatalf("Stop(...): unexpected error: %v", err)
		}
		if len(rt.stopped) != 1 {
			t.Errorf("Stop(...): want the container stopped, got %v", rt.stopped)
		}
	})

	t.Run("ReusesRunningContainerByName", func(t *testing.T) {
		rt := &fakeRuntime{
			addr: servePlanService(t),
			existing: map[string]fakeContainer{
				containerPrefix + "0123456789ab": {id: "existing-id", running: true},
			},
		}
		m := NewManager(pkgs, log, WithContainerRuntime(rt))

		skipped, err := m.Start(ctx, Images([]string{"xpkg.example.org/provider-aws-ec2:v2.9.0"}))
		if err != nil || len(skipped) != 0 {
			t.Fatalf("Start(...): unexpected error or skips: %v %+v", err, skipped)
		}
		if len(rt.started) != 0 {
			t.Errorf("Start(...): want the existing container reused, got new containers %v", rt.started)
		}
	})

	t.Run("OrphanCleanupLeavesContainersRunning", func(t *testing.T) {
		rt := &fakeRuntime{addr: servePlanService(t)}
		m := NewManager(pkgs, log, WithContainerRuntime(rt))

		if _, err := m.Start(ctx, []Input{{Image: "xpkg.example.org/provider-aws-ec2:v2.9.0", Cleanup: CleanupOrphan}}); err != nil {
			t.Fatalf("Start(...): unexpected error: %v", err)
		}
		if err := m.Stop(ctx); err != nil {
			t.Fatalf("Stop(...): unexpected error: %v", err)
		}
		if len(rt.stopped) != 0 {
			t.Errorf("Stop(...): want containers left running with the Orphan cleanup policy, got stops %v", rt.stopped)
		}
	})

	t.Run("NotReadyStopsContainerAndSkips", func(t *testing.T) {
		// A listener that accepts no gRPC: readiness never succeeds.
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("cannot listen: %v", err)
		}
		t.Cleanup(func() { _ = lis.Close() })

		rt := &fakeRuntime{addr: lis.Addr().String()}
		m := NewManager(pkgs, log, WithContainerRuntime(rt), WithReadyTimeout(700*time.Millisecond))

		skipped, err := m.Start(ctx, Images([]string{"xpkg.example.org/provider-aws-ec2:v2.9.0"}))
		if err != nil {
			t.Fatalf("Start(...): unexpected error: %v", err)
		}
		if len(skipped) != 1 || !strings.Contains(skipped[0].Reason, "ready") {
			t.Errorf("Start(...): want a readiness skip, got %+v", skipped)
		}
		if len(rt.stopped) != 1 {
			t.Errorf("Start(...): want the unready container cleaned up, got stops %v", rt.stopped)
		}
	})
}
