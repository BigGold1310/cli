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

package simulate

import (
	"context"
	"testing"
	"time"

	diffv1alpha1 "github.com/crossplane/upjet/v2/proto/diff/v1alpha1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/crossplane/cli/v2/internal/plan"
)

var vpcGVK = schema.GroupVersionKind{Group: "ec2.aws.upbound.io", Version: "v1beta1", Kind: "VPC"}

func vpc(name, cidr string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "ec2.aws.upbound.io/v1beta1",
		"kind":       "VPC",
		"metadata":   map[string]any{"name": name},
		"spec":       map[string]any{"forProvider": map[string]any{"cidrBlock": cidr}},
	}}
}

// fakeRouter routes every resource to the given client, or nowhere.
type fakeRouter struct {
	client diffv1alpha1.PlanServiceClient
}

func (f *fakeRouter) Route(schema.GroupKind) (diffv1alpha1.PlanServiceClient, bool) {
	if f.client == nil {
		return nil, false
	}
	return f.client, true
}

// fakePlanClient answers Plan in-process.
type fakePlanClient struct {
	rsp *diffv1alpha1.PlanResponse
	err error
	got *diffv1alpha1.PlanRequest
}

func (f *fakePlanClient) Plan(_ context.Context, in *diffv1alpha1.PlanRequest, _ ...grpc.CallOption) (*diffv1alpha1.PlanResponse, error) {
	f.got = in
	return f.rsp, f.err
}

func notSupportedErr(t *testing.T) error {
	t.Helper()
	st, err := status.New(codes.FailedPrecondition, "cannot compute diff").WithDetails(&errdetails.PreconditionFailure{
		Violations: []*errdetails.PreconditionFailure_Violation{{Type: "DIFF_COMPUTATION_NOT_SUPPORTED"}},
	})
	if err != nil {
		t.Fatalf("cannot build status: %v", err)
	}
	return st.Err()
}

func TestSimulateOne(t *testing.T) {
	ctx := context.Background()

	s := runtime.NewScheme()
	s.AddKnownTypeWithName(vpcGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(vpcGVK.GroupVersion().WithKind("VPCList"), &unstructured.UnstructuredList{})

	rm := meta.NewDefaultRESTMapper([]schema.GroupVersion{vpcGVK.GroupVersion()})
	rm.Add(vpcGVK, meta.RESTScopeRoot)

	cmd := &Cmd{Timeout: time.Second}

	t.Run("RoutedUpdate", func(t *testing.T) {
		live := vpc("my-vpc", "10.0.0.0/16")
		kube := fake.NewClientBuilder().WithScheme(s).WithObjects(live).Build()
		pc := &fakePlanClient{rsp: &diffv1alpha1.PlanResponse{Action: diffv1alpha1.Action_ACTION_UPDATE}}

		got := cmd.simulateOne(ctx, kube, rm, "default", &fakeRouter{client: pc}, vpc("my-vpc", "10.1.0.0/16"))

		if got.Action != plan.ActionUpdate || got.Approximate || got.Error != "" {
			t.Errorf("simulateOne(...): want a provider-computed update, got %+v", got)
		}
		if pc.got.GetActualResource() == nil {
			t.Errorf("simulateOne(...): want the live resource sent as actual state")
		}
	})

	t.Run("NoLiveCounterpartPlansCreate", func(t *testing.T) {
		kube := fake.NewClientBuilder().WithScheme(s).Build()
		pc := &fakePlanClient{rsp: &diffv1alpha1.PlanResponse{Action: diffv1alpha1.Action_ACTION_CREATE}}

		got := cmd.simulateOne(ctx, kube, rm, "default", &fakeRouter{client: pc}, vpc("new-vpc", "10.0.0.0/16"))

		if got.Action != plan.ActionCreate || got.Error != "" {
			t.Errorf("simulateOne(...): want a create, got %+v", got)
		}
		if pc.got.GetActualResource() != nil {
			t.Errorf("simulateOne(...): want no actual state for a missing live resource")
		}
	})

	t.Run("NoRouteFallsBack", func(t *testing.T) {
		live := vpc("my-vpc", "10.0.0.0/16")
		kube := fake.NewClientBuilder().WithScheme(s).WithObjects(live).Build()

		got := cmd.simulateOne(ctx, kube, rm, "default", &fakeRouter{}, vpc("my-vpc", "10.1.0.0/16"))

		if !got.Approximate || got.Action != plan.ActionUpdate {
			t.Errorf("simulateOne(...): want an approximate update fallback, got %+v", got)
		}
	})

	t.Run("NotSupportedFallsBack", func(t *testing.T) {
		live := vpc("my-vpc", "10.0.0.0/16")
		kube := fake.NewClientBuilder().WithScheme(s).WithObjects(live).Build()
		pc := &fakePlanClient{err: notSupportedErr(t)}

		got := cmd.simulateOne(ctx, kube, rm, "default", &fakeRouter{client: pc}, vpc("my-vpc", "10.1.0.0/16"))

		if !got.Approximate || got.Action != plan.ActionUpdate {
			t.Errorf("simulateOne(...): want an approximate fallback on DIFF_COMPUTATION_NOT_SUPPORTED, got %+v", got)
		}
	})

	t.Run("ServerErrorBecomesResourceError", func(t *testing.T) {
		kube := fake.NewClientBuilder().WithScheme(s).Build()
		pc := &fakePlanClient{err: status.Error(codes.Internal, "boom")}

		got := cmd.simulateOne(ctx, kube, rm, "default", &fakeRouter{client: pc}, vpc("my-vpc", "10.1.0.0/16"))

		if got.Error == "" {
			t.Errorf("simulateOne(...): want a per-resource error, got %+v", got)
		}
	})

	t.Run("SeedsProviderConfigIntoObjectStore", func(t *testing.T) {
		pcGVK := schema.GroupVersionKind{Group: "aws.upbound.io", Version: "v1beta1", Kind: "ProviderConfig"}
		secretGVK := schema.GroupVersionKind{Version: "v1", Kind: "Secret"}

		seedScheme := runtime.NewScheme()
		seedScheme.AddKnownTypeWithName(vpcGVK, &unstructured.Unstructured{})
		seedScheme.AddKnownTypeWithName(vpcGVK.GroupVersion().WithKind("VPCList"), &unstructured.UnstructuredList{})
		seedScheme.AddKnownTypeWithName(pcGVK, &unstructured.Unstructured{})
		seedScheme.AddKnownTypeWithName(pcGVK.GroupVersion().WithKind("ProviderConfigList"), &unstructured.UnstructuredList{})
		seedScheme.AddKnownTypeWithName(secretGVK, &unstructured.Unstructured{})
		seedScheme.AddKnownTypeWithName(secretGVK.GroupVersion().WithKind("SecretList"), &unstructured.UnstructuredList{})

		seedRM := meta.NewDefaultRESTMapper([]schema.GroupVersion{vpcGVK.GroupVersion(), pcGVK.GroupVersion()})
		seedRM.Add(vpcGVK, meta.RESTScopeRoot)
		seedRM.Add(pcGVK, meta.RESTScopeRoot)

		pc := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "aws.upbound.io/v1beta1",
			"kind":       "ProviderConfig",
			"metadata":   map[string]any{"name": "default"},
			"spec": map[string]any{"credentials": map[string]any{
				"source":    "Secret",
				"secretRef": map[string]any{"name": "aws-creds", "namespace": "crossplane-system", "key": "creds"},
			}},
		}}
		secret := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata":   map[string]any{"name": "aws-creds", "namespace": "crossplane-system"},
		}}

		kube := fake.NewClientBuilder().WithScheme(seedScheme).WithObjects(pc, secret).Build()
		pc2 := &fakePlanClient{rsp: &diffv1alpha1.PlanResponse{Action: diffv1alpha1.Action_ACTION_CREATE}}

		got := cmd.simulateOne(ctx, kube, seedRM, "default", &fakeRouter{client: pc2}, vpc("my-vpc", "10.0.0.0/16"))

		if got.Error != "" {
			t.Fatalf("simulateOne(...): unexpected error: %s", got.Error)
		}
		if n := len(pc2.got.GetKubernetesObjectStore()); n != 1 {
			t.Errorf("simulateOne(...): want the ProviderConfig in the object store, got %d objects", n)
		}
		if k := pc2.got.GetKubernetesObjectStore()[0].GetFields()["kind"].GetStringValue(); k != "ProviderConfig" {
			t.Errorf("simulateOne(...): want a ProviderConfig in the object store, got %q", k)
		}
	})

	t.Run("UnservedKindPlansCreate", func(t *testing.T) {
		// A kind the cluster does not serve: no RESTMapping, no live fetch.
		kube := fake.NewClientBuilder().WithScheme(s).Build()
		emptyRM := meta.NewDefaultRESTMapper(nil)
		pc := &fakePlanClient{rsp: &diffv1alpha1.PlanResponse{Action: diffv1alpha1.Action_ACTION_CREATE}}

		got := cmd.simulateOne(ctx, kube, emptyRM, "default", &fakeRouter{client: pc}, vpc("my-vpc", "10.0.0.0/16"))

		if got.Action != plan.ActionCreate || got.Error != "" {
			t.Errorf("simulateOne(...): want a create for an unserved kind, got %+v", got)
		}
	})
}
