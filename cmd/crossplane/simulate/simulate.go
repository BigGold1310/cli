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

// Package simulate contains the resource simulate command: preview what
// applying managed resources would do, to the cluster and to the cloud
// behind it, by asking the providers' own diff servers.
package simulate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	diffv1alpha1 "github.com/crossplane/upjet/v2/proto/diff/v1alpha1"
	"github.com/spf13/afero"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"

	"github.com/crossplane/crossplane/apis/v2/pkg"
	pkgv1 "github.com/crossplane/crossplane/apis/v2/pkg/v1"

	"github.com/crossplane/cli/v2/cmd/crossplane/common/load"
	"github.com/crossplane/cli/v2/internal/diffserver"
	"github.com/crossplane/cli/v2/internal/docker"
	"github.com/crossplane/cli/v2/internal/kube"
	"github.com/crossplane/cli/v2/internal/plan"
	"github.com/crossplane/cli/v2/internal/terminal"
	clixpkg "github.com/crossplane/cli/v2/internal/xpkg"

	_ "embed"
)

//go:embed help/simulate.md
var helpDetail string

// Cmd previews what applying managed resources would do.
type Cmd struct {
	Filename string `help:"YAML file(s) or directory with managed resources, or '-' for stdin (comma-separated)." predictor:"yaml_file_or_directory" required:"" short:"f"`

	ProviderImages  []string      `help:"Override the provider images to run as diff servers. Defaults to the providers installed on the cluster." name:"provider-images"`
	KeepDiffServers bool          `help:"Leave diff server containers running for reuse by later runs."                                            name:"keep-diff-servers"`
	Context         string        `default:""                                                                                                      help:"Kubernetes context."                               name:"context"                                  predictor:"context"   short:"c"`
	CacheDir        string        `default:"~/.crossplane/cache"                                                                                   help:"Cache directory for downloaded provider packages." name:"cache-dir"                                predictor:"directory"`
	Timeout         time.Duration `default:"2m"                                                                                                    help:"Per-resource plan timeout."`
	Output          string        `default:"plan"                                                                                                  enum:"plan,json,yaml"                                    help:"Output format. One of: plan, json, yaml." name:"output"         short:"o"`

	Impersonation kube.ImpersonationFlags `embed:""`
}

// Help returns the help message for the simulate command.
func (c *Cmd) Help() string {
	return helpDetail
}

// AfterApply expands the cache directory.
func (c *Cmd) AfterApply() error {
	if strings.HasPrefix(c.CacheDir, "~/") {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return errors.Wrap(err, "cannot determine home directory for cache dir")
		}
		c.CacheDir = filepath.Join(homeDir, c.CacheDir[2:])
	}
	return nil
}

// Run runs the simulate command.
func (c *Cmd) Run(k *kong.Context, logger logging.Logger, sp terminal.SpinnerPrinter) error {
	ctx := context.Background()

	loader, err := load.NewLoader(c.Filename)
	if err != nil {
		return errors.Wrapf(err, "cannot load resources from %q", c.Filename)
	}
	resources, err := loader.Load()
	if err != nil {
		return errors.Wrapf(err, "cannot load resources from %q", c.Filename)
	}
	if len(resources) == 0 {
		return errors.Errorf("no resources found in %q", c.Filename)
	}

	if err := docker.Check(ctx); err != nil {
		return errors.Wrap(err, "cannot connect to Docker, which simulate needs to run provider diff servers")
	}

	kubeClient, rmapper, defaultNamespace, err := c.setupKubeClient(logger)
	if err != nil {
		return err
	}

	images := c.ProviderImages
	if len(images) == 0 {
		if images, err = installedProviderImages(ctx, kubeClient); err != nil {
			return errors.Wrap(err, "cannot discover providers installed on the cluster")
		}
	}
	if len(images) == 0 {
		_, _ = fmt.Fprintln(k.Stderr, "No providers found to run as diff servers; all results will be approximate.")
	}

	pkgClient, err := clixpkg.NewClient(clixpkg.NewRemoteFetcher(), clixpkg.WithCacheDir(afero.NewOsFs(), c.CacheDir))
	if err != nil {
		return errors.Wrap(err, "cannot create package client")
	}

	mgr := diffserver.NewManager(pkgClient, logger, diffserver.WithKeepServers(c.KeepDiffServers))
	var skipped []diffserver.Skipped
	if err := sp.WrapWithSuccessSpinner("Starting provider diff servers", func() error {
		var err error
		skipped, err = mgr.Start(ctx, images)
		return err
	}); err != nil {
		return errors.Wrap(err, "cannot start diff servers")
	}
	defer func() {
		if err := mgr.Stop(context.Background()); err != nil {
			_, _ = fmt.Fprintf(k.Stderr, "Warning: cannot clean up diff servers: %s\n", err)
		}
	}()
	for _, s := range skipped {
		_, _ = fmt.Fprintf(k.Stderr, "Not running a diff server for %s: %s\n", s.Image, s.Reason)
	}

	plans := make([]*plan.ResourcePlan, 0, len(resources))
	for _, desired := range resources {
		plans = append(plans, c.simulateOne(ctx, kubeClient, rmapper, defaultNamespace, mgr, desired))
	}

	return plan.Render(k.Stdout, c.Output, plans)
}

// router routes a resource's group and kind to a diff server client. It is
// the slice of diffserver.Manager the per-resource flow needs, extracted so
// tests can fake routing.
type router interface {
	Route(gk schema.GroupKind) (diffv1alpha1.PlanServiceClient, bool)
}

// simulateOne plans a single resource: fetch its live counterpart, route it
// to a diff server, and fall back to the approximate client-side diff when no
// server can plan it. Failures become per-resource errors so one resource
// never fails the whole run.
func (c *Cmd) simulateOne(ctx context.Context, kubeClient client.Client, rmapper meta.RESTMapper, defaultNamespace string, mgr router, desired *unstructured.Unstructured) *plan.ResourcePlan {
	if desired.GetName() == "" {
		p := plan.NewResourcePlan(desired)
		p.Error = "resource has no name"
		return p
	}

	live, err := fetchLive(ctx, kubeClient, rmapper, defaultNamespace, desired)
	if err != nil {
		p := plan.NewResourcePlan(desired)
		p.Error = fmt.Sprintf("cannot fetch live resource: %s", err)
		return p
	}

	gk := desired.GroupVersionKind().GroupKind()
	planClient, ok := mgr.Route(gk)
	if !ok {
		return plan.Fallback(desired, live, fmt.Sprintf("no diff server for %s", gk))
	}

	pctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	store := objectStore(ctx, kubeClient, rmapper, defaultNamespace, desired)

	p, err := plan.Plan(pctx, planClient, desired, live, store)
	if err != nil {
		if plan.IsNotSupported(err) {
			return plan.Fallback(desired, live, "the provider cannot compute a diff for this resource")
		}
		p := plan.NewResourcePlan(desired)
		p.Error = fmt.Sprintf("diff server cannot plan: %s", err)
		return p
	}
	return p
}

// fetchLive returns the live counterpart of the desired resource, or nil when
// it does not exist (including when its kind is not installed on the
// cluster), in which case the resource plans as a create.
func fetchLive(ctx context.Context, kubeClient client.Client, rmapper meta.RESTMapper, defaultNamespace string, desired *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	gvk := desired.GroupVersionKind()

	mapping, err := rmapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		// The kind is not served by the cluster: nothing can exist yet. The
		// diff server can still plan the resource as a create.
		return nil, nil //nolint:nilerr // Intentional: an unserved kind means no live counterpart, not a failure.
	}

	key := client.ObjectKey{Name: desired.GetName()}
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		key.Namespace = desired.GetNamespace()
		if key.Namespace == "" {
			key.Namespace = defaultNamespace
		}
	}

	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(gvk)
	if err := kubeClient.Get(ctx, key, live); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return live, nil
}

// objectStore gathers the cluster objects the diff server needs to resolve
// the desired resource: today the ProviderConfig it references. The diff
// server is offline and only sees what travels in the request, so anything it
// must resolve is fetched here, with the user's own kubeconfig and RBAC. The
// ProviderConfig's credentials Secret deliberately stays out: the server runs
// offline with placeholder credentials and never needs the real ones.
// Gathering is best effort: a missing object simply stays out of the store,
// and the server reports precisely what it could not resolve.
func objectStore(ctx context.Context, kubeClient client.Client, rmapper meta.RESTMapper, defaultNamespace string, desired *unstructured.Unstructured) []*unstructured.Unstructured {
	name, _, _ := unstructured.NestedString(desired.Object, "spec", "providerConfigRef", "name")
	if name == "" {
		name = "default"
	}
	kind, _, _ := unstructured.NestedString(desired.Object, "spec", "providerConfigRef", "kind")
	if kind == "" {
		kind = "ProviderConfig"
	}

	// The provider config lives in the provider family's group, one label
	// above the resource's group: ec2.aws.m.upbound.io -> aws.m.upbound.io.
	group := desired.GroupVersionKind().Group
	i := strings.Index(group, ".")
	if i < 0 {
		return nil
	}
	group = group[i+1:]

	mapping, err := rmapper.RESTMapping(schema.GroupKind{Group: group, Kind: kind})
	if err != nil {
		return nil
	}

	pc := &unstructured.Unstructured{}
	pc.SetGroupVersionKind(mapping.GroupVersionKind)
	key := client.ObjectKey{Name: name}
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		key.Namespace = desired.GetNamespace()
		if key.Namespace == "" {
			key.Namespace = defaultNamespace
		}
	}
	if err := kubeClient.Get(ctx, key, pc); err != nil {
		return nil
	}
	return []*unstructured.Unstructured{pc}
}

// installedProviderImages lists the package images of the providers installed
// on the cluster, preferring the resolved identifier over the possibly
// constraint-form spec.package.
func installedProviderImages(ctx context.Context, kubeClient client.Client) ([]string, error) {
	providers := &pkgv1.ProviderList{}
	if err := kubeClient.List(ctx, providers); err != nil {
		return nil, err
	}
	images := make([]string, 0, len(providers.Items))
	for _, p := range providers.Items {
		img := p.Status.CurrentIdentifier
		if img == "" {
			img = p.Spec.Package
		}
		if img != "" {
			images = append(images, img)
		}
	}
	return images, nil
}

func (c *Cmd) setupKubeClient(logger logging.Logger) (client.Client, meta.RESTMapper, string, error) {
	clientconfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: c.Context},
	)

	kubeconfig, err := clientconfig.ClientConfig()
	if err != nil {
		return nil, nil, "", errors.Wrap(err, "failed to get kubeconfig")
	}

	c.Impersonation.Apply(kubeconfig)

	if kubeconfig.QPS == 0 {
		kubeconfig.QPS = 20
	}
	if kubeconfig.Burst == 0 {
		kubeconfig.Burst = 30
	}

	logger.Debug("Found kubeconfig")

	cl, err := client.New(kubeconfig, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		return nil, nil, "", errors.Wrap(err, "cannot init kubeclient")
	}
	_ = pkg.AddToScheme(cl.Scheme())

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(kubeconfig)
	if err != nil {
		return nil, nil, "", errors.Wrap(err, "cannot get discovery client")
	}
	d := memory.NewMemCacheClient(discoveryClient)
	rmapper := restmapper.NewShortcutExpander(restmapper.NewDeferredDiscoveryRESTMapper(d), d, nil)

	namespace, _, err := clientconfig.Namespace()
	if err != nil {
		return nil, nil, "", errors.Wrap(err, "failed to get namespace from kubeconfig")
	}

	return cl, rmapper, namespace, nil
}
