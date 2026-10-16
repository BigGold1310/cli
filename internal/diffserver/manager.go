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

// Package diffserver runs provider packages as local diff server containers
// and routes resources to them by group and kind, the way render runs
// function packages. Whether a package can serve diffs is declared in its
// metadata (the DiffServer capability), and which resources it serves comes
// from the CRDs it ships, so everything is known before a container starts.
package diffserver

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	diffv1alpha1 "github.com/crossplane/upjet/v2/proto/diff/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	runtimexpkg "github.com/crossplane/crossplane-runtime/v2/pkg/xpkg"

	pkgmetav1 "github.com/crossplane/crossplane/apis/v2/pkg/meta/v1"

	"github.com/crossplane/cli/v2/internal/docker"
	clixpkg "github.com/crossplane/cli/v2/internal/xpkg"
)

const (
	// CapabilityDiffServer is the package capability a provider declares when
	// its image serves the diff protocol via `internal diff-server`.
	CapabilityDiffServer = "DiffServer"

	// containerPort is the port the diff server binds inside the container,
	// upjet's default for `internal diff-server --address`.
	containerPort = 9099

	// containerPrefix names diff server containers. The full name carries the
	// package digest, so a changed image never matches a stale container and
	// leftovers from --keep-diff-servers are easy to find.
	containerPrefix = "crossplane-diff-server-"

	readyPollInterval   = 500 * time.Millisecond
	defaultReadyTimeout = 60 * time.Second
)

// Server is a running diff server for one provider package.
type Server struct {
	Image       string
	Address     string
	ContainerID string
	GroupKinds  []schema.GroupKind

	conn   *grpc.ClientConn
	client diffv1alpha1.PlanServiceClient
	reused bool
}

// Skipped records a provider image that does not run a diff server this run,
// and why. Resources it would have served fall back to the approximate
// client-side diff.
type Skipped struct {
	Image  string
	Reason string
}

// Input describes one provider package to run as a diff server. The zero
// values of PublishAddress and Target mean the manager's defaults apply; set
// them to override Docker addressing for this provider only, the way render's
// per-function runtime annotations do.
type Input struct {
	Image          string
	PublishAddress netip.Addr
	Target         string
}

// containerRuntime is the slice of internal/docker the manager uses,
// extracted so tests can fake container lifecycle.
type containerRuntime interface {
	StartContainer(ctx context.Context, name, image string, opts ...docker.StartContainerOption) (string, error)
	StartContainerByID(ctx context.Context, id string) error
	StopContainerByID(ctx context.Context, id string) error
	GetRunningContainerIDByName(ctx context.Context, name string) (id string, found, running bool, err error)
	GetHostPort(ctx context.Context, id string, port int) (string, error)
}

// Manager starts diff server containers for provider packages and routes
// resources to them.
type Manager struct {
	pkgs         runtimexpkg.Client
	rt           containerRuntime
	log          logging.Logger
	keep         bool
	readyTimeout time.Duration
	publishAddr  netip.Addr

	mu      sync.Mutex
	routes  map[schema.GroupKind]*Server
	servers []*Server
}

// Option configures a Manager.
type Option func(*Manager)

// WithKeepServers leaves diff server containers running when the manager
// stops, so later runs find and reuse them by name.
func WithKeepServers(keep bool) Option {
	return func(m *Manager) { m.keep = keep }
}

// WithContainerRuntime replaces the container runtime, for testing.
func WithContainerRuntime(rt containerRuntime) Option {
	return func(m *Manager) { m.rt = rt }
}

// WithReadyTimeout sets how long to wait for a started diff server to answer.
func WithReadyTimeout(d time.Duration) Option {
	return func(m *Manager) { m.readyTimeout = d }
}

// NewManager returns a Manager that fetches provider packages with pkgs and
// runs their images as local diff server containers.
func NewManager(pkgs runtimexpkg.Client, log logging.Logger, opts ...Option) *Manager {
	m := &Manager{
		pkgs:         pkgs,
		rt:           dockerRuntime{},
		log:          log,
		readyTimeout: defaultReadyTimeout,
		publishAddr:  netip.MustParseAddr("127.0.0.1"),
		routes:       map[schema.GroupKind]*Server{},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Start inspects each provider image and runs a diff server container for
// every package that declares the DiffServer capability, in parallel and best
// effort: an image that cannot serve (no capability, fetch or startup
// failure) is returned as skipped rather than failing the run, since its
// resources degrade to the approximate client-side diff.
func (m *Manager) Start(ctx context.Context, inputs []Input) ([]Skipped, error) {
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		skipped []Skipped
	)
	for _, in := range inputs {
		wg.Add(1)
		go func(in Input) {
			defer wg.Done()
			s, skip := m.startOne(ctx, in)
			mu.Lock()
			defer mu.Unlock()
			if skip != nil {
				skipped = append(skipped, *skip)
				return
			}
			m.register(s)
		}(in)
	}
	wg.Wait()
	return skipped, ctx.Err()
}

// Images wraps plain image references into Inputs with no per-provider
// overrides.
func Images(images []string) []Input {
	ins := make([]Input, 0, len(images))
	for _, img := range images {
		ins = append(ins, Input{Image: img})
	}
	return ins
}

func (m *Manager) register(s *Server) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.servers = append(m.servers, s)
	for _, gk := range s.GroupKinds {
		if other, ok := m.routes[gk]; ok {
			m.log.Info("Multiple diff servers serve the same kind; keeping the first",
				"groupKind", gk.String(), "kept", other.Image, "ignored", s.Image)
			continue
		}
		m.routes[gk] = s
	}
}

func (m *Manager) startOne(ctx context.Context, in Input) (*Server, *Skipped) {
	image := in.Image

	pkg, err := m.pkgs.Get(ctx, image)
	if err != nil {
		return nil, &Skipped{Image: image, Reason: fmt.Sprintf("cannot fetch package: %s", err)}
	}

	meta := pkg.GetMeta()
	if meta == nil || !pkgmetav1.CapabilitiesContainFuzzyMatch(meta.GetCapabilities(), CapabilityDiffServer) {
		return nil, &Skipped{Image: image, Reason: "package does not declare the DiffServer capability"}
	}

	gks := clixpkg.GroupKinds(pkg.Package)
	if len(gks) == 0 {
		return nil, &Skipped{Image: image, Reason: "package contains no CRDs to route"}
	}

	// Run the container from the reference the package client resolved, so
	// ImageConfig path rewriting applies to the container image exactly as it
	// applied to the package fetch.
	runImage := pkg.ResolvedRef()
	if pkg.ResolvedSource == "" {
		runImage = image
	}

	// The manager's loopback default is overridable per provider: Input
	// carries the publish and target addresses from the provider manifest's
	// runtime annotations, the way render's per-function annotations work.
	publishAddr := m.publishAddr
	if in.PublishAddress.IsValid() {
		publishAddr = in.PublishAddress
	}

	id, reused, err := m.ensureContainer(ctx, containerName(pkg.Digest), runImage, publishAddr)
	if err != nil {
		return nil, &Skipped{Image: image, Reason: fmt.Sprintf("cannot start diff server container: %s", err)}
	}

	server, err := m.connect(ctx, id, image, in.Target)
	if err != nil {
		if !reused && !m.keep {
			if serr := m.rt.StopContainerByID(ctx, id); serr != nil {
				m.log.Info("Cannot clean up failed diff server container", "container", id, "err", serr)
			}
		}
		return nil, &Skipped{Image: image, Reason: err.Error()}
	}

	server.GroupKinds = gks
	server.reused = reused
	m.log.Debug("Diff server ready", "image", image, "address", server.Address, "reused", reused)
	return server, nil
}

// ensureContainer finds a live container with the deterministic name for this
// package digest and reuses it, or starts a new one.
func (m *Manager) ensureContainer(ctx context.Context, name, image string, publishAddr netip.Addr) (id string, reused bool, err error) {
	id, found, running, err := m.rt.GetRunningContainerIDByName(ctx, name)
	if err != nil {
		return "", false, err
	}
	if found {
		if !running {
			if err := m.rt.StartContainerByID(ctx, id); err != nil {
				return "", false, err
			}
		}
		return id, true, nil
	}

	id, err = m.rt.StartContainer(ctx, name, image,
		// --debug precedes the subcommand: it is the provider's global flag.
		docker.StartWithCommand([]string{"--debug", "internal", "diff-server", "--address", fmt.Sprintf(":%d", containerPort)}),
		docker.StartWithPublishedPort(publishAddr, containerPort),
	)
	return id, false, err
}

func (m *Manager) connect(ctx context.Context, containerID, image, target string) (*Server, error) {
	addr, err := m.rt.GetHostPort(ctx, containerID, containerPort)
	if err != nil {
		return nil, errors.Wrap(err, "cannot determine diff server address")
	}
	if target != "" {
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot parse diff server address %q", addr)
		}
		addr = net.JoinHostPort(target, port)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Wrap(err, "cannot create gRPC client")
	}

	client := diffv1alpha1.NewPlanServiceClient(conn)
	if err := waitReady(ctx, client, m.readyTimeout); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return &Server{
		Image:       image,
		Address:     addr,
		ContainerID: containerID,
		conn:        conn,
		client:      client,
	}, nil
}

// waitReady polls the server with an empty Plan request until it answers.
// Docker publishes the host port before the server inside the container binds
// its own, so a successful TCP connect proves nothing; any gRPC status from
// the server itself (an empty request draws INVALID_ARGUMENT) means it is
// serving.
func waitReady(ctx context.Context, c diffv1alpha1.PlanServiceClient, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var last error
	for {
		_, err := c.Plan(ctx, &diffv1alpha1.PlanRequest{})
		if err == nil {
			return nil
		}
		if s, ok := status.FromError(err); ok && s.Code() != codes.Unavailable && s.Code() != codes.DeadlineExceeded && s.Code() != codes.Canceled {
			return nil
		}
		last = err

		select {
		case <-ctx.Done():
			return errors.Wrap(last, "timed out waiting for diff server to become ready")
		case <-time.After(readyPollInterval):
		}
	}
}

// Route returns the diff server client that serves the given group and kind,
// if any.
func (m *Manager) Route(gk schema.GroupKind) (diffv1alpha1.PlanServiceClient, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.routes[gk]
	if !ok {
		return nil, false
	}
	return s.client, true
}

// Servers returns the running diff servers.
func (m *Manager) Servers() []*Server {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*Server(nil), m.servers...)
}

// Stop closes connections and tears down containers. With keep set the
// containers stay running so the next run finds them by name and skips
// startup.
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for _, s := range m.servers {
		if s.conn != nil {
			_ = s.conn.Close()
		}
		if m.keep {
			continue
		}
		if err := m.rt.StopContainerByID(ctx, s.ContainerID); err != nil {
			errs = append(errs, errors.Wrapf(err, "cannot stop diff server container %s", s.ContainerID))
		}
	}
	return errors.Join(errs...)
}

func containerName(digest string) string {
	d := strings.TrimPrefix(digest, "sha256:")
	if len(d) > 12 {
		d = d[:12]
	}
	return containerPrefix + d
}

// dockerRuntime implements containerRuntime with the real Docker daemon.
type dockerRuntime struct{}

func (dockerRuntime) StartContainer(ctx context.Context, name, image string, opts ...docker.StartContainerOption) (string, error) {
	return docker.StartContainer(ctx, name, image, opts...)
}

func (dockerRuntime) StartContainerByID(ctx context.Context, id string) error {
	return docker.StartContainerByID(ctx, id)
}

func (dockerRuntime) StopContainerByID(ctx context.Context, id string) error {
	return docker.StopContainerByID(ctx, id)
}

func (dockerRuntime) GetRunningContainerIDByName(ctx context.Context, name string) (string, bool, bool, error) {
	c, found, err := docker.GetContainerByName(ctx, name, true)
	if err != nil || !found {
		return "", false, false, err
	}
	return c.ID, true, strings.EqualFold(string(c.State), "running"), nil
}

func (dockerRuntime) GetHostPort(ctx context.Context, id string, port int) (string, error) {
	return docker.GetHostPort(ctx, id, port)
}
