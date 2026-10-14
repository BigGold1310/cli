This command previews what applying one or more managed resources would do,
without applying anything: which resources it would create, which fields it
would change, and, most importantly, which changes the provider could only
apply by destroying and recreating the external resource.

For each input resource the CLI fetches the live managed resource from the
cluster, if one exists, and asks the provider itself for the verdict: it runs
the provider's package image locally as a diff server (a stateless,
credential-free gRPC service upjet-based providers serve) and sends it the
desired and live state. The provider computes its own diff, so the preview
includes replacement detection, values known only after apply, and redacted
sensitive values. A resource with no live counterpart plans as a create.

Which providers run as diff servers comes from the current kubeconfig context
by default: the CLI lists the providers installed on the cluster and uses
their package images, so the preview matches the provider versions the
cluster actually runs. --provider-images overrides that discovery, for
example to preview how a provider version that is not installed yet would
treat existing resources.

Not every provider serves diffs. A provider package declares the capability
in its metadata, and a resource whose provider serves no diff server (or
whose diff server cannot compute a diff for it) still appears in the preview
with an approximate client-side comparison, marked [?], rather than failing
the run or silently dropping out.

Nothing simulate does writes to the cluster or the cloud: it reads the
cluster with your kubeconfig and RBAC, and the diff servers run locally with
no credentials and no cluster access.

Diff server startup takes a few seconds per provider image. In an
edit-simulate-edit loop, --keep-diff-servers leaves the containers running so
the next run reuses them; they are named `crossplane-diff-server-DIGEST` and
are cleaned up by any later run without the flag, or manually with docker rm.

Examples:

  # Preview what applying resources.yaml would do.
  crossplane resource simulate -f resources.yaml

  # Preview against a specific provider version instead of the installed one.
  crossplane resource simulate -f resources.yaml --provider-images xpkg.upbound.io/upbound/provider-aws-ec2:v2.9.0

  # Keep diff servers running for faster subsequent runs.
  crossplane resource simulate -f resources.yaml --keep-diff-servers

  # Emit machine-readable results for CI gating.
  crossplane resource simulate -f resources.yaml -o json
