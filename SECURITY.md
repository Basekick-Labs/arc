# Security Policy

## Reporting a vulnerability

Report suspected vulnerabilities privately through GitHub: open the
repository's **Security** tab and choose **Report a vulnerability**. Do not
open a public issue for a suspected vulnerability.

Read this whole page before you report. A report that ignores it is closed
without discussion.

## What a report must contain

A report is triaged only when all of these hold. Otherwise it is closed with a
one-line reason, and closed reports are not re-argued.

1. **Reproduced against current code.** Name the release *and* the `main`
   commit you tested, and reproduce on both. A report against an old revision
   whose issue is already fixed on `main` is closed as already fixed, with no
   credit.
2. **Reproduced on a running Arc.** Give the configuration, the commands you
   ran and the output Arc actually returned. A unit test against internals is
   welcome as a supplement, never as the evidence: fixtures that fake the
   condition (symlinked directories, fake schedulers, stubbed peers, hand-edited
   responses) prove nothing about Arc.
3. **A boundary crossed.** Say which principal gains what it should not have:
   a token reading or writing data its permissions do not grant, an
   unauthenticated caller doing anything beyond what is documented as public,
   or a remote caller stopping the service. "This could be hardened" is not a
   vulnerability.
4. **Not already known.** Check this page, the release notes and the closed
   advisories first. Repeats of known behaviour are closed without a reply.
5. **One issue per report**, written by someone who has read it.

The severity is ours to set. A CVSS score in a report is ignored.

## Automated and bulk reports

AI-assisted reports are welcome when a person has checked the finding, run the
reproduction against a running build and stands behind every claim in it.
Detected slop (invented code paths, made-up line numbers, reproductions that
were never run, findings that restate this page) is closed and the account is
blocked from this repository.

Reports produced by a tool or a language model and filed without a person
reproducing them against a running build are not accepted. That includes
batches filed within minutes, reports from accounts created to file them, and
reports that restate this page's known behaviour as a finding.

They are closed without a response, the accounts are reported to GitHub, and
they are blocked from the organisation. Using a tool to find a bug is fine;
making us verify what you did not is not.

**Reports from new GitHub accounts are discarded unread.** An account created
less than 90 days before the report is treated as an account created to file
it, whatever the report says.

## Out of scope

These are known, documented or by design, and reports about them are closed
without discussion:

- **Anything under `deploy/`.** The Docker Compose stacks, Helm values and
  manifests are development and demonstration setups. Their example
  credentials, open ports and bundled services are not a production
  deployment; operators provide their own.
- **Anything that requires the cluster shared secret** (`cluster.shared_secret`)
  or a node that holds it. See the cluster trust model below: every holder is a
  fully trusted member, and impersonating another node with it is not a
  boundary.
- **MQTT publishers.** Subscriptions are created by admins and carry no tenant;
  the subscription, its `topic_mapping` rules and the broker's ACLs decide what
  a publisher can write. See the MQTT integration docs (Trust model).
- **Names that differ only by case on a local volume that folds names**
  (macOS, Windows). Known and handled
  ([#1509](https://github.com/Basekick-Labs/arc/issues/1509)).
- **What an admin token can do.** Admins configure Arc; capabilities granted to
  admin tokens by design are not escalations.
- **Deployments without RBAC.** Without roles, a read token reads every
  database by design. Isolation between tenants requires RBAC.
- **Volumetric denial of service** (flooding connections or requests), and
  limits an operator has configured or raised (`server.max_payload_size` and
  similar). A request that makes Arc consume resources far out of proportion
  to its size is in scope.
- **Missing hardening without a demonstrated impact**: security headers, TLS
  settings an operator chooses, "best practice" checklists, scanner output.
- **Vulnerabilities in dependencies** without a demonstrated path through Arc.
- **arcx internals.** arcx is developed in a separate repository; report its
  behaviour through support.

## Cluster trust model

Arc cluster nodes authenticate each other with one shared secret
(`cluster.shared_secret`). What that does and does not guarantee:

- **Every node that holds the shared secret is a fully trusted member of the
  cluster.** Peer messages are authenticated with an HMAC under that one
  secret, so a node ID inside a signed message is a claim, not a proven
  identity. Any holder can act as any node: join, leave, send heartbeats,
  forward Raft commands, read any replicated file, and assert the identity
  of an API caller on a request it forwards.
- **`cluster.tls_enabled` encrypts Raft and coordinator traffic.**
  Inter-node HTTP (request routing, forwarded requests, cache invalidation)
  runs on the API listener and is encrypted by `server.tls_enabled`. With
  `cluster.tls_ca_file` set, Raft and coordinator peers must also present a
  certificate from that CA. That proves the peer is a member of the cluster;
  it does not prove which node the peer is, because certificates are not
  bound to node IDs.
- **Raft tolerates crashed nodes, not malicious ones.** A compromised node
  or a leaked secret can disrupt the cluster or change its state.

Treat the shared secret as a root credential for the whole cluster:

- Keep it out of container images, source control and logs.
- Restrict the cluster coordinator and Raft ports to the cluster network.
- If any node is compromised, rotate the secret on every node.

Per-node peer identity, which would let a node prove which node it is, is
tracked in [#1510](https://github.com/Basekick-Labs/arc/issues/1510).
