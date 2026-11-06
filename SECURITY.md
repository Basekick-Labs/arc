# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately through GitHub: open the
repository's **Security** tab and choose **Report a vulnerability**. Do not
open a public issue for a suspected vulnerability.

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
