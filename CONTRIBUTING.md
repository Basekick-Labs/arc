# Contributing to Arc

Thanks for your interest in improving Arc. Contributions of all sizes are welcome: bug fixes, tests, documentation, and features. This guide explains how to get a change from idea to merged.

## Finding something to work on

- Issues labeled [`good first issue`](https://github.com/Basekick-Labs/arc/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) are scoped, well-described, and a good entry point.
- Most issues include the file, the line, and the intended fix shape. If the shape is unclear, ask on the issue before writing code.
- Comment on an issue when you start working on it, so effort is not duplicated.

## Before you open a PR

1. **One issue per PR.** Reference it in the body (`Closes #123`, or `Refs #123` if your change covers only part of it).
2. **Keep PRs small.** We do not review large PRs. If the fix you have in mind is large, split it into a series of smaller PRs that can be reviewed and merged independently. A PR that does one thing well merges fast; a PR that does five things waits.
3. **Add tests.** A bug fix needs a regression test that fails before the fix and passes after it. Deterministic tests are strongly preferred over sleeps and retries.
4. **Add a release-notes entry.** Fixes go into the current planned release notes file (for example `RELEASE_NOTES_2026.09.2.md`) as a `###` entry under the `## Bug fixes` section, ending with a credit line:

   ```markdown
   Contributed by [@your-handle](https://github.com/your-handle) in [#PR](https://github.com/Basekick-Labs/arc/pull/PR).
   ```

   Merged contributions are also credited in the README and in the release blog post.
5. **Match the house style.** Run `gofmt` and `go vet`. Reuse the patterns the surrounding code already uses (for example struct logger fields, not context-carried loggers) rather than introducing new ones.
6. **Leave "Allow edits by maintainers" enabled.** We often resolve release-notes conflicts and small fixups directly on your branch so your PR can merge without another round trip.
7. **Sign the CLA.** A bot comments on your first PR with a one-line reply to post. See [Contributor License Agreement](#contributor-license-agreement) below for what it covers and why it exists. You sign once, not per PR.

## AI-assisted contributions

AI patches and contributions are welcome. Two conditions:

- **Be strong on the logic.** You are the author. Understand why the change is correct, what the failure mode was, and what the edge cases are. If a reviewer asks why a line exists, "the tool wrote it" is not an answer.
- **Review it yourself first.** Read the whole diff, run the tests, and cut anything you cannot defend before submitting. We review every PR the same way regardless of how it was written, and unverified AI output wastes the review cycle that could have gone to your next contribution.

The same size rule applies double here: AI tools make it easy to generate large diffs, and we will ask you to split them.

## Contributor License Agreement

Arc is licensed under AGPL-3.0, and Basekick Labs also ships commercially
licensed builds of Arc. Including a contribution in both requires your explicit
permission, which is what the [CLA](CLA.md) grants.

In short:

- **You keep ownership of your contribution.** The CLA is a license grant, not a
  copyright assignment. You can use, relicense, or redistribute your own work
  anywhere else, with no restriction.
- **You grant Basekick Labs the right to license your contribution under other
  terms**, including in commercial and closed-source builds of Arc.
- **You confirm the work is yours to give** — that it is your original creation,
  and that if your employer owns your work output, you have permission to submit
  it.

Signing takes one comment. On your first PR a bot posts instructions; you reply
with the sign-off line it gives you, and your signature is recorded against your
GitHub username. You will not be asked again on later PRs.

If you cannot sign (for example, your employer will not permit it), say so on the
issue before writing code and we will find another way to get the fix in —
usually by reimplementing it from the described behavior rather than the patch.

Read the full text in [CLA.md](CLA.md).

## Building and testing

Arc is a Go project (Go 1.26+). DuckDB integration uses cgo, so the full suite needs a cgo-capable toolchain:

```sh
go build ./...
go test ./...                          # package tests
go test -tags=duckdb_arrow -race ./... # what CI runs
```

If your environment cannot run the cgo-dependent packages (common on Windows), say so in the PR body and run what you can. Linux CI is the authoritative validation, and maintainers verify locally before merging.

## Review and merge

- CI must be green. First-time contributors need a maintainer to approve the workflow run; this usually happens at first review.
- Reviews verify claims locally, so precise PR descriptions (what you ran, what you observed) speed things up.
- PRs are squash-merged with the PR title as the commit subject, so write the title as a conventional commit (`fix(scope): what changed`).

## And finally

If Arc is useful to you, or you just enjoyed contributing, star the repo ;)
