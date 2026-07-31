# Releasing milvus-sdk-java

This document is for maintainers publishing immutable release artifacts to Maven Central.
GitHub Actions is the normal release path. Manual local publication is an emergency fallback and
must never run concurrently with the GitHub Actions publisher for the same version.

## GitHub Actions setup

Create a GitHub environment named `maven-central` and configure any required reviewers and
deployment restrictions according to the repository's release policy. A required-reviewer rule
turns the environment into the approval gate for Maven Central publication; without one,
publication starts automatically when a matching tag is pushed.

Add these secrets to the `maven-central` environment:

- `MAVEN_CENTRAL_USERNAME`: username from a Central Portal user token
- `MAVEN_CENTRAL_PASSWORD`: password from the same Central Portal user token
- `MAVEN_GPG_PRIVATE_KEY`: complete ASCII-armored private signing key
- `MAVEN_GPG_PASSPHRASE`: passphrase for the private signing key

Never store these values in the repository, workflow logs, release notes, or shell history.
Protect release tags so that only authorized maintainers can create them.

These environment variables are specific to the ephemeral GitHub runner. A maintainer publishing
locally can instead use Maven credentials from `~/.m2/settings.xml` and a private key already
installed in the local GPG keyring.

## Common release preparation

1. Prepare and merge a release commit that updates:
   - the root `pom.xml` revision
   - version references in `README.md`
   - `CHANGELOG.md`
   - standalone module or example versions when applicable
2. Confirm the publishing workflow exists in the release commit. Workflows run from the commit
   referenced by the tag, not from the latest commit on another branch.
3. Confirm required CI checks passed for the exact release commit.
4. Confirm the POM revision:

   ```bash
   mvn --batch-mode --no-transfer-progress help:evaluate \
     -Dexpression=revision -DforceStdout -q
   ```

5. Create and push an exact `vX.Y.Z` tag pointing to the release commit:

   ```bash
   git tag -a vX.Y.Z <release-commit> -m "Release vX.Y.Z"
   git push source vX.Y.Z
   ```

The tag version must exactly match the root POM revision. Do not create a release tag before the
release commit has been merged into the intended release branch.

## GitHub Actions publication

The tag-triggered workflow is defined in `.github/workflows/publish-release.yaml`. For each exact
`vX.Y.Z` tag, it:

1. Checks out the tagged commit and its submodules.
2. Uses Temurin Java 8u462.
3. Verifies that the tag and POM revision match.
4. Builds, signs, and deploys with the Maven `release` profile.
5. Waits up to 3300 seconds for Central to report the deployment as `PUBLISHED`.
6. Creates or updates the corresponding non-draft GitHub release.

Release runs share a durable concurrency queue and are published one at a time. Artifacts already
published to Maven Central cannot be overwritten or deleted by rerunning the workflow.

## Manual local publication

Manual local publication does not require the GitHub environment variables. It uses:

- Central credentials from the `central` server entry in `~/.m2/settings.xml`
- a private signing key from the local `~/.gnupg` keyring
- a passphrase supplied by Maven settings, `gpg-agent`, or local pinentry

Use this path only as a controlled fallback, such as after the tag-triggered publisher fails before
uploading a bundle. First check Central Portal and confirm that the version has not already been
accepted or published. Then check out the exact release tag and publish:

```bash
git checkout --detach vX.Y.Z
git submodule update --init
mvn --batch-mode --no-transfer-progress \
  -Prelease -Dmaven.test.skip=true clean deploy
```

The POM configures the Central plugin to wait until the deployment reaches `PUBLISHED`. After local
publication succeeds, do not rerun the Maven publishing job. Create the GitHub release manually
using the reconciliation procedure below.

Do not publish locally before pushing a normal release tag: pushing the tag automatically starts
the GitHub Actions publisher and can cause both paths to attempt the same immutable version. If a
release must use the local path from the beginning, an authorized maintainer must first prevent the
tag-triggered publisher from running for that tag and must restore the normal automation afterward.

## Recover from a failed run

First inspect the workflow log and the deployment at
https://central.sonatype.com/publishing/deployments. Do not blindly rerun the complete workflow
after the bundle has been uploaded.

### Failure before Central upload

If the log does not contain a successful bundle upload:

- For a transient runner or network failure, rerunning the workflow is safe.
- For a problem in the tagged source or workflow, fix the release commit. The tag may be recreated
  only after confirming that Central did not accept or publish a deployment for that version.

### Bundle uploaded but workflow failed or timed out

Check the deployment state in Central Portal:

- `VALIDATING` or `PUBLISHING`: wait for Central to finish. Do not rerun Maven.
- `PUBLISHED`: do not rerun Maven. Reconcile the missing GitHub release as described below.
- `FAILED`: inspect the Central validation errors and confirm that no component was published
  before preparing a corrected release attempt.

If Maven reports that the coordinates already exist, treat the version as published and do not
attempt to replace it.

### Central published but the GitHub release is missing

Create the GitHub release from the existing tag without running Maven again:

```bash
version="X.Y.Z"
milvus_minor="X.Y"
gh release create "v${version}" \
  --title "milvus-sdk-java-${version}" \
  --notes "Release date: $(date -u +%F)
Compatible with Milvus v${milvus_minor}.x"
```

If a draft already exists, publish or edit that draft instead of creating another release. Verify
that downstream documentation and release automation completed after the GitHub release is
published.

## Release invariants

- Never move, delete, or reuse a tag after Maven Central publishes its version.
- Never attempt to overwrite an existing Maven Central version.
- Do not create the GitHub release until Central reports `PUBLISHED`.
- When uncertain whether Central accepted a bundle, inspect Central Portal before rerunning Maven.
