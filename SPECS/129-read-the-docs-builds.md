# 129: Read the Docs has not built since 2026-11-17

Ticket #129. Found on 2026-11-27 while checking the #128 paragraph in the served guide.

## EARS spec

- E1. When a commit reaches main or a release tag is pushed, Read the Docs shall build the
  documentation from `Stabilize-Software-Foundation/Stabilize`.
- E2. `https://stabilize.readthedocs.io/en/latest/` shall serve the documentation of main, and
  `/en/stable/` that of the newest release tag.
- E3. After the fix, the served `guide/tasks.html` shall contain the "Failed requests" and "Kept
  connections" sections of 0.36.0 and the #128 paragraph, checked over HTTP with
  `Cache-Control: no-cache`.

No quality threshold applies.

## What was observed

- `https://stabilize.readthedocs.io/en/latest/guide/tasks.html` answers 200. Its title is "Tasks,
  Stabilize 0.34.5 documentation". It has no "Failed requests", `error_code`, `keep_alive` or
  `worker_id`.
- `GET https://readthedocs.org/api/v3/projects/stabilize/builds/` lists the last builds as 35946124
  (latest, 2026-11-17T03:08Z, commit 87d258fb) and 35946021 (stable, 2026-11-17T03:00Z, commit
  d80bcfb9, which is v0.34.5).
- `GET https://readthedocs.org/api/v3/projects/stabilize/` gives the repository as
  `https://github.com/rodmena-limited/stabilize.git`.
- The repository moved to `Stabilize-Software-Foundation/Stabilize` on 2026-11-24. No build has run
  since. The documentation of 0.34.6, 0.35.0, 0.35.1 and 0.36.0 is not served.

## Solution domain and alternatives

The Read the Docs project settings and its GitHub webhook. Nothing in this repository changes.
`.readthedocs.yaml` is present and built until 2026-11-17.

- Set the project's repository URL to the new one, re-create the GitHub integration, and trigger a
  build of `latest` and `stable` [CHOSEN]. It needs a maintainer of the Read the Docs project. `gh` on
  vm-1 is the account `ourway`, which lacks the `admin:repo_hook` scope.
- Trigger builds by hand after every merge [REJECTED: the next merge is stale again].

## Not exercised

Whether the old webhook still exists on the moved repository. The hook list needs a scope this host
does not have.
