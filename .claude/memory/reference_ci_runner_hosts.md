---
name: reference_ci_runner_hosts
description: Linux self-hosted runner hosts (ubuntu-runner, ubuntu-runner-aws1/2), their labels and build parallelism, the /opt layout, and the icecream gate
metadata:
  type: reference
---

Linux self-hosted GitHub runners are org-level (`https://github.com/turing-db`). Host addresses and SSH users are not recorded here because this repo is public.

- `ubuntu-runner` - 20 cores / 62 GB. 4x `gh-runner:2404` containers only. History: 2204 was replaced by 2604 on 2026-09-27; on 2026-09-29 the 2604 ones were removed (cache/data dirs and org registrations too, since glibc 2.42 wheels can't be auditwheel-repaired) and 2204 was re-added, because 22.04 builds the `manylinux_2_35` wheels PyPI has always shipped. Later on 2026-09-29 the four 2204 ones were removed again (containers, data/cache dirs, org registrations) once `ubuntu-runner-aws2` took over `ubuntu-22.04-turing`; the compose is now `.no2204-bak`. Compose backups: `.2204-bak` (with the four 2204 services), `.2604-bak`, `.no2204-bak`. The `gh-runner:2204` image was deleted too (same day); `gh-runner:2604` is still on the host.
- Runner image: `/opt/github-runners/build/Dockerfile` takes `--build-arg BASE_IMAGE=ubuntu:XX.04`; tag it `gh-runner:XX04`. The entrypoint registers only when `/actions-runner/.runner` is missing, using `RUNNER_TOKEN` from `.env` (root, 0600). Registration tokens expire after 1 h, so write a fresh one before the first `docker compose up -d <new services>`.
- `aws-github-runner1` (c5.4xlarge, 16 cores / 30 GB) - Amazon Linux 2023. One `gh-runner:2404` container, set up 2026-09-25 by copying ubuntu-runner. As of 2026-09-29 it no longer appears in the org's runner list.
- `ubuntu-runner-aws1` (c5.4xlarge, 16 cores / 30 GB + 16 GB `/swapfile`) - Ubuntu 24.04, reached over its private IP (not on the tailnet). Set up 2026-09-29: one `gh-runner:2404` container `gh-runner-2404-1`, runner `ubuntu-runner-aws1-2404-1`, labels `ubuntu-24.04,ubuntu-24.04-turing,docker`. Its compose sets `HOST_BUILD_JOBS: 16`, which the first step of `ci_build.yml` writes into BUILD_JOBS / CMAKE_BUILD_PARALLEL_LEVEL / CTEST_PARALLEL_LEVEL through `$GITHUB_ENV`, replacing the job-level 4. Hostname is pinned with `/etc/cloud/cloud.cfg.d/99_preserve_hostname.cfg`. No icecream. After a hard reboot the runner logs "A session for this runner already exists" for about 2 min, then reconnects without help. Root volume is 145 GB: on 2026-09-30 a cold-cache job (run 36656311190 attempt 1) started with 91 GB free and died with ENOSPC at the start of Run regress. At -j16 that job took 48 min for the dependencies and 25 min for the wheel, no faster than an `ubuntu-runner` container at -j4 (47.7 and 21 min).
- `ubuntu-runner-aws2` (8 cores / 15 GB + 16 GB `/swapfile`) - Ubuntu 24.04 host, reached over its private IP. Set up 2026-09-29: Docker CE from docker's apt repo, one `gh-runner:2204` container `gh-runner-2204-1`, runner `ubuntu-runner-aws2-2204-1`, labels `ubuntu-22.04,ubuntu-22.04-turing,docker`. Compose sets `HOST_BUILD_JOBS: 8` (added 2026-09-29; backup `docker-compose.yml.no-jobs-bak`). Whether 8 parallel LLVM/MLIR compiles fit in 15 GB + swap was not yet measured. Hostname pinned like aws1. Watchdog script + timer copied unchanged from aws1 the same day; it discovers `gh-runner-2204-1` on its own. No icecream.
- `aws-github-runner2` (t3.2xlarge) was set up the same way, then terminated on 2026-09-26 and its registration removed. A replacement as c5.2xlarge (8 cores / 16 GB) was under consideration; whether LLVM/MLIR builds fit 16 GB at `BUILD_JOBS=4` was not yet measured.

Layout on every host: `/opt/github-runners/build/{Dockerfile,entrypoint.sh,docker-compose.yml,.env}`, runner state in `/opt/github-runners/data/<name>`, dep cache in `/opt/github-runners/cache/<name>`, `runner_watchdog.sh` + `gh-runner-watchdog.timer` restarts wedged listeners. Registration tokens: `gh api -X POST /orgs/turing-db/actions/runners/registration-token`.

Icecream (distributed C++ compiles for Linux dev machines) on the two AWS hosts, `/opt/icecream/`: scheduler on runner1 (tailscale0, ports 8765/8766, netname `turing`), `icecc-daemon` (built/tested on runner2; on runner1 once its runner container got the hook). Gated so it never overlaps a CI job: runner job-started hook (`ACTIONS_RUNNER_HOOK_JOB_STARTED`, `/opt/icecream/hooks/icecc_gate.sh drain`) blocks the host on the scheduler (`blockcs`), waits for in-flight compiles, stops the daemon; `gh-icecc-gate.timer` (30s) restarts + `unblockcs` when no `Runner.Worker` is running.

Verified: killing iceccd mid-compile fails the client (icecc exit 105, no local retry) - that is why it drains instead of stopping. With every helper blocked, clients compile locally.

Recreating a runner container while it runs a job kills the job - check `docker top ... | grep Runner.Worker` first.

Runner labels in workflows: see [[feedback-explicit-ci-runner-labels]].
