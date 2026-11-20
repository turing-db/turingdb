#!/usr/bin/env python3
"""Issue and verify the review certificate a pull request needs before it merges.

A certificate is a PR comment recording one review of the PR's diff: the review that ran,
the diff it covers, and every finding with its category and status. It names the diff by
its verbatim patch ID. A rebase keeps it valid when it leaves the diff unchanged and
merging the certified diff onto the new base gives the PR's tree exactly. The
latest certificate posted by someone with write access sets the review-certificate/<base>
commit status: success when it covers the PR's current diff and every correctness or
efficiency finding is fixed or rejected.

Usage:
  review_certificate.py target [--pr N] [--head COMMIT]
  review_certificate.py issue --input FILE
  review_certificate.py verify --pr N [--post-status]
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

MARKER = "<!-- turingdb-review-certificate -->"
CERTIFICATE_TYPE = "turingdb-review-certificate"
CERTIFICATE_VERSION = 1
STATUS_CONTEXT = "review-certificate"
COMMENT_LIMIT = 65536
STATUS_DESCRIPTION_LIMIT = 140
MINIMUM_GIT_VERSION = (2, 40)
EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
FETCH_REF = "refs/review-certificate/fetch"

LEVELS = ("low", "medium", "high", "xhigh", "max")
STATUSES = {
    "certify": ("rejected", "open"),
    "converge": ("fixed", "rejected", "deferred", "recorded", "open"),
}
STATUSES_WITH_REASON = ("rejected", "deferred")
CATEGORIES = ("correctness", "efficiency", "simplification", "test-coverage", "style")
BLOCKING_CATEGORIES = ("correctness", "efficiency")
RESOLVED_STATUSES = ("fixed", "rejected")
TRUSTED_PERMISSIONS = ("admin", "write")

DIFF_CONFIG = [
    "-c", "core.quotePath=true",
    "-c", "core.attributesFile=/dev/null",
    "-c", "diff.suppressBlankEmpty=false",
    "-c", "core.bigFileThreshold=512m",
    "-c", "merge.renames=false",
    "-c", "merge.renormalize=false",
    f"--attr-source={EMPTY_TREE}",
]
DIFF_OPTIONS = [
    "--no-color",
    "--no-ext-diff",
    "--no-textconv",
    "--no-renames",
    "--no-relative",
    "--full-index",
    "--unified=3",
    "--inter-hunk-context=0",
    "--diff-algorithm=myers",
    "--indent-heuristic",
    "--src-prefix=a/",
    "--dst-prefix=b/",
    "--ignore-submodules=none",
    "--submodule=short",
]


class CertificateError(Exception):
    pass


class CommandError(Exception):
    def __init__(self, args, message):
        super().__init__(f"{' '.join(args)}: {message}")
        match = re.search(r"\(HTTP (\d{3})\)", message)
        self.http_status = int(match[1]) if match else None


def run(args, input_bytes=None, env=None):
    result = subprocess.run(args, input=input_bytes, capture_output=True, env=env)
    if result.returncode != 0:
        raise CommandError(args, result.stderr.decode(errors="replace").strip())

    return result.stdout


def run_text(args):
    return run(args).decode().strip()


def gh_api(path, method="GET", payload=None):
    args = ["gh", "api", "-X", method, path]
    input_bytes = None
    if payload is not None:
        args += ["--input", "-"]
        input_bytes = json.dumps(payload).encode()

    output = run(args, input_bytes)
    return json.loads(output) if output.strip() else None


def check_git_version():
    version = run_text(["git", "version"])
    match = re.search(r"(\d+)\.(\d+)", version)
    if not match or (int(match[1]), int(match[2])) < MINIMUM_GIT_VERSION:
        raise CertificateError(f"{version}: pinning the diff's attributes needs git 2.40 or later")


def repository():
    slug = os.environ.get("GITHUB_REPOSITORY")
    if slug:
        return slug

    return run_text(["gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"])


def current_pull_request_number():
    try:
        return int(run_text(["gh", "pr", "view", "--json", "number", "--jq", ".number"]))
    except CommandError:
        raise CertificateError("the current branch has no open pull request: pass --pr")


def fetch(ref):
    local_ref = f"{FETCH_REF}/{os.getpid()}"
    run(["git", "fetch", "--quiet", "--no-tags", "--no-recurse-submodules", "--no-write-fetch-head",
         "origin", f"+{ref}:{local_ref}"])
    try:
        return commit_of(local_ref)
    finally:
        run(["git", "update-ref", "-d", local_ref])


def has_commit(commit):
    present = subprocess.run(["git", "cat-file", "-e", f"{commit}^{{commit}}"], capture_output=True)
    if present.returncode == 0:
        return True

    try:
        fetch(commit)
        return True
    except CommandError:
        return False


def commit_of(revision):
    return run_text(["git", "rev-parse", "--verify", "--end-of-options", f"{revision}^{{commit}}"])


def is_ancestor(ancestor, descendant):
    result = subprocess.run(["git", "merge-base", "--is-ancestor", ancestor, descendant])
    return result.returncode == 0


def pinned_git_environment():
    path = run_text(["git", "rev-parse", "--git-path", "info/attributes"])
    try:
        with open(path) as file:
            lines = file.read().splitlines()
    except FileNotFoundError:
        lines = []

    if any(line.strip() and not line.lstrip().startswith("#") for line in lines):
        raise CertificateError(f"{path} sets attributes, which change the diff a patch ID covers: empty it first")

    return {**os.environ, "GIT_ATTR_NOSYSTEM": "1"}


def patch_id(merge_base, head):
    diff = run(["git", *DIFF_CONFIG, "diff", *DIFF_OPTIONS, merge_base, head], env=pinned_git_environment())
    if not diff:
        raise CertificateError(f"{merge_base[:10]}..{head[:10]} has no diff to certify")

    output = run(["git", "patch-id", "--verbatim"], diff).decode().split()
    if not output:
        raise CertificateError(f"git patch-id found no patch in {merge_base[:10]}..{head[:10]}")

    return output[0]


def covers(certified, subject):
    if certified["patchId"] != subject["patchId"]:
        return False
    elif certified["head"] == subject["head"]:
        return True
    elif not has_commit(certified["mergeBase"]) or not has_commit(certified["head"]):
        return False

    merge = subprocess.run(["git", *DIFF_CONFIG, "merge-tree", "--write-tree", f"--merge-base={certified['mergeBase']}",
                            subject["mergeBase"], certified["head"]],
                           capture_output=True, env=pinned_git_environment())
    if merge.returncode != 0:
        return False

    return merge.stdout.decode().split()[0] == run_text(["git", "rev-parse", f"{subject['head']}^{{tree}}"])


def subject_of(pull, head):
    base = pull["base"]["ref"]
    base_commit = fetch(f"refs/heads/{base}")
    merge_base = run_text(["git", "merge-base", base_commit, head])

    return {
        "patchId": patch_id(merge_base, head),
        "base": base,
        "mergeBase": merge_base,
        "head": head,
    }


def pull_request_head(repo, pull):
    pushed_head = fetch(f"refs/pull/{pull['number']}/head")
    branch = run_text(["git", "branch", "--show-current"])
    head_repository = (pull["head"].get("repo") or {}).get("full_name")
    if not branch or branch != pull["head"]["ref"] or head_repository != repo:
        return pushed_head

    local_head = commit_of("HEAD")
    if not is_ancestor(pushed_head, local_head):
        raise CertificateError(f"HEAD does not contain #{pull['number']}'s head {pushed_head[:10]}: "
                               "pull it first, or pass --head to certify HEAD anyway")

    return local_head


def open_pull_request(repo, number):
    pull = gh_api(f"repos/{repo}/pulls/{number}")
    if pull["state"] != "open":
        raise CertificateError(f"#{number} is {pull['state']}")

    return pull


def validate_findings(kind, findings):
    if not isinstance(findings, list):
        raise CertificateError("findings must be a list")

    allowed = STATUSES[kind]
    seen = set()
    for finding in findings:
        if not isinstance(finding, dict):
            raise CertificateError("every finding must be an object")

        identifier = finding.get("id")
        if not isinstance(identifier, str) or not identifier:
            raise CertificateError("every finding needs an id")
        elif identifier in seen:
            raise CertificateError(f"finding {identifier} appears twice")
        seen.add(identifier)

        summary = finding.get("summary")
        location = finding.get("file")
        category = finding.get("category")
        status = finding.get("status")
        reason = finding.get("note")
        line = finding.get("line")
        has_reason = isinstance(reason, str) and reason.strip() != ""
        if not isinstance(summary, str) or not summary.strip():
            raise CertificateError(f"{identifier} has no summary")
        elif location is not None and (not isinstance(location, str) or re.search(r"[\x00-\x1f`]", location)):
            raise CertificateError(f"{identifier}: file must be a path on one line, with no backtick")
        elif category not in CATEGORIES:
            raise CertificateError(f"{identifier}: category must be one of {', '.join(CATEGORIES)}, not {category!r}")
        elif status not in allowed:
            raise CertificateError(f"{identifier}: /{kind} marks a finding {' or '.join(allowed)}, not {status!r}")
        elif status in STATUSES_WITH_REASON and not has_reason:
            raise CertificateError(f"{identifier} is {status} without a note giving the reason")
        elif line is not None and (not isinstance(line, int) or isinstance(line, bool)):
            raise CertificateError(f"{identifier}: line must be an integer")


def blocking_findings(findings):
    return [finding["id"] for finding in findings
            if finding["category"] in BLOCKING_CATEGORIES and finding["status"] not in RESOLVED_STATUSES]


def validate_review(review):
    kind = review.get("kind")
    if not isinstance(kind, str) or kind not in STATUSES:
        raise CertificateError(f"kind must be one of {', '.join(STATUSES)}, not {kind!r}")
    elif review.get("level") not in LEVELS:
        raise CertificateError(f"level must be one of {', '.join(LEVELS)}, not {review.get('level')!r}")

    if kind == "converge":
        iterations = review.get("iterations")
        stop_rule = review.get("stopRule")
        if not isinstance(iterations, int) or isinstance(iterations, bool) or iterations < 1:
            raise CertificateError("a /converge certificate needs iterations, at least 1")
        elif not isinstance(stop_rule, str) or not stop_rule.strip():
            raise CertificateError("a /converge certificate needs the stopRule that ended the loop")

    validate_findings(kind, review.get("findings"))


def validate_certificate(certificate):
    if not isinstance(certificate, dict):
        raise CertificateError("the certificate is not a JSON object")
    elif certificate.get("type") != CERTIFICATE_TYPE or certificate.get("version") != CERTIFICATE_VERSION:
        raise CertificateError(f"not a {CERTIFICATE_TYPE} version {CERTIFICATE_VERSION}")

    subject = certificate.get("subject")
    if not isinstance(subject, dict):
        raise CertificateError("the certificate has no subject")

    for field in ("patchId", "mergeBase", "head"):
        if not re.fullmatch(r"[0-9a-f]{40,64}", str(subject.get(field))):
            raise CertificateError(f"the subject has no {field}")

    validate_review(certificate)


def parse_certificate(body):
    blocks = re.findall(r"```json\r?\n(.*?)\r?\n```", body, re.DOTALL)
    if not blocks:
        raise CertificateError("the comment holds no certificate data")

    try:
        return json.loads(blocks[-1])
    except json.JSONDecodeError as error:
        raise CertificateError(f"the certificate data is not JSON: {error}")


def markdown_cell(text):
    escaped = str(text).replace("|", "\\|").replace("<", "&lt;").replace("@", "&#64;")
    return " ".join(escaped.split())


def render(certificate):
    subject = certificate["subject"]
    findings = certificate["findings"]
    kind = certificate["kind"]
    blocking = blocking_findings(findings)

    if blocking:
        headline = ("**Review certificate: fails.** Correctness or efficiency findings not fixed or "
                    f"rejected: {markdown_cell(', '.join(blocking))}.")
    else:
        headline = "**Review certificate: passes.** No correctness or efficiency finding is open."

    review = f"`/{kind} {certificate['level']}`"
    if kind == "converge":
        iterations = certificate["iterations"]
        plural = "" if iterations == 1 else "s"
        review += f", {iterations} iteration{plural}. Stopped: {markdown_cell(certificate['stopRule'])}"

    lines = [
        MARKER,
        headline,
        "",
        f"{review}.",
        f"Covers `{subject['mergeBase'][:10]}..{subject['head'][:10]}` against `{subject['base']}`, "
        f"patch ID `{subject['patchId'][:12]}`.",
        "",
    ]

    if findings:
        lines += ["| | Finding | Category | Status |", "|---|---|---|---|"]
        for finding in findings:
            location = finding.get("file", "")
            if location and finding.get("line") is not None:
                location += f":{finding['line']}"

            text = f"`{markdown_cell(location)}` " if location else ""
            text += markdown_cell(finding["summary"])
            status = finding["status"]
            if finding.get("note"):
                status += f": {markdown_cell(finding['note'])}"

            lines.append(f"| {markdown_cell(finding['id'])} | {text} | {finding['category']} | {status} |")
    else:
        lines.append("The review reported no finding.")

    data = json.dumps(certificate, indent=2, ensure_ascii=False).replace("`", "\\u0060")
    lines += ["", "<details><summary>Certificate data</summary>", "", "```json", data, "```", "", "</details>"]

    body = "\n".join(lines) + "\n"
    if len(body) > COMMENT_LIMIT:
        raise CertificateError(f"the certificate is {len(body)} characters, over GitHub's {COMMENT_LIMIT}")

    return body


def certificate_comments(repo, number):
    comments = []
    page = 1
    while True:
        batch = gh_api(f"repos/{repo}/issues/{number}/comments?per_page=100&page={page}")
        comments += [comment for comment in batch if (comment.get("body") or "").startswith(MARKER)]
        if len(batch) < 100:
            break
        page += 1

    comments.sort(key=lambda comment: (comment["created_at"], comment["id"]), reverse=True)
    return comments


def trusted(repo, comments):
    permissions = {}
    trusted_comments = []
    for comment in comments:
        login = comment["user"]["login"]
        if login not in permissions:
            try:
                permissions[login] = gh_api(f"repos/{repo}/collaborators/{login}/permission")["permission"]
            except CommandError as error:
                if error.http_status != 404:
                    raise
                permissions[login] = "none"

        if permissions[login] in TRUSTED_PERMISSIONS:
            trusted_comments.append(comment)

    return trusted_comments


def comment_covers(comment, subject):
    try:
        certificate = parse_certificate(comment["body"])
        validate_certificate(certificate)
    except CertificateError:
        return False

    return covers(certificate["subject"], subject)


def evaluate(repo, pull):
    head = fetch(f"refs/pull/{pull['number']}/head")
    subject = subject_of(pull, head)
    comments = certificate_comments(repo, pull["number"])
    trusted_comments = trusted(repo, comments)
    if not comments:
        return ("pending", "No review certificate: run /certify or /converge", None, head)
    elif not trusted_comments:
        return ("pending", "No certificate from a user with write access: one has to run /certify or /converge", None, head)

    for comment in trusted_comments:
        try:
            certificate = parse_certificate(comment["body"])
            validate_certificate(certificate)
        except CertificateError as error:
            return ("failure", f"Malformed certificate: {error}", comment["html_url"], head)

        if covers(certificate["subject"], subject):
            return verdict(certificate, comment, head)

    return ("pending", "The certificate covers another diff: run /certify or /converge again",
            trusted_comments[0]["html_url"], head)


def verdict(certificate, comment, head):
    url = comment["html_url"]
    findings = certificate["findings"]
    blocking = blocking_findings(findings)
    if blocking:
        return ("failure", f"Correctness or efficiency findings not fixed or rejected: {', '.join(blocking)}", url, head)

    plural = "" if len(findings) == 1 else "s"
    description = f"/{certificate['kind']} {certificate['level']} by {comment['user']['login']}: "
    description += f"{len(findings)} finding{plural}, no correctness or efficiency finding open"
    return ("success", description, url, head)


def run_url(repo):
    run_id = os.environ.get("GITHUB_RUN_ID")
    if not run_id:
        return None

    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    return f"{server}/{repo}/actions/runs/{run_id}"


def post_status(repo, pull, head, state, description, url):
    status = {
        "state": state,
        "context": f"{STATUS_CONTEXT}/{pull['base']['ref']}",
        "description": description[:STATUS_DESCRIPTION_LIMIT],
    }
    if url:
        status["target_url"] = url

    gh_api(f"repos/{repo}/statuses/{head}", method="POST", payload=status)


def command_target(args):
    check_git_version()
    repo = repository()
    number = args.pr or current_pull_request_number()
    pull = open_pull_request(repo, number)
    head = commit_of(args.head) if args.head else pull_request_head(repo, pull)
    subject = subject_of(pull, head)

    print(json.dumps({"pr": number, **subject}, indent=2))
    return 0


def command_issue(args):
    check_git_version()
    with open(args.input) as file:
        review = json.load(file)

    if not isinstance(review, dict):
        raise CertificateError(f"{args.input} does not hold a JSON object")

    missing = [field for field in ("pr", "head", "reviewBase") if field not in review]
    if missing:
        raise CertificateError(f"{args.input} has no {', '.join(missing)}")

    validate_review(review)
    repo = repository()
    pull = open_pull_request(repo, review["pr"])
    head = commit_of(review["head"])
    review_base = commit_of(review["reviewBase"])
    subject = subject_of(pull, head)

    if not is_ancestor(review_base, subject["mergeBase"]):
        raise CertificateError(f"the review diffed from {review_base[:10]}, so it never saw "
                               f"{subject['mergeBase'][:10]}..{review_base[:10]} of #{pull['number']}: "
                               "run /certify on the whole pull request")

    pushed = subject_of(pull, fetch(f"refs/pull/{pull['number']}/head"))
    issuer = gh_api("user")["login"]
    certificate = {
        "type": CERTIFICATE_TYPE,
        "version": CERTIFICATE_VERSION,
        "subject": subject,
        "kind": review["kind"],
        "level": review["level"],
        "reviewBase": review_base,
    }
    if review["kind"] == "converge":
        certificate["iterations"] = review["iterations"]
        certificate["stopRule"] = review["stopRule"]
    certificate["findings"] = review["findings"]
    certificate["issuer"] = issuer
    certificate["issuedAt"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    body = render(certificate)
    if parse_certificate(body) != certificate:
        raise CertificateError("the rendered certificate does not read back as the same data")

    comments_path = f"repos/{repo}/issues/{pull['number']}/comments"
    posted = gh_api(comments_path, method="POST", payload={"body": body})
    replaces_pushed = covers(subject, pushed)
    for comment in certificate_comments(repo, pull["number"]):
        is_older_own = comment["user"]["login"] == issuer and comment["id"] < posted["id"]
        if is_older_own and (replaces_pushed or not comment_covers(comment, pushed)):
            gh_api(f"repos/{repo}/issues/comments/{comment['id']}", method="DELETE")

    state, description, _, pull_head = evaluate(repo, pull)
    print(f"posted {posted['html_url']}")
    print(f"#{pull['number']} at {pull_head[:10]}: {state}: {description}")
    return 0


def command_verify(args):
    repo = repository()
    pull = gh_api(f"repos/{repo}/pulls/{args.pr}")
    if pull["state"] != "open":
        print(f"#{args.pr} is {pull['state']}: nothing to verify")
        return 0

    try:
        check_git_version()
        state, description, url, head = evaluate(repo, pull)
    except Exception as error:
        if args.post_status:
            post_status(repo, pull, pull["head"]["sha"], "error", f"Could not verify: {error}", run_url(repo))
        raise

    print(f"#{args.pr} at {head[:10]}: {state}: {description}")
    if args.post_status:
        post_status(repo, pull, head, state, description, url or run_url(repo))
        return 0
    else:
        return 0 if state == "success" else 1


def main():
    parser = argparse.ArgumentParser(description="Issue and verify pull request review certificates.")
    commands = parser.add_subparsers(dest="command", required=True)

    target = commands.add_parser("target", help="print the diff a certificate of the pull request covers")
    target.add_argument("--pr", type=int, help="pull request number, default: the current branch's")
    target.add_argument("--head", help="commit to certify, default: HEAD on the PR's branch, else the PR head")
    target.set_defaults(handler=command_target)

    issue = commands.add_parser("issue", help="post a certificate on the pull request")
    issue.add_argument("--input", required=True, help="JSON file holding the review and its findings")
    issue.set_defaults(handler=command_issue)

    verify = commands.add_parser("verify", help="check the pull request's latest certificate")
    verify.add_argument("--pr", type=int, required=True, help="pull request number")
    verify.add_argument("--post-status", action="store_true", help=f"post the {STATUS_CONTEXT}/<base> commit status")
    verify.set_defaults(handler=command_verify)

    args = parser.parse_args()
    try:
        return args.handler(args)
    except (CertificateError, CommandError, OSError, json.JSONDecodeError) as error:
        print(f"review_certificate: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
