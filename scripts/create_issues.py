"""Bootstrap step S10: one GitHub issue per task of docs/implementation-plan.md, with labels and a milestone.

    python scripts/create_issues.py --due 2026-10-27 --dry-run   # print what would be created
    python scripts/create_issues.py --due 2026-10-27             # needs `gh auth login` and a GitHub remote

Part A tasks (A2..A11) get label `checkpoint-1` and milestone "Checkpoint 1"; Part B milestones get `later`.
Each issue body is the task's section from the plan (Deliverables / Spec / Done when). Re-running skips issues
whose title already exists, so the script is idempotent.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

PLAN = Path(__file__).resolve().parents[1] / "docs" / "implementation-plan.md"
MILESTONE = "Checkpoint 1"

OWNERS = {  # task id -> owners (A Platform, B Ingestion, C Speed layer, D Batch and data, E Serving and docs)
    "A2": "BC",  # done in the bootstrap; B and C review the contracts they consume
    "A3": "B",
    "A4": "D",
    "A5": "C",
    "A6": "CDE",
    "A7": "AE",
    "A8": "D",
    "A9": "E",
    "A10": "E",
    "A11": "ABCDE",
    "B1": "B",
    "B2": "D",
    "B3": "C",
    "B4": "CDE",
    "B5": "AE",
    "B6": "A",
    "B7": "E",
}
LABELS = {
    **{f"owner:{m}": ("1d76db", f"Owned by member {m}") for m in "ABCDE"},
    "checkpoint-1": ("d93f0b", "Needed for Checkpoint 1"),
    "later": ("c5def5", "After the checkpoint (Part B)"),
}


def parse_tasks(text: str) -> list[tuple[str, str, str]]:
    """(task id, title, body) for every `### A<n>.` / `### B<n>.` section."""
    tasks = []
    heads = list(re.finditer(r"^### ([AB]\d+)\. (.+)$", text, re.M))
    for i, m in enumerate(heads):
        end = heads[i + 1].start() if i + 1 < len(heads) else len(text)
        body = text[m.end() : end]
        body = re.split(r"^---\s*$|^## ", body, flags=re.M)[0].strip()
        tasks.append((m.group(1), m.group(2).strip(), body))
    return tasks


def gh(*args: str, dry_run: bool, input: str | None = None) -> str:
    if dry_run:
        print("gh", " ".join(a if " " not in a else repr(a) for a in args))
        return ""
    return subprocess.run(
        ["gh", *args], check=True, text=True, encoding="utf-8", capture_output=True, input=input
    ).stdout


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # task titles contain emoji
    parser = argparse.ArgumentParser()
    parser.add_argument("--due", required=True, help="Checkpoint date YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    tasks = [t for t in parse_tasks(PLAN.read_text(encoding="utf-8")) if t[0] != "A1"]  # A1 = the bootstrap
    for name, (color, desc) in LABELS.items():
        gh("label", "create", name, "--color", color, "--description", desc, "--force", dry_run=args.dry_run)

    if args.dry_run:
        existing_titles: set[str] = set()
    else:
        milestones = json.loads(gh("api", "repos/{owner}/{repo}/milestones?state=all", dry_run=False))
        existing_titles = {i["title"] for i in json.loads(
            gh("issue", "list", "--state", "all", "--limit", "500", "--json", "title", dry_run=False))}  # fmt: skip
        if MILESTONE not in {m["title"] for m in milestones}:
            gh("api", "repos/{owner}/{repo}/milestones", "-f", f"title={MILESTONE}",
               "-f", f"due_on={args.due}T23:59:59Z", dry_run=False)  # fmt: skip
    if args.dry_run:
        gh("api", "repos/{owner}/{repo}/milestones", "-f", f"title={MILESTONE}", "-f",
           f"due_on={args.due}T23:59:59Z", dry_run=True)  # fmt: skip

    for task_id, title, body in tasks:
        full_title = f"{task_id}: {title}"
        if full_title in existing_titles:
            print(f"exists: {full_title}")
            continue
        labels = [f"owner:{m}" for m in OWNERS.get(task_id, "")]
        labels.append("checkpoint-1" if task_id.startswith("A") else "later")
        cmd = ["issue", "create", "--title", full_title, "--body-file", "-", "--label", ",".join(labels)]
        if task_id.startswith("A"):
            cmd += ["--milestone", MILESTONE]
        source = f"\n\n---\nFrom `docs/implementation-plan.md`, task {task_id}. Owners: {', '.join(labels[:-1])}."
        gh(*cmd, dry_run=args.dry_run, input=body + source)
    print(f"{len(tasks)} tasks processed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
