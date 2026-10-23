"""Download the jars in jars.txt from Maven Central and verify each against its published SHA-1.

Build-time helper for docker/spark/Dockerfile (owner A):  python fetch_jars.py jars.txt /jars
"""

from __future__ import annotations

import hashlib
import sys
import urllib.request
from pathlib import Path

MAVEN = "https://repo1.maven.org/maven2"


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as resp:
        return resp.read()


def main(spec: str, out_dir: str) -> int:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for line in Path(spec).read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        group, artifact, version = line.split(":")
        name = f"{artifact}-{version}.jar"
        url = f"{MAVEN}/{group.replace('.', '/')}/{artifact}/{version}/{name}"
        data = fetch(url)
        expected = fetch(url + ".sha1").decode().split()[0].strip().lower()
        actual = hashlib.sha1(data).hexdigest()
        if actual != expected:
            print(f"SHA-1 mismatch for {name}: expected {expected}, got {actual}", file=sys.stderr)
            return 1
        (out / name).write_bytes(data)
        print(f"{name:70s} {len(data) / 1e6:6.1f} MB  sha1 ok")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
