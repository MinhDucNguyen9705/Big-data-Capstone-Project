"""Download winutils.exe + hadoop.dll into .tools/hadoop so local Spark can write files on Windows.

Linux, macOS, WSL, CI and the Docker images do not need this. Usage: python scripts/setup_winutils.py
`cryptopipe.common.spark.get_spark(local=True)` picks up .tools/hadoop automatically.
"""

from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

HADOOP_VERSION = "hadoop-3.3.6"
BASE = f"https://github.com/cdarlint/winutils/raw/master/{HADOOP_VERSION}/bin"
FILES = ["winutils.exe", "hadoop.dll"]


def main() -> int:
    dest = Path(__file__).resolve().parents[1] / ".tools" / "hadoop" / "bin"
    dest.mkdir(parents=True, exist_ok=True)
    for name in FILES:
        target = dest / name
        if target.exists():
            print(f"exists: {target}")
            continue
        print(f"downloading {BASE}/{name}")
        urllib.request.urlretrieve(f"{BASE}/{name}", target)
    print(f"HADOOP_HOME will be {dest.parent}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
