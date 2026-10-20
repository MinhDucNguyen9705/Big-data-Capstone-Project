"""SparkApplication main file: `local:///opt/cryptopipe/run_job.py <job name> [job args...]`.

Dispatches to `cryptopipe.jobs.<name>.main(argv)` using the registry in `cryptopipe.jobs.JOBS`.
"""

import importlib
import sys

from cryptopipe.jobs import JOBS

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in JOBS:
        sys.exit(f"usage: run_job.py <{'|'.join(JOBS)}> [args...]")
    module, _owner, _task = JOBS[sys.argv[1]]
    sys.exit(importlib.import_module(module).main(sys.argv[2:]))
