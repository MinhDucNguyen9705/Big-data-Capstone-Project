"""Spark jobs: thin `main(argv)` wrappers around `cryptopipe.transforms`. Registry used by `crypto job run`."""

# job name -> (module, owner, task)
JOBS: dict[str, tuple[str, str, str]] = {
    "stream_bronze": ("cryptopipe.jobs.stream_bronze", "C", "A6"),
    "stream_ohlcv": ("cryptopipe.jobs.stream_ohlcv", "C", "A6"),
    "batch_load_archive": ("cryptopipe.jobs.batch_load_archive", "D", "A6"),
    "batch_silver": ("cryptopipe.jobs.batch_silver", "D", "A6"),
    "profile": ("cryptopipe.jobs.profile", "D", "A8"),
}
