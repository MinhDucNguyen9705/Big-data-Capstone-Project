def test_local_spark_session(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "4"
    assert spark.range(10).count() == 10


def test_to_spark_path_keeps_spaces_and_uris():
    from cryptopipe.common.spark import to_spark_path

    assert to_spark_path("hdfs://nn:8020/lake") == "hdfs://nn:8020/lake"
    out = to_spark_path("some dir/x.jsonl")
    assert out.startswith("file:/") and out.endswith("some dir/x.jsonl") and "%20" not in out
