def test_local_spark_session(spark):
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
    assert spark.conf.get("spark.sql.shuffle.partitions") == "4"
    assert spark.range(10).count() == 10
