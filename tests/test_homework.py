"""Барлық 19 тапсырма және шекаралық мәндерге арналған автотесттер."""

import csv
from collections import Counter, defaultdict
from pathlib import Path
import sys

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from solution import homework as hw

DATA_PATH = str(Path(__file__).resolve().parents[1] / "data" / "popular_people.csv")


@pytest.fixture(scope="session")
def spark():
    session = hw.create_spark_session()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def raw_df(spark):
    return hw.load_data(spark, DATA_PATH)


@pytest.fixture(scope="session")
def clean_df(raw_df):
    result = hw.normalize_names(hw.add_popularity_tier(hw.decode_gender(hw.clean_data(raw_df))))
    result.cache()
    result.count()
    yield result
    result.unpersist()


@pytest.fixture
def sample(spark):
    schema = StructType([
        StructField("name", StringType()),
        StructField("gender", IntegerType()),
        StructField("known_for_department", StringType()),
        StructField("original_name", StringType()),
        StructField("popularity", DoubleType()),
    ])
    return spark.createDataFrame([
        ("  aLICE SMITH  ", 1, "Acting", "Alice Smith", 40.0),
        ("Bob", 2, "Acting", "Bob", 39.999),
        ("Carol", 3, "Directing", "Carol", 20.0),
        ("Dan", 0, "Directing", "Dan", 19.999),
        ("Eve", 99, "Acting", "Eve", 10.0),
        ("Finn", None, "Acting", "Finn", 9.999),
        ("No department", 2, "", "No department", 1.0),
        ("Null department", 2, None, "Null department", 1.0),
        (None, 1, "Acting", None, 1.0),
    ], schema)


def test_create_spark_session(spark):
    assert spark.sparkContext.appName == "PopularPeopleApp"
    assert hw.create_spark_session() is spark


def test_load_data(raw_df):
    assert raw_df.count() == 9980
    assert raw_df.columns == ["name", "gender", "known_for_department", "original_name", "popularity"]


def test_load_data_with_schema(spark):
    df = hw.load_data_with_schema(spark, DATA_PATH)
    assert [field.dataType.typeName() for field in df.schema] == [
        "string", "integer", "string", "string", "double",
    ]
    assert df.count() == 9980


def test_clean_data(sample, clean_df):
    cleaned = hw.clean_data(sample)
    assert cleaned.count() == 6
    assert cleaned.filter(F.col("name").isNull() | F.col("known_for_department").isNull()).count() == 0
    assert clean_df.filter(F.col("known_for_department") == "").count() == 0


def test_decode_gender(sample):
    labels = {row.original_name: row.gender_label for row in hw.decode_gender(sample).collect()}
    assert labels["Alice Smith"] == "Female"
    assert labels["Bob"] == "Male"
    assert labels["Carol"] == "Other"
    assert labels["Dan"] == labels["Eve"] == labels["Finn"] == "Unknown"
    assert "gender" in hw.decode_gender(sample).columns


def test_popularity_tier_boundaries(sample):
    tiers = {row.original_name: row.popularity_tier for row in hw.add_popularity_tier(sample).collect()}
    assert [tiers[name] for name in ["Alice Smith", "Bob", "Carol", "Dan", "Eve", "Finn"]] == [
        "A-list", "B-list", "B-list", "C-list", "C-list", "Unknown",
    ]


def test_normalize_names(sample):
    row = hw.normalize_names(sample).filter(F.col("original_name") == "Alice Smith").first()
    assert row.name == "Alice Smith"
    assert row.original_name == "Alice Smith"


def test_register_temp_view(spark, clean_df):
    hw.register_temp_view(clean_df)
    assert spark.table("people").count() == clean_df.count()


def test_dept_stats_sql(spark, clean_df):
    hw.register_temp_view(clean_df)
    actual = hw.dept_stats_sql(spark).collect()
    # Тәуелсіз бақылау: бастапқы CSV-ді Python арқылы санап салыстыру.
    expected = defaultdict(list)
    with open(DATA_PATH, encoding="utf-8", newline="") as source:
        for row in csv.DictReader(source):
            if row["name"] and row["known_for_department"]:
                expected[row["known_for_department"]].append(float(row["popularity"]))
    for row in actual:
        values = expected[row.known_for_department]
        assert row.person_count == len(values)
        assert row.avg_popularity == pytest.approx(sum(values) / len(values), abs=0.0051)
        assert row.max_popularity == max(values)
    assert len(actual) == len(expected)
    assert [row.avg_popularity for row in actual] == sorted(
        [row.avg_popularity for row in actual], reverse=True,
    )


def test_gender_distribution_sql(spark, sample):
    df = hw.decode_gender(hw.clean_data(sample))
    hw.register_temp_view(df)
    actual = {row.gender_label: (row["count"], row.percentage)
              for row in hw.gender_distribution_sql(spark).collect()}
    expected = {"Unknown": (3, 50.0), "Female": (1, 16.67),
                "Male": (1, 16.67), "Other": (1, 16.67)}
    assert set(actual) == set(expected)
    for label, (count, percentage) in actual.items():
        assert count == expected[label][0]
        assert float(percentage) == pytest.approx(expected[label][1], abs=0.001)


def test_top5_by_dept_sql(spark, clean_df):
    hw.register_temp_view(clean_df)
    rows = hw.top5_by_dept_sql(spark).collect()
    counts = Counter(row.known_for_department for row in rows)
    sizes = {row.known_for_department: row["count"]
             for row in clean_df.groupBy("known_for_department").count().collect()}
    assert counts == {dept: min(5, count) for dept, count in sizes.items()}
    for dept in counts:
        values = [row.popularity for row in rows if row.known_for_department == dept]
        expected = [row.popularity for row in clean_df.filter(F.col("known_for_department") == dept)
                    .orderBy(F.desc("popularity")).limit(5).collect()]
        assert values == expected


def test_repartition(clean_df):
    result = hw.repartition_demo(clean_df)
    assert result.rdd.getNumPartitions() == 4
    assert result.count() == clean_df.count()


def test_coalesce(clean_df):
    result = hw.coalesce_demo(clean_df)
    assert result.rdd.getNumPartitions() == 4
    assert result.count() == clean_df.count()


def test_partition_write_read(spark, clean_df, tmp_path):
    output = tmp_path / "people"
    hw.write_partitioned(clean_df, output.as_posix())
    assert (output / "known_for_department=Acting").is_dir()
    assert spark.read.parquet(output.as_posix()).count() == clean_df.count()
    acting = hw.read_single_partition(spark, output.as_posix(), "Acting")
    assert acting.count() == clean_df.filter(F.col("known_for_department") == "Acting").count()
    assert [row[0] for row in acting.select("known_for_department").distinct().collect()] == ["Acting"]
    assert hw.read_single_partition(spark, output.as_posix(), "Missing").count() == 0


def test_partition_performance_report(sample, capsys):
    report = hw.partition_performance_report(hw.clean_data(sample))
    assert set(report) == {1, 8}
    assert all(seconds > 0 for seconds in report.values())
    output = capsys.readouterr().out
    assert "repartition(1)" in output and "repartition(8)" in output


def test_gender_dept_pivot(sample):
    result = hw.gender_dept_pivot(hw.decode_gender(hw.clean_data(sample)))
    actual = {row.known_for_department: (row.Unknown, row.Female, row.Male, row.Other)
              for row in result.collect()}
    assert actual == {"Acting": (2, 1, 1, 0), "Directing": (1, 0, 0, 1)}


def test_rank_within_dept(sample):
    df = hw.clean_data(sample)
    tied = df.unionByName(df.filter(F.col("original_name") == "Alice Smith")
                         .withColumn("name", F.lit("Twin")))
    ranks = {row.name: row["rank"] for row in hw.rank_within_dept(tied).collect()}
    assert ranks["  aLICE SMITH  "] == ranks["Twin"] == 1
    assert ranks["Bob"] == 3
    assert ranks["Carol"] == 1


def test_run_full_pipeline(spark, clean_df, tmp_path):
    output = (tmp_path / "pipeline").as_posix()
    result = hw.run_full_pipeline(spark, DATA_PATH, output)
    assert result.count() == clean_df.count()
    assert {"gender_label", "popularity_tier", "rank"}.issubset(result.columns)
    restored = spark.read.parquet(output)
    assert restored.count() == result.count()
    assert set(restored.columns) == set(result.columns)
    assert spark.table("people").count() == result.count()
