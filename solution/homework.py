"""popular_people.csv бойынша 19 практикалық тапсырма.

Іске қосу: python -m solution.homework --input data/popular_people.csv
"""

import argparse
from pathlib import Path
import tempfile
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)


# №1: SparkSession
def create_spark_session():
    return (SparkSession.builder.master("local[2]")
            .appName("PopularPeopleApp")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())


# №2: Схеманы автоматты анықтап оқу
def load_data(spark, path):
    return spark.read.csv(str(path), header=True, inferSchema=True)


# №3: Схеманы қолмен анықтап оқу
def load_data_with_schema(spark, path):
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("gender", IntegerType(), True),
        StructField("known_for_department", StringType(), True),
        StructField("original_name", StringType(), True),
        StructField("popularity", DoubleType(), True),
    ])
    return spark.read.csv(str(path), header=True, inferSchema=False, schema=schema)


# №4: Бос саланы NULL-ға айналдырып, қажет мәндері жоқ жолдарды жою
def clean_data(df):
    cleaned = df.withColumn(
        "known_for_department",
        F.when(F.col("known_for_department") == "", F.lit(None))
        .otherwise(F.col("known_for_department")),
    )
    return cleaned.na.drop(subset=["known_for_department", "name"])


# №5: Сандық код бастапқы gender бағанында сақталады
def decode_gender(df):
    return df.withColumn(
        "gender_label",
        F.when(F.col("gender") == 0, "Unknown")
        .when(F.col("gender") == 1, "Female")
        .when(F.col("gender") == 2, "Male")
        .when(F.col("gender") == 3, "Other")
        .otherwise("Unknown"),
    )


# №6: Танымалдылық деңгейі
def add_popularity_tier(df):
    return df.withColumn(
        "popularity_tier",
        F.when(F.col("popularity") >= 40, "A-list")
        .when(F.col("popularity") >= 20, "B-list")
        .when(F.col("popularity") >= 10, "C-list")
        .otherwise("Unknown"),
    )


# №7: Әр сөздің бірінші әрпін бас әріпке айналдыру
def normalize_names(df):
    return df.withColumn("name", F.initcap(F.trim(F.col("name"))))


# №8: SQL үшін уақытша кесте
def register_temp_view(df):
    df.createOrReplaceTempView("people")


# №9: Салалар бойынша статистика
def dept_stats_sql(spark):
    return spark.sql("""
        SELECT known_for_department,
               COUNT(*) AS person_count,
               ROUND(AVG(popularity), 2) AS avg_popularity,
               MAX(popularity) AS max_popularity
        FROM people
        GROUP BY known_for_department
        ORDER BY avg_popularity DESC, known_for_department ASC
    """)


# №10: Әр жыныстың барлық тазаланған адамдар ішіндегі үлесі
def gender_distribution_sql(spark):
    return spark.sql("""
        SELECT gender_label, COUNT(*) AS count,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
        FROM people
        GROUP BY gender_label
        ORDER BY count DESC, gender_label ASC
    """)


# №11: ROW_NUMBER әр саладан ең көбі 5 адам береді
def top5_by_dept_sql(spark):
    return spark.sql("""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY known_for_department
                ORDER BY popularity DESC, name ASC, original_name ASC
            ) AS row_number
            FROM people
        ) ranked
        WHERE row_number <= 5
        ORDER BY known_for_department, row_number
    """)


# №12: Толық shuffle арқылы қайта бөлу
def repartition_demo(df):
    print(f"Initial partitions: {df.rdd.getNumPartitions()}")
    eight = df.repartition(8)
    print(f"After repartition(8): {eight.rdd.getNumPartitions()}")
    by_dept = eight.repartition(4, "known_for_department")
    print(f"After repartition(4, department): {by_dept.rdd.getNumPartitions()}")
    return by_dept


# №13: Партиция санын азайту
def coalesce_demo(df):
    sixteen = df.repartition(16)
    # coalesce(4) бар партицияларды толық shuffle жасамай біріктіреді.
    # Сондықтан санын азайтқанда repartition-ға қарағанда шығыны аз.
    four = sixteen.coalesce(4)
    print(f"Coalesce: {sixteen.rdd.getNumPartitions()} -> {four.rdd.getNumPartitions()}")
    return four


# №14: Parquet файлдарын салаға сәйкес қалталарға жазу
def write_partitioned(df, output_path):
    (df.write.mode("overwrite").partitionBy("known_for_department")
     .parquet(str(output_path)))


# №15: Түбірден оқу partition бағанын сақтайды; filter partition pruning береді
def read_single_partition(spark, output_path, dept):
    return (spark.read.parquet(str(output_path))
            .filter(F.col("known_for_department") == dept))


# №16: Екі жазудың нақты орындалу уақытын өлшеу
def partition_performance_report(df):
    # Уақытша қалта қайталанған іске қосуларда файл қақтығысын болдырмайды.
    # Бұл шағын тәжірибе: 8 партиция міндетті түрде жылдамырақ болмайды.
    timings = {}
    with tempfile.TemporaryDirectory(prefix="people_benchmark_") as tmp:
        for partitions in (1, 8):
            output = (Path(tmp) / f"partitions_{partitions}").as_posix()
            started = time.perf_counter()
            df.repartition(partitions).write.mode("overwrite").parquet(output)
            timings[partitions] = time.perf_counter() - started
            print(f"repartition({partitions}): {timings[partitions]:.3f} seconds")
    print(f"Difference (1 minus 8): {timings[1] - timings[8]:.3f} seconds")
    return timings


# №17: Сала бойынша жолдар, жыныс бойынша бағандар
def gender_dept_pivot(df):
    return (df.groupBy("known_for_department")
            .pivot("gender_label", ["Unknown", "Female", "Male", "Other"])
            .count().fillna(0).orderBy("known_for_department"))


# №18: Бірдей танымалдылыққа бірдей rank беріледі
def rank_within_dept(df):
    window = Window.partitionBy("known_for_department").orderBy(F.desc("popularity"))
    return df.withColumn("rank", F.rank().over(window))


# №19: Толық өңдеу pipeline
def run_full_pipeline(spark, input_path, output_path):
    raw = load_data(spark, input_path)
    # Қолмен берілген схемамен оқу мүмкіндігін де тексеру.
    load_data_with_schema(spark, input_path)
    result = normalize_names(add_popularity_tier(decode_gender(clean_data(raw))))
    result = rank_within_dept(result)
    register_temp_view(result)
    dept_stats_sql(spark).show(truncate=False)
    gender_distribution_sql(spark).show(truncate=False)
    top5_by_dept_sql(spark).show(100, truncate=False)
    gender_dept_pivot(result).show(truncate=False)
    repartition_demo(result)
    compact = coalesce_demo(result)
    write_partitioned(compact, output_path)
    print(f"Acting partition rows: {read_single_partition(spark, output_path, 'Acting').count()}")
    partition_performance_report(result)
    return result


def main():
    parser = argparse.ArgumentParser(description="Popular people PySpark homework")
    parser.add_argument("--input", default="data/popular_people.csv")
    parser.add_argument("--output", default="output/people")
    args = parser.parse_args()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = run_full_pipeline(spark, args.input, args.output)
        print(f"Final row count: {result.count()}")
        result.show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
