# PySpark практикалық тапсырма


```powershell
.\run.ps1 -Test
.\run.ps1
```


## Құрылымы

```text
pyspark-homework/
├── .github/workflows/check_spark.yml
├── data/popular_people.csv
├── solution/__init__.py
├── solution/homework.py
├── tests/test_homework.py
├── pytest.ini
├── requirements.txt
└── README.md
```

## Орнату және іске қосу

```powershell
py -3.10 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:SPARK_LOCAL_IP = '127.0.0.1'
python -m pytest tests/test_homework.py -v --tb=short
python -m solution.homework --input data/popular_people.csv --output output/people
```



## 19 тапсырманың функциялары

| № | Функция | Нәтиже |
|---|---|---|
| 1 | `create_spark_session` | PopularPeopleApp SparkSession |
| 2 | `load_data` | CSV, header=True, inferSchema=True |
| 3 | `load_data_with_schema` | StructType, IntegerType, DoubleType арқылы оқу |
| 4 | `clean_data` | Бос саланы NULL-ға ауыстыру, NULL name/саланы жою |
| 5 | `decode_gender` | Unknown, Female, Male, Other мәндері бар gender_label |
| 6 | `add_popularity_tier` | A-list ≥40, B-list ≥20, C-list ≥10, қалғаны Unknown |
| 7 | `normalize_names` | trim + initcap арқылы есімді реттеу |
| 8 | `register_temp_view` | people уақытша SQL кестесі |
| 9 | `dept_stats_sql` | Саладағы адам саны, орташа және ең үлкен popularity |
| 10 | `gender_distribution_sql` | Жыныс бойынша сан және пайыз |
| 11 | `top5_by_dept_sql` | ROW_NUMBER арқылы әр саладағы TOP-5 |
| 12 | `repartition_demo` | 8 бөлік, кейін сала бойынша 4 бөлік |
| 13 | `coalesce_demo` | 16 бөлікті 4 бөлікке біріктіру |
| 14 | `write_partitioned` | Сала бойынша қалталарға Parquet жазу |
| 15 | `read_single_partition` | Берілген саланы ғана оқу |
| 16 | `partition_performance_report` | 1 және 8 партициямен жазу уақытын салыстыру |
| 17 | `gender_dept_pivot` | Сала × жыныс бойынша адам саны |
| 18 | `rank_within_dept` | Әр саладағы танымалдылық рейтингі |
| 19 | `run_full_pipeline` | Жүктеу, тазалау, талдау, демонстрациялар және сақтау |


