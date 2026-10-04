param([switch]$Test)

$ErrorActionPreference = 'Stop'
$taskProject = $PSScriptRoot
$taskRuntime = Join-Path (Split-Path $taskProject -Parent) 'work\pyspark-runtime'
$taskLocalPython = Join-Path $taskProject '.venv\Scripts\python.exe'
$taskPreparedPython = Join-Path $taskRuntime 'venv\Scripts\python.exe'

if (Test-Path -LiteralPath $taskLocalPython) {
    $taskPython = $taskLocalPython
} elseif (Test-Path -LiteralPath $taskPreparedPython) {
    $taskPython = $taskPreparedPython
} else {
    $taskPython = (Get-Command python -ErrorAction Stop).Source
}

$taskJavaRoot = Join-Path $taskRuntime 'java11'
if (Test-Path -LiteralPath $taskJavaRoot) {
    $taskJava = Get-ChildItem -LiteralPath $taskJavaRoot -Directory | Select-Object -First 1
    $env:JAVA_HOME = $taskJava.FullName
}
$taskHadoop = Join-Path $taskRuntime 'hadoop'
if (Test-Path -LiteralPath $taskHadoop) {
    $env:HADOOP_HOME = $taskHadoop
    $env:PATH = "$taskHadoop\bin;$env:PATH"
    $env:JAVA_TOOL_OPTIONS = "$env:JAVA_TOOL_OPTIONS `"-Djava.library.path=$taskHadoop\bin`""
}
if ($env:JAVA_HOME) {
    $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
}
$env:PYSPARK_PYTHON = $taskPython
$env:PYTHONIOENCODING = 'utf-8'
$env:SPARK_LOCAL_IP = '127.0.0.1'

Push-Location -LiteralPath $taskProject
try {
    if ($Test) {
        & $taskPython -m pytest tests/test_homework.py -v --tb=short
    } else {
        & $taskPython -m solution.homework --input data/popular_people.csv --output output/people
    }
    if ($LASTEXITCODE -ne 0) { throw "Python exited with code $LASTEXITCODE" }
} finally {
    Pop-Location
}
