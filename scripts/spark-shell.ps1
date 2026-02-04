# Spark shell launcher for Apilytics
# Usage: .\scripts\spark-shell.ps1 [example-name] [-SkipBuild] [-Force]
# Example: .\scripts\spark-shell.ps1 pokeapi
# Example: .\scripts\spark-shell.ps1 pokeapi -SkipBuild

param(
    [string]$Example = "pokeapi",
    [switch]$SkipBuild,
    [switch]$Force
)

$ValidExamples = @("pokeapi", "github", "countries")
if ($Example -notin $ValidExamples) {
    Write-Error "Unknown example: $Example. Available: $($ValidExamples -join ', ')"
    exit 1
}

$JarFile = "target/scala-2.13/apilytics-0.1.0-SNAPSHOT.jar"

if ($SkipBuild) {
    Write-Host "Skipping build..." -ForegroundColor Yellow
} elseif (-not $Force -and (Test-Path $JarFile)) {
    Write-Host "JAR exists, skipping build. Use -Force to rebuild." -ForegroundColor Yellow
} else {
    Write-Host "Building JAR..." -ForegroundColor Cyan
    sbt assembly
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed"
        exit 1
    }
}

# Config paths (inside container)
$Container = if ($env:SPARK_MASTER_CONTAINER) { $env:SPARK_MASTER_CONTAINER } else { "spark-master" }
$JarPath = "/opt/spark/jars/apilytics.jar"
$ConfigPath = "/opt/spark/examples/$Example/$Example-config.conf"

Write-Host ""
Write-Host "Starting spark-shell with $Example config..." -ForegroundColor Green
Write-Host "Container: $Container"
Write-Host ""

docker exec -it $Container spark-shell `
    --jars $JarPath `
    --conf "spark.sql.catalog.api=com.apilytics.spark.RESTCatalog" `
    --conf "spark.sql.catalog.api.config=$ConfigPath"
