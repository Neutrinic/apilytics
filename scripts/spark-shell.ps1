# Spark shell launcher for Apilytics
# Usage: .\scripts\spark-shell.ps1 [example-name] [-SkipBuild] [-Force] [-Clean] [-Restart]
# Example: .\scripts\spark-shell.ps1 pokeapi
# Example: .\scripts\spark-shell.ps1 github -SkipBuild
# Example: .\scripts\spark-shell.ps1 github -Force -Clean -Restart

param(
    [string]$Example = "pokeapi",
    [switch]$SkipBuild,
    [switch]$Force,
    [switch]$Clean,
    [switch]$Restart
)

$ValidExamples = @("pokeapi", "github", "slack", "multi")
if ($Example -notin $ValidExamples) {
    Write-Error "Unknown example: $Example. Available: $($ValidExamples -join ', ')"
    exit 1
}

# Check required env vars early (before build)
if ($Example -eq "slack" -and -not $env:SLACK_BOT_TOKEN) {
    Write-Error "SLACK_BOT_TOKEN environment variable is required for Slack example"
    Write-Host "Create a Slack app at https://api.slack.com/apps" -ForegroundColor Yellow
    Write-Host 'Set it with: $env:SLACK_BOT_TOKEN = "xoxb-..."' -ForegroundColor Yellow
    exit 1
}

# Find the JAR file (handles version changes)
$JarPattern = "target/scala-2.13/apilytics-*.jar"
$JarFile = Get-ChildItem -Path $JarPattern -ErrorAction SilentlyContinue | Select-Object -First 1

if ($SkipBuild) {
    Write-Host "Skipping build..." -ForegroundColor Yellow
} elseif (-not $Force -and $JarFile) {
    Write-Host "JAR exists ($($JarFile.Name)), skipping build. Use -Force to rebuild." -ForegroundColor Yellow
} else {
    Write-Host "Building JAR..." -ForegroundColor Cyan
    if ($Clean) {
        sbt clean assembly
    } else {
        sbt assembly
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Build failed"
        exit 1
    }
    # Re-find JAR after build
    $JarFile = Get-ChildItem -Path $JarPattern -ErrorAction SilentlyContinue | Select-Object -First 1
}

# Set env var for docker compose (used in volume mount)
if ($JarFile) {
    $env:APILYTICS_JAR = $JarFile.FullName -replace '\\', '/'
    Write-Host "Using JAR: $($JarFile.Name)" -ForegroundColor Gray
} else {
    Write-Error "No JAR found matching $JarPattern"
    exit 1
}

# Restart Docker containers if requested (picks up new JAR via volume mount)
if ($Restart) {
    Write-Host "Restarting Spark containers..." -ForegroundColor Cyan
    docker compose -f docker/spark/compose.spark.yaml restart

    # Wait for spark-master to be running
    $Container = if ($env:SPARK_MASTER_CONTAINER) { $env:SPARK_MASTER_CONTAINER } else { "spark-master" }
    Write-Host "Waiting for $Container to be ready..." -ForegroundColor Yellow
    $maxAttempts = 30
    $attempt = 0
    do {
        Start-Sleep -Seconds 1
        $status = docker inspect -f '{{.State.Running}}' $Container 2>$null
        $attempt++
    } while ($status -ne 'true' -and $attempt -lt $maxAttempts)

    if ($status -ne 'true') {
        Write-Error "Container $Container failed to start"
        exit 1
    }
    Write-Host "$Container is running" -ForegroundColor Green
}

# Config paths (inside container)
$Container = if ($env:SPARK_MASTER_CONTAINER) { $env:SPARK_MASTER_CONTAINER } else { "spark-master" }
$JarPath = "/opt/spark/jars/apilytics.jar"
$ConfigPath = "/opt/spark/examples/$Example/$Example-config.conf"

Write-Host ""
Write-Host "Starting spark-shell with $Example config..." -ForegroundColor Green
Write-Host "Container: $Container"
Write-Host ""

# Example queries to try
Write-Host "Example queries:" -ForegroundColor Cyan
if ($Example -eq "pokeapi") {
    Write-Host '  spark.sql("SELECT name, url FROM api.default.pokemon LIMIT 10").show()' -ForegroundColor DarkGray
    Write-Host '  spark.sql("SELECT * FROM api.default.pokemon WHERE name = ''pikachu''").show()' -ForegroundColor DarkGray
} elseif ($Example -eq "github") {
    Write-Host '  spark.sql("SELECT number, title, state FROM api.default.issues LIMIT 10").show()' -ForegroundColor DarkGray
    Write-Host '  spark.sql("SELECT number, title FROM api.default.issues WHERE state = ''open''").show()' -ForegroundColor DarkGray
} elseif ($Example -eq "slack") {
    Write-Host '  spark.sql("SELECT id, name FROM api.default.channels LIMIT 10").show()' -ForegroundColor DarkGray
    Write-Host '  spark.sql("SELECT id, name, real_name FROM api.default.users LIMIT 10").show()' -ForegroundColor DarkGray
} elseif ($Example -eq "multi") {
    Write-Host '  -- Two catalogs: github.default.* and pokemon.default.*' -ForegroundColor DarkGray
    Write-Host '  spark.sql("SHOW TABLES IN github.default").show()' -ForegroundColor DarkGray
    Write-Host '  spark.sql("SHOW TABLES IN pokemon.default").show()' -ForegroundColor DarkGray
    Write-Host '  spark.sql("SELECT number, title FROM github.default.issues LIMIT 5").show()' -ForegroundColor DarkGray
    Write-Host '  spark.sql("SELECT name, url FROM pokemon.default.pokemon LIMIT 5").show()' -ForegroundColor DarkGray
}
Write-Host ""

# Build env var args to pass to container
$EnvArgs = @()
if ($env:GITHUB_TOKEN) { $EnvArgs += @("-e", "GITHUB_TOKEN=$env:GITHUB_TOKEN") }
if ($env:SLACK_BOT_TOKEN) { $EnvArgs += @("-e", "SLACK_BOT_TOKEN=$env:SLACK_BOT_TOKEN") }

# Multi-catalog mode: load both github and pokemon catalogs
if ($Example -eq "multi") {
    docker exec -it @EnvArgs $Container spark-shell `
        --jars $JarPath `
        --conf "spark.sql.catalog.github=com.apilytics.spark.RESTCatalog" `
        --conf "spark.sql.catalog.github.config=/opt/spark/examples/github/github-config.conf" `
        --conf "spark.sql.catalog.pokemon=com.apilytics.spark.RESTCatalog" `
        --conf "spark.sql.catalog.pokemon.config=/opt/spark/examples/pokeapi/pokeapi-config.conf"
} else {
    docker exec -it @EnvArgs $Container spark-shell `
        --jars $JarPath `
        --conf "spark.sql.catalog.api=com.apilytics.spark.RESTCatalog" `
        --conf "spark.sql.catalog.api.config=$ConfigPath"
}
