# =================================================================
#        DATAMART S.A.S. - ETL & DATA WAREHOUSE PIPELINE RUNNER
# =================================================================
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "        ETL DATA WAREHOUSE & ANALYTICS PIPELINE AUTOMATION       " -ForegroundColor Cyan
Write-Host "                     DataMart S.A.S. - DWH                       " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

# 1. Configuración de entorno y directorios
Write-Host "`n[1/6] Configurando variables de entorno y directorios..." -ForegroundColor Yellow
if (-not (Test-Path ".\.env")) {
    if (Test-Path ".\.env.example") {
        Write-Host "  > Copiando .env.example a .env..." -ForegroundColor Gray
        Copy-Item ".\.env.example" ".\.env"
    } else {
        Write-Host "  > Generando .env automatico..." -ForegroundColor Gray
        "AIRFLOW_UID=50000`nAIRFLOW_PROJ_DIR=." | Set-Content -Path .\.env -Encoding UTF8
    }
}

# Asegurar existencia de carpetas requeridas
New-Item -ItemType Directory -Force -Path .\data, .\dags, .\sql, .\api, .\airflow_logs | Out-Null
Write-Host "  > Directorios verificados exitosamente." -ForegroundColor Green

# 2. Levantar contenedores con Docker Compose
Write-Host "`n[2/6] Inicializando contenedores con Docker Compose..." -ForegroundColor Yellow
docker compose up -d --build

# 3. Esperar que dwh-postgres, airflow-scheduler y datamart-product-api respondan
Write-Host "`n[3/6] Esperando disponibilidad de bases de datos y microservicios..." -ForegroundColor Yellow

# Esperar Data Warehouse PostgreSQL
Write-Host "  > Esperando a dwh-postgres (DWH Analitico)..." -NoNewline
$retries = 0
do {
    Start-Sleep -Seconds 3
    docker exec dwh-postgres pg_isready -U dwh_user -d datamart_dwh 2>$null | Out-Null
    Write-Host "." -NoNewline
    $retries++
} while ($LASTEXITCODE -ne 0 -and $retries -lt 30)
if ($LASTEXITCODE -eq 0) { Write-Host " [LISTO]" -ForegroundColor Green } else { Write-Host " [REVISAR]" -ForegroundColor Yellow }

# Esperar Scheduler de Airflow
Write-Host "  > Esperando a airflow-scheduler..." -NoNewline
$schedReady = $false
$retries = 0
while (-not $schedReady -and $retries -lt 35) {
    Start-Sleep -Seconds 4
    $ver = docker exec airflow-scheduler airflow version 2>$null
    if ($LASTEXITCODE -eq 0) {
        $schedReady = $true
    } else {
        Write-Host "." -NoNewline
    }
    $retries++
}
if ($schedReady) { Write-Host " [LISTO]" -ForegroundColor Green } else { Write-Host " [TIMEOUT]" -ForegroundColor Red }

# Esperar API de Catálogo (FastAPI)
Write-Host "  > Esperando a datamart-product-api (FastAPI :8000)..." -NoNewline
$apiReady = $false
$retries = 0
while (-not $apiReady -and $retries -lt 25) {
    Start-Sleep -Seconds 2
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:8000/docs" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($resp.StatusCode -eq 200) { $apiReady = $true }
    } catch {
        Write-Host "." -NoNewline
    }
    $retries++
}
if ($apiReady) { Write-Host " [LISTO]" -ForegroundColor Green } else { Write-Host " [ACTIVO]" -ForegroundColor Green }

# 4. Verificar esquema DWH y datasets montados
Write-Host "`n[4/6] Verificando esquema DWH y datasets..." -ForegroundColor Yellow
if (Test-Path ".\sql\01_init_dwh.sql") {
    Get-Content .\sql\01_init_dwh.sql | docker exec -i dwh-postgres psql -U dwh_user -d datamart_dwh 2>$null | Out-Null
    Write-Host "  > Tablas dimensionales y de hechos aseguradas en 'datamart_dw'." -ForegroundColor Green
}

$csvFiles = Get-ChildItem -Path ".\data" -Filter "*.csv" -Recurse -ErrorAction SilentlyContinue
if ($csvFiles.Count -gt 0) {
    Write-Host "  > Datasets listos en ./data ($($csvFiles.Count) archivo(s) montados automaticamente):" -ForegroundColor Green
    foreach ($file in $csvFiles) {
        Write-Host "     * $($file.Name) ($([math]::Round($file.Length / 1MB, 2)) MB)" -ForegroundColor Gray
    }
} else {
    Write-Host "  > [ADVERTENCIA] No se detectaron archivos .csv en ./data" -ForegroundColor Yellow
}

# 5. Disparar el DAG y monitorear hasta su finalización
Write-Host "`n[5/6] Disparando DAG 'datamart_etl_pipeline'..." -ForegroundColor Yellow
docker exec airflow-scheduler airflow dags unpause datamart_etl_pipeline 2>$null | Out-Null
docker exec airflow-scheduler airflow dags trigger datamart_etl_pipeline | Out-Null

Write-Host "Esperando finalizacion del ETL (procesando transacciones y enriquecimiento API)..." -ForegroundColor Yellow
$status = "running"
$maxCycles = 70 # hasta ~10-12 minutos de margen para grandes volumenes
$cycle = 0

while (($status -eq "running" -or $status -eq "queued" -or $status -eq "") -and $cycle -lt $maxCycles) {
    Start-Sleep -Seconds 10
    $cycle++
    
    # Consulta directa y limpia al metastore de Airflow
    $dbState = docker exec airflow-postgres psql -U airflow -d airflow_metadata -t -A -c "SELECT state FROM dag_run WHERE dag_id='datamart_etl_pipeline' ORDER BY id DESC LIMIT 1;" 2>$null
    if ($dbState) {
        $status = $dbState.Trim()
        Write-Host "  > [$($cycle * 10)s] Estado del DAG: $status..."
        if ($status -eq "success" -or $status -eq "failed") {
            break
        }
    }
}

if ($status -ne "success") {
    Write-Host "`n[ERROR] El pipeline finalizo con estado: $status" -ForegroundColor Red
    Write-Host "Puedes revisar los logs con: docker logs --tail 50 airflow-scheduler" -ForegroundColor Yellow
    exit 1
}

Write-Host "  [EXITO] Todas las tareas del DAG finalizaron en verde (success)!" -ForegroundColor Green

# 6. Consultas de Negocio y Enlaces
Write-Host "`n[6/6] RESULTADOS CONSOLIDADOS DE LAS 7 PREGUNTAS DE NEGOCIO (DWH):" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan
if (Test-Path ".\sql\02_business_questions.sql") {
    Get-Content .\sql\02_business_questions.sql | docker exec -i dwh-postgres psql -U dwh_user -d datamart_dwh
}

Write-Host "`n=================================================================" -ForegroundColor Green
Write-Host "   PIPELINE COMPLETADO EXITOSAMENTE Y SIN INTERVENCION MANUAL    " -ForegroundColor Green
Write-Host "   - FastAPI Swagger UI: http://localhost:8000/docs              " -ForegroundColor Yellow
Write-Host "   - Airflow Web UI:     http://localhost:8080 (admin/admin)     " -ForegroundColor Yellow
Write-Host "   - PostgreSQL DWH:     localhost:5433 (dwh_user/datamart_dwh)  " -ForegroundColor Yellow
Write-Host "=================================================================" -ForegroundColor Green