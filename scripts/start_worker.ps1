#Requires -Version 5.1
<#
.SYNOPSIS
    Start one DormPool Worker agent on this machine.

.DESCRIPTION
    Joins this machine to a pool as a single node.

    Worker data is isolated per WORKER_ID (data/worker_data/<id>), so two
    workers can run on one machine for local testing.

.PARAMETER Master
    Master base URL, e.g. http://10.4.139.66:8000.
    Resolution order: -Master, $env:MASTER_URL, http://127.0.0.1:8000.

.PARAMETER WorkerId
    Stable node identity. Defaults to "<hostname>-<random 4>". Keep it fixed:
    reconnecting with the same id is treated as the same node, not a new one.

.PARAMETER MaxSlots
    Concurrent tasks this node will accept, default 1.

.PARAMETER DelayMs
    DORMPOOL_EXECUTOR_DELAY_MS — extra sleep after each image. Slows a task so
    pause/hot-join experiments have a visible window. Does not change output bytes.

.PARAMETER ControlPort
    Enable the loopback control API (/status /pause /resume) on this port.

.PARAMETER Foreground
    Run in this window (interactive REPL available) instead of backgrounding.

.EXAMPLE
    .\scripts\start_worker.ps1
.EXAMPLE
    .\scripts\start_worker.ps1 -Master http://10.4.139.66:8000 -WorkerId worker-B
.EXAMPLE
    .\scripts\start_worker.ps1 -WorkerId worker-A -DelayMs 800 -ControlPort 8122
#>
[CmdletBinding()]
param(
    [string]$Master,
    [string]$WorkerId,
    [int]$MaxSlots = 100,
    [string]$Python,
    [int]$DelayMs = 0,
    [int]$ControlPort = 0,
    [switch]$Foreground
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
. (Join-Path $PSScriptRoot 'python_runtime.ps1')
$Python = Resolve-DormPoolPython -Root $Root -Python $Python -EnvironmentNames @('.venv-gpu', '.venv')
$env:CUPY_CACHE_DIR = Join-Path $Root '._tmp\cupy-cache'
# Leave default model discovery to the workload: models/convnext_small-0c510722.pth
# is the canonical location. An explicit environment override still wins.
$env:DORMPOOL_CONVNEXT_DEVICE = 'cuda'
Write-Host "Python  : $Python"

$LogDir = Join-Path $Root "logs"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

# ── master url ───────────────────────────────────────────────────────────────
if (-not $Master) { $Master = $env:MASTER_URL }
if (-not $Master) { $Master = "http://127.0.0.1:8000" }
if ($Master -notmatch '^https?://') {
    Write-Error "Master must be a URL like http://10.4.139.66:8000 (got '$Master')."
    exit 1
}
$Master = $Master.TrimEnd('/')

# ── worker id ────────────────────────────────────────────────────────────────
if (-not $WorkerId) {
    $WorkerId = "{0}-{1}" -f $env:COMPUTERNAME, (-join ((48..57) + (97..122) | Get-Random -Count 4 | ForEach-Object { [char]$_ }))
}

$workerData = Join-Path $Root ("data/worker_data/" + $WorkerId)

# ── reachability preflight ───────────────────────────────────────────────────
try {
    $r = Invoke-WebRequest -Uri "$Master/ping" -UseBasicParsing -TimeoutSec 5
    Write-Host "Master  : $Master (HTTP $($r.StatusCode))" -ForegroundColor Green
} catch {
    $port_ = ([Uri]$Master).Port
    Write-Error ("Cannot reach " + $Master + "/ping`n" + $_.Exception.Message + "`n`nCheck that the Master is running, the host is on the same LAN, and that Windows firewall allows inbound TCP " + $port_ + " on the Master machine.")
    exit 1
}

# ── launch ───────────────────────────────────────────────────────────────────
$env:MASTER_URL     = $Master
$env:WORKER_ID      = $WorkerId
$env:WORKER_DATA    = $workerData
$env:MAX_SLOTS      = "$MaxSlots"
$env:DORMPOOL_EXECUTOR_DELAY_MS = "$DelayMs"
$env:WORKER_CONTROL_PORT = "$ControlPort"

if (-not (Test-Path $workerData)) { New-Item -ItemType Directory -Path $workerData -Force | Out-Null }

$outLog  = Join-Path $LogDir "worker-$WorkerId.out.log"
$errLog  = Join-Path $LogDir "worker-$WorkerId.err.log"
$pidFile = Join-Path $LogDir "worker-$WorkerId.pid"

Write-Host ""
Write-Host "  Worker  : $WorkerId"       -ForegroundColor Cyan
Write-Host "  Master  : $Master"         -ForegroundColor Cyan
Write-Host "  Slots   : $MaxSlots"       -ForegroundColor Cyan
Write-Host "  Data    : $workerData"     -ForegroundColor Cyan
if ($ControlPort -gt 0) { Write-Host "  Control : http://127.0.0.1:$ControlPort" -ForegroundColor Cyan }
if ($DelayMs -gt 0)     { Write-Host "  Delay   : $DelayMs ms/image" -ForegroundColor Yellow }
Write-Host ""

if ($Foreground) {
    & $Python worker/main.py
    exit $LASTEXITCODE
}

$proc = Start-Process -FilePath $Python `
    -ArgumentList @("worker/main.py", "--headless") `
    -WorkingDirectory $Root `
    -RedirectStandardOutput $outLog `
    -RedirectStandardError $errLog `
    -NoNewWindow -PassThru

$proc.Id | Set-Content -Path $pidFile -Encoding ascii
Write-Host "Started Worker as PID $($proc.Id)  (stop: .\scripts\stop.ps1 -WorkerId $WorkerId)"

Start-Sleep -Seconds 3
if ($proc.HasExited) {
    Write-Error "Worker exited immediately. Last log lines:`n$(Get-Content $errLog -Tail 20 | Out-String)"
    exit 1
}

try {
    $w = Invoke-RestMethod -Uri "$Master/workers" -TimeoutSec 5
    $me = $w.workers | Where-Object { $_.worker_id -eq $WorkerId } | Select-Object -First 1
    if ($me) {
        Write-Host ("Registered: {0} / {1} / {2}  cpu={3}c ram={4}GB gpu={5}" -f `
            $me.conn_state, $me.contrib_state, $me.exec_state, `
            $me.cpu_cores, $me.ram_gb, ($(if ($me.gpu_name) { $me.gpu_name } else { "none" }))) -ForegroundColor Green
    } else {
        Write-Warning "Worker is running but not yet listed in /workers -- check $errLog"
    }
} catch {
    Write-Warning "Could not read /workers: $($_.Exception.Message)"
}
