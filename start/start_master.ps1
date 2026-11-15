#Requires -Version 5.1
<#
.SYNOPSIS
    Start the DormPool Master (and its console) on this machine.

.DESCRIPTION
    Picks a bind address, preflights the port, and
    launches uvicorn in a background process. Logs and the PID file go to logs/.


.PARAMETER Bind
    Address to listen on. Resolution order: -Bind, $env:MASTER_BIND, the first
    non-loopback IPv4 of this machine, then 127.0.0.1.
    A wildcard (0.0.0.0) is rejected on purpose: it exposes the pool on every
    interface and fails acceptance gate G0-5.

.PARAMETER Port
    TCP port, default 8000.

.PARAMETER Python
    Python executable path or command. Defaults to .venv, .venv-gpu,
    the active virtual environment, then python.exe on PATH.

.PARAMETER DataDir
    Master data root, default "data".

.PARAMETER ShardSize
    Images per Task, default 1 (the value used by the acceptance gates; the
    production default in code is 3).

.PARAMETER Foreground
    Run in this window instead of backgrounding. Logs go straight to the
    console and Ctrl+C stops it.

.EXAMPLE
    .\start\start_master.ps1
.EXAMPLE
    .\start\start_master.ps1 -Bind 10.4.139.66 -Port 8000
.EXAMPLE
    .\start\start_master.ps1 -Foreground
#>
[CmdletBinding()]
param(
    [string]$Bind,
    [int]$Port = 8000,
    [string]$Python,
    [string]$DataDir = "data",
    [int]$ShardSize = 50,
    [switch]$KillIfBusy,
    [switch]$Foreground
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
. (Join-Path $PSScriptRoot 'python_runtime.ps1')
$Python = Resolve-DormPoolPython -Root $Root -Python $Python
$ProjectParent = Split-Path -Parent $Root
$pathParts = @($ProjectParent)
if ($env:PYTHONPATH) { $pathParts += $env:PYTHONPATH }
$env:PYTHONPATH = ($pathParts -join [IO.Path]::PathSeparator)
Write-Host "Python  : $Python"

$LogDir = Join-Path $Root "logs"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

# ── bind address ─────────────────────────────────────────────────────────────
if (-not $Bind) { $Bind = $env:MASTER_BIND }
if (-not $Bind) {
    $addr = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
        Where-Object {
            $_.InterfaceAlias -notmatch 'Loopback' -and
            $_.IPAddress -notmatch '^(127\.|169\.254\.)' -and
            $_.IPAddress -ne '0.0.0.0'
        } | Select-Object -First 1
    if ($addr) { $Bind = $addr.IPAddress } else { $Bind = "127.0.0.1" }
}
if ($Bind -in @("0.0.0.0", "::", "*")) {
    Write-Error "Bind must be a specific address (e.g. 10.4.139.66), not '$Bind'."
    exit 1
}

# ── port preflight ───────────────────────────────────────────────────────────
$busy = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
if ($busy) {
    $pid_ = $busy.OwningProcess
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId=$pid_" -ErrorAction SilentlyContinue
    $cmd  = if ($proc) { $proc.CommandLine } else { "unknown process" }

    if ($KillIfBusy) {
        Write-Host "Port $Port is in use by PID $pid_ -- stopping it..." -ForegroundColor Yellow
        Write-Host "  $cmd" -ForegroundColor DarkGray
        Stop-Process -Id $pid_ -Force -ErrorAction SilentlyContinue
        Start-Sleep -Milliseconds 800
        $still = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
        if ($still) {
            Write-Error "Port $Port is still in use after stopping PID $pid_."
            exit 1
        }
        Write-Host "Port $Port is now free." -ForegroundColor Green
    } else {
        Write-Host ""
        Write-Host "  Port $Port is already in use:" -ForegroundColor Red
        Write-Host "  PID $pid_  :  $cmd" -ForegroundColor Red
        Write-Host ""
        Write-Host "  Option 1 (recommended): pass -KillIfBusy to stop it automatically:" -ForegroundColor Yellow
        Write-Host "    .\start\start_master.ps1 -KillIfBusy" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "  Option 2: stop it manually, then re-run:" -ForegroundColor Yellow
        Write-Host ("    Stop-Process -Id " + $pid_ + " -Force") -ForegroundColor Cyan
        Write-Host ""
        Write-Host "  Option 3: use a different port (update Worker and browser too):" -ForegroundColor Yellow
        Write-Host "    .\start\start_master.ps1 -Port 8001" -ForegroundColor Cyan
        Write-Host ""
        exit 1
    }
}

# ── data dir ─────────────────────────────────────────────────────────────────
if (-not (Test-Path $DataDir)) { New-Item -ItemType Directory -Path $DataDir | Out-Null }

# ── launch ───────────────────────────────────────────────────────────────────
$env:DORMPOOL_DATA  = [IO.Path]::GetFullPath($DataDir)
$env:SHARD_SIZE     = "$ShardSize"
$env:MASTER_BIND    = $Bind
$env:MASTER_PORT    = "$Port"

$outLog  = Join-Path $LogDir "master.out.log"
$errLog  = Join-Path $LogDir "master.err.log"
$pidFile = Join-Path $LogDir "master.pid"

Write-Host ""
Write-Host "  Master  : http://${Bind}:${Port}"        -ForegroundColor Cyan
Write-Host "  Console : http://${Bind}:${Port}/console" -ForegroundColor Cyan
Write-Host "  Data    : $DataDir"                      -ForegroundColor Cyan
if (-not $Foreground) { Write-Host "  Logs    : $errLog" -ForegroundColor Cyan }
Write-Host ""

if ($Foreground) {
    & $Python -m uvicorn src.master.main:app --host $Bind --port $Port
    exit $LASTEXITCODE
}

$proc = Start-Process -FilePath $Python `
    -ArgumentList @("-m", "uvicorn", "src.master.main:app", "--host", $Bind, "--port", $Port) `
    -WorkingDirectory $Root `
    -RedirectStandardOutput $outLog `
    -RedirectStandardError $errLog `
    -NoNewWindow -PassThru

$proc.Id | Set-Content -Path $pidFile -Encoding ascii
Write-Host "Started Master as PID $($proc.Id)  (stop: .\start\stop.ps1)"

Start-Sleep -Seconds 2
if ($proc.HasExited) {
    Write-Error "Master exited immediately. Last log lines:`n$(Get-Content $errLog -Tail 20 | Out-String)"
    exit 1
}

try {
    $r = Invoke-WebRequest -Uri "http://${Bind}:${Port}/ping" -UseBasicParsing -TimeoutSec 5
    Write-Host "Health  : HTTP $($r.StatusCode) $($r.Content)" -ForegroundColor Green
} catch {
    Write-Warning "Ping failed: $($_.Exception.Message)"
    Write-Warning ("On a LAN address this is usually the Windows firewall; allow inbound TCP " + $Port + ".")
}
