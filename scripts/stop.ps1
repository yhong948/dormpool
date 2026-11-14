#Requires -Version 5.1
<#
.SYNOPSIS
    Stop DormPool processes started by start_master.ps1 / start_worker.ps1.

.DESCRIPTION
    Reads the PID files under logs/ and stops those processes. Only touches
    processes this repository started, so an unrelated python.exe is safe.

.PARAMETER What
    master | worker | all (default: all).

.PARAMETER WorkerId
    Stop only this worker. Omitted with -What worker stops every worker.

.PARAMETER Force
    Also stop processes that no longer respond to a normal kill switch.

.EXAMPLE
    .\scripts\stop.ps1
.EXAMPLE
    .\scripts\stop.ps1 -What worker -WorkerId worker-A
#>
[CmdletBinding()]
param(
    [ValidateSet("master", "worker", "all")]
    [string]$What = "all",
    [string]$WorkerId,
    [switch]$Force
)

$ErrorActionPreference = "Continue"

$Root = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $Root "logs"

if (-not (Test-Path $LogDir)) {
    Write-Host "No logs/ directory — nothing was started by these scripts."
    exit 0
}

function Stop-One {
    param([string]$PidFile)
    $name = [System.IO.Path]::GetFileNameWithoutExtension($PidFile)
    $raw = Get-Content -Path $PidFile -Raw -ErrorAction SilentlyContinue
    if (-not $raw -or -not $raw.Trim()) {
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
        return
    }
    $procId = [int]$raw.Trim()
    $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
    if (-not $proc) {
        Write-Host "  $name (PID $procId) already gone" -ForegroundColor DarkGray
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
        return
    }
    if ($Force) {
        Stop-Process -Id $procId -Force
    } else {
        # Workers shut down cleanly on a console interrupt; a plain Stop-Process
        # is the closest equivalent available without a shared console.
        Stop-Process -Id $procId
    }
    Start-Sleep -Milliseconds 400
    if (Get-Process -Id $procId -ErrorAction SilentlyContinue) {
        Stop-Process -Id $procId -Force
        Write-Host "  $name (PID $procId) stopped forcefully" -ForegroundColor Yellow
    } else {
        Write-Host "  $name (PID $procId) stopped" -ForegroundColor Green
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

Write-Host "Stopping DormPool processes..."

if ($What -in @("master", "all")) {
    $f = Join-Path $LogDir "master.pid"
    if (Test-Path $f) { Stop-One $f }
}

if ($What -in @("worker", "all")) {
    if ($WorkerId) {
        $f = Join-Path $LogDir "worker-$WorkerId.pid"
        if (Test-Path $f) { Stop-One $f } else { Write-Host "  no pid file for $WorkerId" -ForegroundColor DarkGray }
    } else {
        Get-ChildItem -Path $LogDir -Filter "worker-*.pid" -ErrorAction SilentlyContinue |
            ForEach-Object { Stop-One $_.FullName }
    }
}

$leftover = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue
if ($leftover) {
    Write-Warning "Port 8000 is still held by PID $($leftover.OwningProcess) — not started by these scripts."
}

Write-Host "Done."
