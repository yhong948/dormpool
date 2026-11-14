#Requires -Version 5.1
<#
.SYNOPSIS
    Start the DormPool Master (and its console) on this machine.

.DESCRIPTION
    Resolves the shared token, picks a bind address, preflights the port, and
    launches uvicorn in a background process. Logs and the PID file go to logs/.

    The token is stored in .dormpool-token at the repository root. Run this
    script once, then copy that file to every Worker machine (or pass -Token
    explicitly to start_worker.ps1) so both sides agree.

.PARAMETER Bind
    Address to listen on. Resolution order: -Bind, $env:MASTER_BIND, the first
    non-loopback IPv4 of this machine, then 127.0.0.1.
    A wildcard (0.0.0.0) is rejected on purpose: it exposes the pool on every
    interface and fails acceptance gate G0-5.

.PARAMETER Port
    TCP port, default 8000.

.PARAMETER Token
    Bearer token. Resolution order: -Token, $env:DORMPOOL_TOKEN,
    .dormpool-token, then a freshly generated one written to that file.

.PARAMETER DataDir
    Master data root, default "data".

.PARAMETER ShardSize
    Images per Task, default 1 (the value used by the acceptance gates; the
    production default in code is 3).

.PARAMETER Foreground
    Run in this window instead of backgrounding. Logs go straight to the
    console and Ctrl+C stops it.

.EXAMPLE
    .\scripts\start_master.ps1
.EXAMPLE
    .\scripts\start_master.ps1 -Bind 10.4.139.66 -Port 8000
.EXAMPLE
    .\scripts\start_master.ps1 -Foreground
#>
[CmdletBinding()]
param(
    [string]$Bind,
    [int]$Port = 8000,
    [string]$Token,
    [string]$DataDir = "data",
    [int]$ShardSize = 1,
    [switch]$Foreground
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$LogDir = Join-Path $Root "logs"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

# ── token ────────────────────────────────────────────────────────────────────
$TokenFile = Join-Path $Root ".dormpool-token"
if (-not $Token) { $Token = $env:DORMPOOL_TOKEN }
if (-not $Token -and (Test-Path $TokenFile)) {
    $Token = (Get-Content -Path $TokenFile -Raw).Trim()
}
if (-not $Token) {
    $bytes = New-Object byte[] 24
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $rng.GetBytes($bytes)
    $Token = [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
    Set-Content -Path $TokenFile -Value $Token -NoNewline -Encoding ascii
    Write-Host "Generated a new token -> $TokenFile" -ForegroundColor Yellow
}

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
    Write-Error ("Port {0} is already in use by PID {1}.`nStop it first:  Stop-Process -Id {1} -Force" -f $Port, $busy.OwningProcess)
    exit 1
}

# ── data dir ─────────────────────────────────────────────────────────────────
if (-not (Test-Path $DataDir)) { New-Item -ItemType Directory -Path $DataDir | Out-Null }

# ── launch ───────────────────────────────────────────────────────────────────
$env:DORMPOOL_TOKEN = $Token
$env:DORMPOOL_DATA  = $DataDir
$env:SHARD_SIZE     = "$ShardSize"
$env:MASTER_BIND    = $Bind
$env:MASTER_PORT    = "$Port"

$outLog  = Join-Path $LogDir "master.out.log"
$errLog  = Join-Path $LogDir "master.err.log"
$pidFile = Join-Path $LogDir "master.pid"

Write-Host ""
Write-Host "  Master  : http://${Bind}:${Port}"        -ForegroundColor Cyan
Write-Host "  Console : http://${Bind}:${Port}/console" -ForegroundColor Cyan
Write-Host "  Token   : $Token"                        -ForegroundColor Cyan
Write-Host "  Data    : $DataDir"                      -ForegroundColor Cyan
if (-not $Foreground) { Write-Host "  Logs    : $errLog" -ForegroundColor Cyan }
Write-Host ""

if ($Foreground) {
    python -m uvicorn master.main:app --host $Bind --port $Port
    exit $LASTEXITCODE
}

$proc = Start-Process python `
    -ArgumentList @("-m", "uvicorn", "master.main:app", "--host", $Bind, "--port", $Port) `
    -WorkingDirectory $Root `
    -RedirectStandardOutput $outLog `
    -RedirectStandardError $errLog `
    -NoNewWindow -PassThru

$proc.Id | Set-Content -Path $pidFile -Encoding ascii
Write-Host "Started Master as PID $($proc.Id)  (stop: .\scripts\stop.ps1)"

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
    Write-Warning "On a LAN address this is usually the Windows firewall; allow inbound TCP $Port."
}
