# Shared interpreter discovery for Windows launchers.
function Resolve-DormPoolPython {
    param(
        [string]$Root,
        [string]$Python,
        [string[]]$EnvironmentNames = @('.venv', '.venv-gpu')
    )

    if ($Python) {
        if (Test-Path -LiteralPath $Python -PathType Leaf) {
            return (Resolve-Path -LiteralPath $Python).Path
        }
        $command = Get-Command -Name $Python -CommandType Application -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if ($command) { return $command.Source }
        throw "Python interpreter not found: $Python. Pass -Python with an existing python.exe path."
    }

    foreach ($name in $EnvironmentNames) {
        $candidate = Join-Path $Root "$name\Scripts\python.exe"
        if (Test-Path -LiteralPath $candidate -PathType Leaf) { return $candidate }
    }
    if ($env:VIRTUAL_ENV) {
        $candidate = Join-Path $env:VIRTUAL_ENV 'Scripts\python.exe'
        if (Test-Path -LiteralPath $candidate -PathType Leaf) { return $candidate }
    }
    $command = Get-Command -Name python.exe -CommandType Application -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($command) { return $command.Source }

    throw "No Python interpreter found. Install Python and run 'python -m venv .venv', then '.\.venv\Scripts\python.exe -m pip install -r requirements.txt', or pass -Python with an existing python.exe path. GPU workers also need compatible GPU dependencies and local model weights; see the GPU deployment guide in docs/."
}
