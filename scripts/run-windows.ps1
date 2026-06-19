[CmdletBinding()]
param(
    [int]$Port = 8088,
    [string]$HostAddress = "127.0.0.1",
    [switch]$Reinstall
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
Set-Location -LiteralPath $RepoRoot

$LocalDir = Join-Path $RepoRoot ".local"
$VenvDir = Join-Path $LocalDir ".venv"
$DatabasePath = Join-Path $LocalDir "never-stelle.sqlite3"
$LibraryDir = Join-Path $LocalDir "library"
$TempDir = Join-Path $LocalDir "tmp"
$PipCacheDir = Join-Path $LocalDir "pip-cache"

foreach ($Path in @($LocalDir, $LibraryDir, $TempDir, $PipCacheDir)) {
    New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

$PythonExe = Join-Path $VenvDir "Scripts\python.exe"
if (-not (Test-Path -LiteralPath $PythonExe)) {
    Write-Host "Creating virtual environment in .local\.venv..."
    $PyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($PyLauncher) {
        & py -3 -m venv $VenvDir
    } else {
        $Python = Get-Command python -ErrorAction Stop
        & $Python.Source -m venv $VenvDir
    }
}

$RequirementsPath = Join-Path $RepoRoot "requirements.txt"
$RequirementsHash = (Get-FileHash -LiteralPath $RequirementsPath -Algorithm SHA256).Hash
$RequirementsStamp = Join-Path $LocalDir "requirements.sha256"
$InstalledHash = if (Test-Path -LiteralPath $RequirementsStamp) {
    (Get-Content -LiteralPath $RequirementsStamp -Raw).Trim()
} else {
    ""
}

$env:PIP_CACHE_DIR = $PipCacheDir
if ($Reinstall -or $InstalledHash -ne $RequirementsHash) {
    Write-Host "Installing Python dependencies..."
    & $PythonExe -m pip install --upgrade pip
    & $PythonExe -m pip install -r $RequirementsPath
    Set-Content -LiteralPath $RequirementsStamp -Value $RequirementsHash -Encoding ascii
}

$env:APP_DATA_DIR = $LocalDir
$env:APP_DATABASE_PATH = $DatabasePath
$env:FRONTEND_DIR = Join-Path $RepoRoot "frontend"
$env:ACCESSIBLE_VOLUMES_ROOTS = $LibraryDir
$env:PYTHONPATH = $RepoRoot
$env:PYTHONDONTWRITEBYTECODE = "1"
$env:PYTHONUNBUFFERED = "1"
$env:TEMP = $TempDir
$env:TMP = $TempDir

Write-Host ""
Write-Host "Never Stelle"
Write-Host "  URL:      http://${HostAddress}:$Port"
Write-Host "  Runtime:  $LocalDir"
Write-Host "  Database: $DatabasePath"
Write-Host "  Library:  $LibraryDir"
Write-Host ""
Write-Host "Press Ctrl+C to stop."
Write-Host ""

& $PythonExe -m uvicorn backend.app.main:app --host $HostAddress --port $Port
