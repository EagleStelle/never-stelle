@echo off
rem Self-contained launcher: batch header below boots the embedded PowerShell
rem script that follows the marker line. No separate .ps1 needed.
setlocal
set "NS_REPO_ROOT=%~dp0"
set "PS=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
set "NS_TMP=%TEMP%\never-stelle-run.ps1"
rem Stage 1: carve the embedded PowerShell (everything past the marker) to a temp file.
"%PS%" -NoProfile -ExecutionPolicy Bypass -Command "(((Get-Content -Raw -LiteralPath '%~f0') -split ('#PS'+'MARK'),2)[1]) | Set-Content -LiteralPath $env:NS_TMP -Encoding UTF8"
if errorlevel 1 exit /b %errorlevel%
rem Stage 2: run it as a real script so -Port/-HostAddress/-Reinstall bind natively.
"%PS%" -NoProfile -ExecutionPolicy Bypass -File "%NS_TMP%" %*
set "RC=%ERRORLEVEL%"
del "%NS_TMP%" >nul 2>&1
exit /b %RC%
#PSMARK
[CmdletBinding()]
param(
    [int]$Port = 8840,
    [int]$FrontendPort = 5173,
    [string]$HostAddress = "127.0.0.1",
    [switch]$Reinstall,
    [switch]$Prod
)

$Dev = -not $Prod
$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path -LiteralPath $env:NS_REPO_ROOT).Path
Set-Location -LiteralPath $RepoRoot

$LocalDir = Join-Path $RepoRoot ".local"
# Role dirs mirror the container layout (/data /media /scratch), kept under .local on Windows.
$DataDir = Join-Path $LocalDir "data"
$MediaDir = Join-Path $LocalDir "media"
$ScratchDir = Join-Path $LocalDir "scratch"
$BuildDir = Join-Path $LocalDir "build"   # tooling that must survive scratch wipes
$LogDir = Join-Path $LocalDir "logs"

$VenvDir = Join-Path $BuildDir ".venv"
$DatabasePath = Join-Path $DataDir "never-stelle.sqlite3"
$TempDir = $ScratchDir
$PipCacheDir = Join-Path $BuildDir "pip-cache"
$NpmCacheDir = Join-Path $BuildDir "npm-cache"
$FrontendWorkDir = Join-Path $BuildDir "frontend-work"
$FrontendDistDir = Join-Path $DataDir "frontend-dist"

foreach ($Path in @($LocalDir, $DataDir, $MediaDir, $ScratchDir, $BuildDir, $LogDir, $TempDir, $PipCacheDir, $NpmCacheDir, $FrontendWorkDir, $FrontendDistDir)) {
    New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

function Assert-UnderLocal {
    param([string]$Path)
    $FullLocal = [System.IO.Path]::GetFullPath($LocalDir)
    $FullPath = [System.IO.Path]::GetFullPath($Path)
    if (-not $FullPath.StartsWith($FullLocal, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to modify path outside .local: $FullPath"
    }
}

function Get-ListenProcessDescription {
    param([int]$Port)

    try {
        $Connection = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop | Select-Object -First 1
    } catch {
        $Connection = $null
    }

    if (-not $Connection) {
        return ""
    }

    $Owner = Get-Process -Id $Connection.OwningProcess -ErrorAction SilentlyContinue
    if ($Owner) {
        return "PID $($Owner.Id) ($($Owner.ProcessName))"
    }
    return "PID $($Connection.OwningProcess)"
}

function Stop-ProcessTree {
    param([int]$RootProcessId)

    $Children = Get-CimInstance Win32_Process -Filter "ParentProcessId = $RootProcessId" -ErrorAction SilentlyContinue
    foreach ($Child in $Children) {
        Stop-ProcessTree -RootProcessId ([int]$Child.ProcessId)
    }

    $Process = Get-Process -Id $RootProcessId -ErrorAction SilentlyContinue
    if ($Process) {
        Stop-Process -Id $RootProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Show-LogTail {
    param(
        [string]$Path,
        [int]$Lines = 25
    )

    if (Test-Path -LiteralPath $Path) {
        Write-Host ""
        Write-Host "$Path"
        Get-Content -LiteralPath $Path -Tail $Lines
    }
}

$PythonExe = Join-Path $VenvDir "Scripts\python.exe"
if (-not (Test-Path -LiteralPath $PythonExe)) {
    Write-Host "Creating virtual environment in .local\build\.venv..."
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
$RequirementsStamp = Join-Path $BuildDir "requirements.sha256"
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

$FrontendSourceDir = Join-Path $RepoRoot "frontend"
$Node = Get-Command node -ErrorAction SilentlyContinue
$Npm = Get-Command npm -ErrorAction SilentlyContinue
if (-not $Node -or -not $Npm) {
    throw "Node.js and npm are required to build the Vue frontend. Install Node.js ^20.19.0 or >=22.12.0."
}

$FrontendProcess = $null
$FrontendOutLog = Join-Path $LogDir "frontend-dev.out.log"
$FrontendErrLog = Join-Path $LogDir "frontend-dev.err.log"

if ($Dev) {
    $FrontendDevNodeModules = Join-Path $FrontendSourceDir "node_modules"
    if (-not (Test-Path -LiteralPath $FrontendDevNodeModules)) {
        Write-Host "Installing frontend dependencies for development..."
        & $Npm.Source install --prefix $FrontendSourceDir
        if ($LASTEXITCODE -ne 0) {
            throw "Frontend dev dependency installation failed."
        }
    }

    $FrontendPortOwner = Get-ListenProcessDescription -Port $FrontendPort
    if ($FrontendPortOwner) {
        throw "Frontend port $FrontendPort is already in use by $FrontendPortOwner. Stop that process or run .\run.cmd -FrontendPort <port>."
    }

    $ApiTargetHost = if ($HostAddress -eq "0.0.0.0" -or $HostAddress -eq "::") { "127.0.0.1" } else { $HostAddress }
    $ApiTarget = "http://${ApiTargetHost}:$Port"

    Write-Host "Starting Vue frontend dev server..."
    $FrontendCommand = "title Frontend Dev Server && set `"VITE_API_TARGET=$ApiTarget`" && set `"VITE_DEV_HOST=$HostAddress`" && set `"VITE_DEV_PORT=$FrontendPort`" && npm --prefix `"$FrontendSourceDir`" run dev -- --host `"$HostAddress`" --port $FrontendPort --strictPort"
    $FrontendProcess = Start-Process -FilePath cmd.exe -ArgumentList "/d", "/s", "/c", $FrontendCommand -WorkingDirectory $FrontendSourceDir -PassThru
    Start-Sleep -Seconds 2
    if ($FrontendProcess.HasExited) {
        throw "Vue frontend dev server exited early."
    }
} else {
    foreach ($Name in @("src", "public")) {
        $Target = Join-Path $FrontendWorkDir $Name
        if (Test-Path -LiteralPath $Target) {
            Assert-UnderLocal $Target
            Remove-Item -LiteralPath $Target -Recurse -Force
        }
    }

    foreach ($Name in @("package.json", "package-lock.json", "tsconfig.json", "vite.config.ts", "index.html", "src", "public")) {
        $Source = Join-Path $FrontendSourceDir $Name
        if (Test-Path -LiteralPath $Source) {
            Copy-Item -LiteralPath $Source -Destination $FrontendWorkDir -Recurse -Force
        }
    }

    $FrontendLockPath = Join-Path $FrontendSourceDir "package-lock.json"
    $FrontendNodeModules = Join-Path $FrontendWorkDir "node_modules"
    $FrontendStamp = Join-Path $BuildDir "frontend-package-lock.sha256"
    $FrontendLockHash = (Get-FileHash -LiteralPath $FrontendLockPath -Algorithm SHA256).Hash
    $InstalledFrontendHash = if (Test-Path -LiteralPath $FrontendStamp) {
        (Get-Content -LiteralPath $FrontendStamp -Raw).Trim()
    } else {
        ""
    }

    if ($Reinstall -or $InstalledFrontendHash -ne $FrontendLockHash -or -not (Test-Path -LiteralPath $FrontendNodeModules)) {
        Write-Host "Installing frontend dependencies..."
        & $Npm.Source install --prefix $FrontendWorkDir --cache $NpmCacheDir
        if ($LASTEXITCODE -ne 0) {
            throw "Frontend dependency installation failed."
        }
        Set-Content -LiteralPath $FrontendStamp -Value $FrontendLockHash -Encoding ascii
    }

    Write-Host "Building Vue frontend..."
    $env:FRONTEND_OUT_DIR = $FrontendDistDir
    & $Npm.Source --prefix $FrontendWorkDir run build
    if ($LASTEXITCODE -ne 0) {
        throw "Vue frontend build failed."
    }
    Remove-Item Env:\FRONTEND_OUT_DIR -ErrorAction SilentlyContinue
}

$env:PYTHONPATH = $RepoRoot
$env:PYTHONDONTWRITEBYTECODE = "1"
$env:PYTHONUNBUFFERED = "1"
$env:APP_DATA_DIR = $DataDir
$env:APP_MEDIA_DIR = $MediaDir
$env:APP_SCRATCH_DIR = $ScratchDir
$env:FRONTEND_DIR = $FrontendDistDir
$env:TEMP = $TempDir
$env:TMP = $TempDir
$env:TMPDIR = $TempDir

Write-Host ""
Write-Host "Never Stelle"
if ($Dev) {
    Write-Host "  Mode:     Development (Hot Reload)"
    Write-Host "  URL:      http://${HostAddress}:$FrontendPort"
    Write-Host "  API URL:  http://${HostAddress}:$Port"
} else {
    Write-Host "  URL:      http://${HostAddress}:$Port"
}
Write-Host "  Data:     $DataDir"
Write-Host "  Media:    $MediaDir"
Write-Host "  Scratch:  $ScratchDir"
Write-Host "  Database: $DatabasePath"
Write-Host "  Frontend: $FrontendDistDir"
Write-Host ""
Write-Host "Press Ctrl+C to stop."
Write-Host ""

try {
    if ($Dev) {
        & $PythonExe -m uvicorn backend.app.main:app --host $HostAddress --port $Port --reload
    } else {
        & $PythonExe -m uvicorn backend.app.main:app --host $HostAddress --port $Port
    }
} finally {
    if ($FrontendProcess -and -not $FrontendProcess.HasExited) {
        Stop-ProcessTree -RootProcessId $FrontendProcess.Id
    }
}
