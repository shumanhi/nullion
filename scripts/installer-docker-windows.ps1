param(
    [string]$AppRepo = $env:NULLION_APP_REPO,
    [string]$ImageName = "nullion-installer-smoke:windows"
)

$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
if (-not $AppRepo) {
    $AppRepo = $RootDir
}
if (-not (Test-Path $AppRepo)) {
    Write-Error "Nullion app checkout not found: $AppRepo. Set NULLION_APP_REPO to an absolute app checkout path."
}

$AppRepoHost = (Resolve-Path $AppRepo).Path
Set-Location $RootDir

function Wait-DockerDaemon {
    param([int]$TimeoutSeconds = 60)

    try {
        $service = Get-Service -Name docker -ErrorAction SilentlyContinue
        if ($service -and $service.Status -ne "Running") {
            Start-Service docker
        }
    } catch {
        Write-Warning "Could not start Docker service: $($_.Exception.Message)"
    }

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        docker version --format "{{.Server.Version}}" *> $null
        if ($LASTEXITCODE -eq 0) {
            return
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)

    Write-Error "Docker daemon did not become available within ${TimeoutSeconds}s."
}

Wait-DockerDaemon

docker build -f docker/installer-windows.Dockerfile -t $ImageName .
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$containerCommand = @'
$ErrorActionPreference = "Stop"
$env:NULLION_VERSION = "main"
$env:NULLION_INSTALLER_SMOKE = "true"
New-Item -ItemType Directory -Force C:\tmp\nullion-work | Out-Null
New-Item -ItemType Directory -Force C:\tmp\nullion-keyring | Out-Null
@"
import json
import os
from pathlib import Path

from keyring.backend import KeyringBackend


class FileKeyring(KeyringBackend):
    priority = 1

    @property
    def _path(self):
        return Path(os.environ.get("NULLION_INSTALLER_KEYRING_FILE", r"C:\tmp\nullion-keyring\store.json"))

    def get_password(self, service, username):
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except OSError:
            return None
        return data.get(f"{service}\0{username}")

    def set_password(self, service, username, password):
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except OSError:
            data = {}
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data[f"{service}\0{username}"] = password
        self._path.write_text(json.dumps(data), encoding="utf-8")

    def delete_password(self, service, username):
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except OSError:
            return
        data.pop(f"{service}\0{username}", None)
        self._path.write_text(json.dumps(data), encoding="utf-8")
"@ | Set-Content -Path C:\tmp\nullion-keyring\installer_smoke_keyring.py -Encoding UTF8
$env:PYTHONPATH = "C:\tmp\nullion-keyring;$env:PYTHONPATH"
$env:PYTHON_KEYRING_BACKEND = "installer_smoke_keyring.FileKeyring"
$env:NULLION_INSTALLER_KEYRING_FILE = "C:\tmp\nullion-keyring\store.json"
Copy-Item -Recurse C:\workspace\nullion C:\tmp\nullion-work\nullion-src
Set-Location C:\tmp\nullion-work\nullion-src

function Invoke-InstallerSmokeAnswers {
    param(
        [string]$OpenAnswer = "y",
        [string]$KeyStorageAnswer = "n"
    )

$answers = @(
  "y",
  $KeyStorageAnswer,
  "4",
  "5",
  "",
  "4",
  "2",
  "4",
  "n",
  "n",
  "n",
  $OpenAnswer
) -join "`r`n"
$answers + "`r`n" | powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\install.ps1
}

function Assert-InstalledEntrypoints {
param([string]$ExpectedKeyStorage = "local")
$installDir = Join-Path $env:USERPROFILE ".nullion"
$venvScripts = Join-Path $installDir "venv\Scripts"
$envFile = Join-Path $installDir ".env"
if (-not (Test-Path (Join-Path $venvScripts "nullion.exe"))) { throw "Missing nullion.exe" }
if (-not (Test-Path (Join-Path $venvScripts "nullion-web.exe"))) { throw "Missing nullion-web.exe" }
if (-not (Test-Path (Join-Path $venvScripts "nullion-tray.exe"))) { throw "Missing nullion-tray.exe" }
if (-not (Test-Path (Join-Path $venvScripts "nullion-webview.exe"))) { throw "Missing nullion-webview.exe" }
if (-not (Test-Path (Join-Path $venvScripts "nullion-telegram.exe"))) { throw "Missing nullion-telegram.exe" }
if (-not (Test-Path (Join-Path $venvScripts "nullion-slack.exe"))) { throw "Missing nullion-slack.exe" }
if (-not (Test-Path (Join-Path $venvScripts "nullion-discord.exe"))) { throw "Missing nullion-discord.exe" }
if (-not (Test-Path $envFile)) { throw "Missing .env" }
$envText = Get-Content -Raw $envFile
if ($envText -notmatch 'NULLION_MODEL_PROVIDER="ollama"') { throw "Expected ollama provider in .env" }
if ($envText -notmatch 'NULLION_WEB_PORT=8742') { throw "Expected web port in .env" }
if ($envText -notmatch "NULLION_KEY_STORAGE=$ExpectedKeyStorage") { throw "Expected NULLION_KEY_STORAGE=$ExpectedKeyStorage in .env" }
if ($envText -notmatch "search_plugin=builtin_search_provider") { throw "Expected built-in search provider in .env" }
if ($ExpectedKeyStorage -eq "system") {
    if (Test-Path (Join-Path $installDir "chat_history.key")) { throw "System key storage should not leave chat_history.key" }
    if (-not (Test-Path $env:NULLION_INSTALLER_KEYRING_FILE)) { throw "System key storage did not write the keyring store" }
} else {
    if (-not (Test-Path (Join-Path $installDir "chat_history.key"))) { throw "Local key storage did not create chat_history.key" }
}

Invoke-EntrypointHelp (Join-Path $venvScripts "nullion.exe")
Invoke-EntrypointHelp (Join-Path $venvScripts "nullion-web.exe")
Invoke-EntrypointHelp (Join-Path $venvScripts "nullion-tray.exe")
Invoke-EntrypointHelp (Join-Path $venvScripts "nullion-webview.exe")
Invoke-EntrypointHelp (Join-Path $venvScripts "nullion-telegram.exe")
Invoke-EntrypointHelp (Join-Path $venvScripts "nullion-slack.exe")
Invoke-EntrypointHelp (Join-Path $venvScripts "nullion-discord.exe")
}

function Invoke-EntrypointHelp {
    param([string]$Path)
    Write-Host "Checking entrypoint help: $Path"
    $oldPythonWarnings = $env:PYTHONWARNINGS
    $outputFile = [System.IO.Path]::GetTempFileName()
    $errorFile = [System.IO.Path]::GetTempFileName()
    try {
        $env:PYTHONWARNINGS = "ignore"
        $process = Start-Process -FilePath $Path -ArgumentList "--help" -Wait -PassThru -NoNewWindow -RedirectStandardOutput $outputFile -RedirectStandardError $errorFile
        if ($process.ExitCode -ne 0) {
            Get-Content $outputFile -ErrorAction SilentlyContinue | Write-Host
            Get-Content $errorFile -ErrorAction SilentlyContinue | Write-Host
            throw "Entrypoint help failed for $Path with exit code $($process.ExitCode)"
        }
    } finally {
        if ($null -eq $oldPythonWarnings) {
            Remove-Item Env:\PYTHONWARNINGS -ErrorAction SilentlyContinue
        } else {
            $env:PYTHONWARNINGS = $oldPythonWarnings
        }
        Remove-Item -Force $outputFile, $errorFile -ErrorAction SilentlyContinue
    }
}

function Import-NullionEnv {
    param([string]$EnvFile)
Get-Content $envFile | ForEach-Object {
    if ($_ -match '^([^#=]+)="?([^"]*)"?$') {
        [System.Environment]::SetEnvironmentVariable($Matches[1].Trim(), $Matches[2].Trim(), "Process")
    }
}
}

function Assert-WebHealth {
$installDir = Join-Path $env:USERPROFILE ".nullion"
$venvScripts = Join-Path $installDir "venv\Scripts"
$envFile = Join-Path $installDir ".env"
Import-NullionEnv -EnvFile $envFile
$logDir = Join-Path $installDir "logs"
New-Item -ItemType Directory -Force $logDir | Out-Null
$webProc = Start-Process `
    -FilePath (Join-Path $venvScripts "nullion-web.exe") `
    -ArgumentList "--host", "127.0.0.1", "--port", "8742", "--env-file", $envFile, "--checkpoint", (Join-Path $installDir "runtime.db") `
    -RedirectStandardOutput (Join-Path $logDir "installer-smoke-web.log") `
    -RedirectStandardError (Join-Path $logDir "installer-smoke-web-error.log") `
    -WindowStyle Hidden `
    -PassThru

try {
    $healthy = $false
    for ($i = 0; $i -lt 60; $i++) {
        if ($webProc.HasExited) {
            throw "nullion-web exited before becoming healthy with code $($webProc.ExitCode)"
        }
        try {
            $health = Invoke-RestMethod -Uri "http://127.0.0.1:8742/api/health" -TimeoutSec 2
            if ($health.status -eq "ok") {
                $healthy = $true
                break
            }
        } catch {
            Start-Sleep -Seconds 1
        }
    }
    if (-not $healthy) {
        Get-Content (Join-Path $logDir "installer-smoke-web.log") -Tail 100 -ErrorAction SilentlyContinue | Write-Host
        Get-Content (Join-Path $logDir "installer-smoke-web-error.log") -Tail 100 -ErrorAction SilentlyContinue | Write-Host
        throw "Nullion web service did not become healthy after installer smoke launch."
    }
} finally {
    if (-not $webProc.HasExited) {
        Stop-Process -Id $webProc.Id -Force
    }
}
}

function Stop-InstalledServices {
    Get-Process -Name "nullion-web", "nullion-tray" -ErrorAction SilentlyContinue |
        Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
}

function Assert-NoLockedEntrypointPipFailure {
    param(
        [string]$OutputText,
        [string]$Context
    )

    if ($OutputText -match "WinError 32|\.deleteme|process cannot access the file") {
        $OutputText | Write-Host
        throw "$Context hit a Windows locked-entrypoint pip failure."
    }
}

function Invoke-UpdateRollbackSmoke {
    $src = "C:\tmp\nullion-work\nullion-src"
    $origin = "C:\tmp\nullion-work\nullion-update-origin.git"
    $outputFile = "C:\tmp\nullion-work\nullion-update-rollback.out"
    $installDir = Join-Path $env:USERPROFILE ".nullion"
    $venvScripts = Join-Path $installDir "venv\Scripts"
    $browserPython = Join-Path $installDir "browser-use-venv\Scripts\python.exe"
    if (-not (Test-Path $browserPython)) { throw "Visual browser worker was not installed." }
    & $browserPython -c "import browser_use; import playwright"
    if ($LASTEXITCODE -ne 0) { throw "Visual browser worker dependencies are unavailable." }

    Remove-Item -Recurse -Force (Join-Path $src ".git") -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force $origin -ErrorAction SilentlyContinue
    & git -C $src init | Out-Null
    & git -C $src checkout -B main | Out-Null
    & git -C $src config user.email "installer-smoke@nullion.local"
    & git -C $src config user.name "Nullion Installer Smoke"
    & git -C $src add -A
    & git -C $src commit -m "healthy install source" | Out-Null
    $healthyCommit = (& git -C $src rev-parse HEAD).Trim()

    & git init --bare $origin | Out-Null
    & git -C $src remote add origin $origin
    & git -C $src push -u origin main | Out-Null

    Add-Content -Path (Join-Path $src "src\nullion\web_app.py") -Value "`nthis is invalid python for rollback smoke" -Encoding UTF8
    & git -C $src add "src\nullion\web_app.py"
    & git -C $src commit -m "broken update target" | Out-Null
    & git -C $src push origin main | Out-Null
    & git -C $src reset --hard $healthyCommit | Out-Null

    $previousSrcDir = $env:NULLION_SRC_DIR
    $previousInstallDir = $env:NULLION_INSTALL_DIR
    $previousEnvFile = $env:NULLION_ENV_FILE
    $previousCheckpoint = $env:NULLION_CHECKPOINT
    $previousErrorActionPreference = $ErrorActionPreference
    $nativeErrorPreferenceExists = $null -ne (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue)
    if ($nativeErrorPreferenceExists) {
        $previousNativeErrorPreference = $PSNativeCommandUseErrorActionPreference
    }
    try {
        $env:NULLION_SRC_DIR = $src
        $env:NULLION_INSTALL_DIR = $installDir
        $env:NULLION_ENV_FILE = Join-Path $installDir ".env"
        $env:NULLION_CHECKPOINT = Join-Path $installDir "runtime.db"

        $ErrorActionPreference = "Continue"
        if ($nativeErrorPreferenceExists) {
            $PSNativeCommandUseErrorActionPreference = $false
        }
        $output = & (Join-Path $venvScripts "nullion.exe") update --hash 2>&1
        $status = $LASTEXITCODE
        $output | Set-Content -Path $outputFile -Encoding UTF8
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
        if ($nativeErrorPreferenceExists) {
            $PSNativeCommandUseErrorActionPreference = $previousNativeErrorPreference
        }
        if ($null -eq $previousSrcDir) { Remove-Item Env:\NULLION_SRC_DIR -ErrorAction SilentlyContinue } else { $env:NULLION_SRC_DIR = $previousSrcDir }
        if ($null -eq $previousInstallDir) { Remove-Item Env:\NULLION_INSTALL_DIR -ErrorAction SilentlyContinue } else { $env:NULLION_INSTALL_DIR = $previousInstallDir }
        if ($null -eq $previousEnvFile) { Remove-Item Env:\NULLION_ENV_FILE -ErrorAction SilentlyContinue } else { $env:NULLION_ENV_FILE = $previousEnvFile }
        if ($null -eq $previousCheckpoint) { Remove-Item Env:\NULLION_CHECKPOINT -ErrorAction SilentlyContinue } else { $env:NULLION_CHECKPOINT = $previousCheckpoint }
    }

    if ($status -eq 0) {
        Get-Content $outputFile | Write-Host
        throw "Update rollback smoke unexpectedly succeeded."
    }
    $outputText = Get-Content -Raw $outputFile
    Assert-NoLockedEntrypointPipFailure -OutputText $outputText -Context "Update rollback smoke"
    if ($outputText -notmatch "Rolled back to previous version automatically|Rollback successful") {
        Get-Content $outputFile | Write-Host
        throw "Update rollback smoke did not report automatic rollback."
    }

    $afterCommit = (& git -C $src rev-parse HEAD).Trim()
    if ($afterCommit -ne $healthyCommit) {
        Get-Content $outputFile | Write-Host
        throw "Update rollback smoke left HEAD at $afterCommit instead of $healthyCommit."
    }
    if (-not (Get-ChildItem -Path (Join-Path $installDir "backups") -Filter git_commit.txt -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1)) {
        throw "Update rollback smoke did not create a rollback snapshot."
    }
}

function Invoke-EntrypointUpdateSmoke {
    $src = "C:\tmp\nullion-work\nullion-src"
    $origin = "C:\tmp\nullion-work\nullion-entrypoint-origin.git"
    $outputFile = "C:\tmp\nullion-work\nullion-entrypoint-update.out"
    $installDir = Join-Path $env:USERPROFILE ".nullion"
    $venvScripts = Join-Path $installDir "venv\Scripts"

    Remove-Item -Recurse -Force (Join-Path $src ".git") -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force $origin -ErrorAction SilentlyContinue
    & git -C $src init | Out-Null
    & git -C $src checkout -B main | Out-Null
    & git -C $src config user.email "installer-smoke@nullion.local"
    & git -C $src config user.name "Nullion Installer Smoke"
    & git -C $src add -A
    & git -C $src commit -m "healthy install source" | Out-Null
    $healthyCommit = (& git -C $src rev-parse HEAD).Trim()

    & git init --bare $origin | Out-Null
    & git -C $src remote add origin $origin
    & git -C $src push -u origin main | Out-Null

    Set-Content -Path (Join-Path $src "entrypoint-update-smoke.txt") -Value "updated through installed nullion.exe" -Encoding ASCII
    & git -C $src add "entrypoint-update-smoke.txt"
    & git -C $src commit -m "entrypoint update target" | Out-Null
    $targetCommit = (& git -C $src rev-parse HEAD).Trim()
    & git -C $src push origin main | Out-Null
    & git -C $src reset --hard $healthyCommit | Out-Null

    $previousSrcDir = $env:NULLION_SRC_DIR
    $previousInstallDir = $env:NULLION_INSTALL_DIR
    $previousEnvFile = $env:NULLION_ENV_FILE
    $previousCheckpoint = $env:NULLION_CHECKPOINT
    $previousErrorActionPreference = $ErrorActionPreference
    $nativeErrorPreferenceExists = $null -ne (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue)
    if ($nativeErrorPreferenceExists) {
        $previousNativeErrorPreference = $PSNativeCommandUseErrorActionPreference
    }
    try {
        $env:NULLION_SRC_DIR = $src
        $env:NULLION_INSTALL_DIR = $installDir
        $env:NULLION_ENV_FILE = Join-Path $installDir ".env"
        $env:NULLION_CHECKPOINT = Join-Path $installDir "runtime.db"

        $ErrorActionPreference = "Continue"
        if ($nativeErrorPreferenceExists) {
            $PSNativeCommandUseErrorActionPreference = $false
        }
        $output = & (Join-Path $venvScripts "nullion.exe") update --hash 2>&1
        $status = $LASTEXITCODE
        $output | Set-Content -Path $outputFile -Encoding UTF8
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
        if ($nativeErrorPreferenceExists) {
            $PSNativeCommandUseErrorActionPreference = $previousNativeErrorPreference
        }
        if ($null -eq $previousSrcDir) { Remove-Item Env:\NULLION_SRC_DIR -ErrorAction SilentlyContinue } else { $env:NULLION_SRC_DIR = $previousSrcDir }
        if ($null -eq $previousInstallDir) { Remove-Item Env:\NULLION_INSTALL_DIR -ErrorAction SilentlyContinue } else { $env:NULLION_INSTALL_DIR = $previousInstallDir }
        if ($null -eq $previousEnvFile) { Remove-Item Env:\NULLION_ENV_FILE -ErrorAction SilentlyContinue } else { $env:NULLION_ENV_FILE = $previousEnvFile }
        if ($null -eq $previousCheckpoint) { Remove-Item Env:\NULLION_CHECKPOINT -ErrorAction SilentlyContinue } else { $env:NULLION_CHECKPOINT = $previousCheckpoint }
    }

    $outputText = Get-Content -Raw $outputFile
    Assert-NoLockedEntrypointPipFailure -OutputText $outputText -Context "Installed entrypoint update smoke"
    if ($status -ne 0) {
        $outputText | Write-Host
        throw "Installed entrypoint update smoke failed."
    }
    if ($outputText -notmatch "Nullion updated from") {
        $outputText | Write-Host
        throw "Installed entrypoint update smoke did not report update success."
    }

    $afterCommit = (& git -C $src rev-parse HEAD).Trim()
    if ($afterCommit -ne $targetCommit) {
        $outputText | Write-Host
        throw "Installed entrypoint update smoke left HEAD at $afterCommit instead of $targetCommit."
    }
}

function Invoke-LockedEntrypointUpdateSmoke {
    $src = "C:\tmp\nullion-work\nullion-src"
    $origin = "C:\tmp\nullion-work\nullion-locked-entrypoint-origin.git"
    $outputFile = "C:\tmp\nullion-work\nullion-locked-entrypoint-update.out"
    $installDir = Join-Path $env:USERPROFILE ".nullion"
    $venvScripts = Join-Path $installDir "venv\Scripts"
    $nullionExe = Join-Path $venvScripts "nullion.exe"
    $pythonExe = Join-Path $venvScripts "python.exe"
    $readyFile = "C:\tmp\nullion-work\nullion-entrypoint-lock.ready"

    Remove-Item -Force $readyFile -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force (Join-Path $src ".git") -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force $origin -ErrorAction SilentlyContinue
    & git -C $src init | Out-Null
    & git -C $src checkout -B main | Out-Null
    & git -C $src config user.email "installer-smoke@nullion.local"
    & git -C $src config user.name "Nullion Installer Smoke"
    & git -C $src add -A
    & git -C $src commit -m "healthy install source" | Out-Null
    $healthyCommit = (& git -C $src rev-parse HEAD).Trim()

    & git init --bare $origin | Out-Null
    & git -C $src remote add origin $origin
    & git -C $src push -u origin main | Out-Null

    Set-Content -Path (Join-Path $src "locked-entrypoint-update-smoke.txt") -Value "updated while nullion.exe is locked" -Encoding ASCII
    & git -C $src add "locked-entrypoint-update-smoke.txt"
    & git -C $src commit -m "locked entrypoint update target" | Out-Null
    $targetCommit = (& git -C $src rev-parse HEAD).Trim()
    & git -C $src push origin main | Out-Null
    & git -C $src reset --hard $healthyCommit | Out-Null

    $lockJob = Start-Job -ScriptBlock {
        param([string]$Path, [string]$ReadyPath)
        $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::None)
        try {
            Set-Content -Path $ReadyPath -Value "ready" -Encoding ASCII
            Start-Sleep -Seconds 120
        } finally {
            $stream.Dispose()
        }
    } -ArgumentList $nullionExe, $readyFile

    try {
        for ($i = 0; $i -lt 30; $i++) {
            if (Test-Path $readyFile) { break }
            if ($lockJob.State -ne "Running") {
                Receive-Job $lockJob | Write-Host
                throw "Could not lock nullion.exe for update smoke."
            }
            Start-Sleep -Seconds 1
        }
        if (-not (Test-Path $readyFile)) {
            throw "Timed out waiting for nullion.exe lock."
        }

        $previousSrcDir = $env:NULLION_SRC_DIR
        $previousInstallDir = $env:NULLION_INSTALL_DIR
        $previousEnvFile = $env:NULLION_ENV_FILE
        $previousCheckpoint = $env:NULLION_CHECKPOINT
        $previousErrorActionPreference = $ErrorActionPreference
        $nativeErrorPreferenceExists = $null -ne (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue)
        if ($nativeErrorPreferenceExists) {
            $previousNativeErrorPreference = $PSNativeCommandUseErrorActionPreference
        }
        try {
            $env:NULLION_SRC_DIR = $src
            $env:NULLION_INSTALL_DIR = $installDir
            $env:NULLION_ENV_FILE = Join-Path $installDir ".env"
            $env:NULLION_CHECKPOINT = Join-Path $installDir "runtime.db"

            $ErrorActionPreference = "Continue"
            if ($nativeErrorPreferenceExists) {
                $PSNativeCommandUseErrorActionPreference = $false
            }
            $output = & $pythonExe -m nullion.cli update --hash 2>&1
            $status = $LASTEXITCODE
            $output | Set-Content -Path $outputFile -Encoding UTF8
        } finally {
            $ErrorActionPreference = $previousErrorActionPreference
            if ($nativeErrorPreferenceExists) {
                $PSNativeCommandUseErrorActionPreference = $previousNativeErrorPreference
            }
            if ($null -eq $previousSrcDir) { Remove-Item Env:\NULLION_SRC_DIR -ErrorAction SilentlyContinue } else { $env:NULLION_SRC_DIR = $previousSrcDir }
            if ($null -eq $previousInstallDir) { Remove-Item Env:\NULLION_INSTALL_DIR -ErrorAction SilentlyContinue } else { $env:NULLION_INSTALL_DIR = $previousInstallDir }
            if ($null -eq $previousEnvFile) { Remove-Item Env:\NULLION_ENV_FILE -ErrorAction SilentlyContinue } else { $env:NULLION_ENV_FILE = $previousEnvFile }
            if ($null -eq $previousCheckpoint) { Remove-Item Env:\NULLION_CHECKPOINT -ErrorAction SilentlyContinue } else { $env:NULLION_CHECKPOINT = $previousCheckpoint }
        }
    } finally {
        Stop-Job $lockJob -ErrorAction SilentlyContinue
        Remove-Job $lockJob -Force -ErrorAction SilentlyContinue
        Remove-Item -Force $readyFile -ErrorAction SilentlyContinue
    }

    $outputText = Get-Content -Raw $outputFile
    Assert-NoLockedEntrypointPipFailure -OutputText $outputText -Context "Locked entrypoint update smoke"
    if ($status -ne 0) {
        $outputText | Write-Host
        throw "Locked entrypoint update smoke failed."
    }
    if ($outputText -notmatch "Nullion updated from") {
        $outputText | Write-Host
        throw "Locked entrypoint update smoke did not report update success."
    }

    $afterCommit = (& git -C $src rev-parse HEAD).Trim()
    if ($afterCommit -ne $targetCommit) {
        $outputText | Write-Host
        throw "Locked entrypoint update smoke left HEAD at $afterCommit instead of $targetCommit."
    }
    if (Test-Path (Join-Path $venvScripts "nullion.exe.deleteme")) {
        throw "Locked entrypoint update smoke left nullion.exe.deleteme behind."
    }
}

function Complete-ScheduledUninstallCleanup {
    param([string]$InstallDir)

    for ($i = 0; $i -lt 30; $i++) {
        if (-not (Test-Path $InstallDir)) {
            return
        }
        try {
            Remove-Item -LiteralPath $InstallDir -Recurse -Force -ErrorAction Stop
        } catch {
            if (-not (Test-Path $InstallDir)) {
                return
            }
            Write-Host "Waiting for scheduled uninstall cleanup: $($_.Exception.Message)"
        }
        Start-Sleep -Seconds 1
    }
}

function Invoke-InstalledUninstall {
    $installDir = Join-Path $env:USERPROFILE ".nullion"
    Set-Content -Path (Join-Path $installDir "webview.pid") -Value "999999" -Encoding ASCII
    & (Join-Path $installDir "venv\Scripts\nullion.exe") uninstall --yes
    if ($LASTEXITCODE -ne 0) { throw "Installed nullion.exe uninstall failed." }
    Complete-ScheduledUninstallCleanup -InstallDir $installDir
    if (Test-Path $installDir) { throw "Uninstall left install directory behind: $installDir" }
}

function Invoke-SmokeStep {
    param(
        [string]$Name,
        [scriptblock]$Script
    )
    Write-Host "== $Name =="
    try {
        & $Script
        Write-Host "OK: $Name"
    } catch {
        Write-Host "FAILED: $Name"
        Write-Host $_
        throw
    }
}

Invoke-InstallerSmokeAnswers -OpenAnswer "n" -KeyStorageAnswer "n"
Invoke-SmokeStep "installed entrypoints after first install" { Assert-InstalledEntrypoints "local" }
Invoke-SmokeStep "web health after first install" { Assert-WebHealth }
Invoke-SmokeStep "stop services after first install" { Stop-InstalledServices }

Invoke-SmokeStep "uninstall after first install" { Invoke-InstalledUninstall }

Invoke-InstallerSmokeAnswers -OpenAnswer "n" -KeyStorageAnswer "y"
Invoke-SmokeStep "installed entrypoints after reinstall" { Assert-InstalledEntrypoints "system" }
Invoke-SmokeStep "web health after reinstall" { Assert-WebHealth }
Invoke-SmokeStep "stop services after reinstall" { Stop-InstalledServices }

$installDir = Join-Path $env:USERPROFILE ".nullion"
$staleMarker = Join-Path $installDir "stale-reinstall-marker.txt"
Set-Content -Path $staleMarker -Value "this must disappear during fresh reset" -Encoding ASCII
$sourcePyproject = Join-Path $installDir "src\pyproject.toml"
if (Test-Path $sourcePyproject) {
    Remove-Item -Force $sourcePyproject
}
$env:NULLION_FRESH_INSTALL = "true"
try {
    Invoke-InstallerSmokeAnswers -OpenAnswer "n" -KeyStorageAnswer "n"
} finally {
    Remove-Item Env:\NULLION_FRESH_INSTALL -ErrorAction SilentlyContinue
}
if (Test-Path $staleMarker) { throw "Fresh reinstall reset did not remove stale install marker." }
Invoke-SmokeStep "installed entrypoints after fresh reinstall reset" { Assert-InstalledEntrypoints "local" }
Invoke-SmokeStep "web health after fresh reinstall reset" { Assert-WebHealth }
Invoke-SmokeStep "update through installed nullion.exe" { Invoke-EntrypointUpdateSmoke }
Invoke-SmokeStep "update while nullion.exe is locked" { Invoke-LockedEntrypointUpdateSmoke }
Invoke-SmokeStep "update rollback after broken target" { Invoke-UpdateRollbackSmoke }
'@

$smokeDir = Join-Path ([System.IO.Path]::GetTempPath()) "nullion-installer-smoke-$PID"
$smokeScript = Join-Path $smokeDir "installer-smoke.ps1"
New-Item -ItemType Directory -Force $smokeDir | Out-Null
try {
    Set-Content -Path $smokeScript -Value $containerCommand -Encoding UTF8

    Get-Content -Raw $smokeScript | docker run --rm -i `
        --mount "type=bind,source=$AppRepoHost,target=C:\workspace\nullion,readonly" `
        $ImageName `
        powershell.exe -NoProfile -ExecutionPolicy Bypass -Command -
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
} finally {
    Remove-Item -Recurse -Force $smokeDir -ErrorAction SilentlyContinue
}
