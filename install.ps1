#Requires -Version 5.1
<#
.SYNOPSIS
    Nullion - one-command installer for Windows
.DESCRIPTION
    Installs Nullion into %USERPROFILE%\.nullion, walks you through messaging
    apps and API key setup, and registers an auto-start task with Task Scheduler.
.EXAMPLE
    # Run from PowerShell (may need to allow scripts first):
    Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
    .\install.ps1
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$NULLION_VERSION   = if ($env:NULLION_VERSION) { $env:NULLION_VERSION } else { "latest" }
$NULLION_DIR       = Join-Path $env:USERPROFILE ".nullion"
$NULLION_ENV_FILE  = Join-Path $NULLION_DIR ".env"
$NULLION_LOG_DIR   = Join-Path $NULLION_DIR "logs"
$NULLION_VENV_DIR  = Join-Path $NULLION_DIR "venv"
$REPO_URL          = "https://github.com/shumanhi/nullion.git"
$TASK_NAME         = "Nullion Web Dashboard"
$TRAY_TASK_NAME    = "Nullion Tray"
$TELEGRAM_TASK_NAME = "Nullion Telegram"
$SLACK_TASK_NAME   = "Nullion Slack"
$DISCORD_TASK_NAME = "Nullion Discord"
$NULLION_WEB_PORT  = 8742
$WHISPER_CPP_MODEL_URL = "https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-base.en.bin"
$WHISPER_CPP_MODEL = Join-Path $NULLION_DIR "models\ggml-base.en.bin"
$PLAYWRIGHT_RUNTIME_READY = $false

# helpers
function Write-Header { param([string]$Text)
    Write-Host ""
    Write-Host "  +------------------------------------------------------------+" -ForegroundColor DarkGray
    Write-Host "  | " -ForegroundColor DarkGray -NoNewline
    Write-Host $Text -ForegroundColor Cyan
    Write-Host "  +------------------------------------------------------------+" -ForegroundColor DarkGray
}

function Write-Ok     { param([string]$Text) Write-Host "  [OK]  $Text" -ForegroundColor Green }
function Write-Info   { param([string]$Text) Write-Host "  [->]  $Text" -ForegroundColor Yellow }
function Write-Err    { param([string]$Text) Write-Host "  [!!]  $Text" -ForegroundColor Red }
function Write-Chip   { param([string]$Label, [string]$Text) Write-Host "  [$Label] $Text" -ForegroundColor DarkGray }

function Get-InstallerScriptPath {
    $pathValue = Get-Variable -Name PSCommandPath -ValueOnly -ErrorAction SilentlyContinue
    if ((-not $pathValue) -and ($MyInvocation.MyCommand.PSObject.Properties.Name -contains "Path")) {
        $pathValue = $MyInvocation.MyCommand.Path
    }
    if ($pathValue -and (Test-Path $pathValue)) { return $pathValue }
    return $null
}

function Test-LocalInstallerSource {
    param([string]$Path)
    if (-not $Path) { return $false }
    $dir = Split-Path -Parent $Path
    return ($dir -and (Test-Path (Join-Path $dir "pyproject.toml")))
}

function Invoke-InstallerSelfRefresh {
    if ($env:NULLION_INSTALLER_SELF_REFRESHED -eq "true") { return }
    if ($env:NULLION_INSTALLER_NO_SELF_REFRESH -eq "true") { return }

    $currentPath = Get-InstallerScriptPath
    if (Test-LocalInstallerSource $currentPath) { return }

    $freshPath = Join-Path $env:TEMP ("nullion-install-{0}.ps1" -f ([guid]::NewGuid().ToString("N")))
    $freshUrl = "https://raw.githubusercontent.com/shumanhi/nullion/main/install.ps1?cb=$([guid]::NewGuid().ToString("N"))"
    try {
        Write-Info "Refreshing installer from GitHub..."
        Invoke-WebRequest -UseBasicParsing -Headers @{"Cache-Control"="no-cache"} -Uri $freshUrl -OutFile $freshPath
        $env:NULLION_INSTALLER_SELF_REFRESHED = "true"
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $freshPath
        $exitCode = $LASTEXITCODE
        Remove-Item -Force $freshPath -ErrorAction SilentlyContinue
        exit $exitCode
    } catch {
        Write-Info "Could not refresh installer automatically; continuing with the current copy."
    }
}

Invoke-InstallerSelfRefresh

function Refresh-ProcessPath {
    $machinePath = [System.Environment]::GetEnvironmentVariable("Path", "Machine")
    $userPath = [System.Environment]::GetEnvironmentVariable("Path", "User")
    $env:Path = "$machinePath;$userPath"
}

function Add-UserPathEntry {
    param([Parameter(Mandatory=$true)][string]$PathEntry)
    if (-not $PathEntry) { return }
    $expandedEntry = [System.Environment]::ExpandEnvironmentVariables($PathEntry).TrimEnd("\")
    $userPath = [System.Environment]::GetEnvironmentVariable("Path", "User")
    $parts = @()
    if ($userPath) {
        $parts = @($userPath -split ";" | Where-Object { $_ })
    }
    foreach ($part in $parts) {
        $expandedPart = [System.Environment]::ExpandEnvironmentVariables($part).TrimEnd("\")
        if ($expandedPart.Equals($expandedEntry, [System.StringComparison]::OrdinalIgnoreCase)) {
            Refresh-ProcessPath
            Write-Ok "Nullion command folder is already on your user PATH."
            return
        }
    }
    $newUserPath = if ($userPath) { "$userPath;$PathEntry" } else { $PathEntry }
    [System.Environment]::SetEnvironmentVariable("Path", $newUserPath, "User")
    Refresh-ProcessPath
    Write-Ok "Added Nullion commands to your user PATH."
}

function Invoke-SchtasksCommand {
    param([Parameter(Mandatory=$true)][string[]]$Arguments)
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(& schtasks @Arguments 2>&1 | ForEach-Object { [string]$_ })
        return [pscustomobject]@{
            ExitCode = $LASTEXITCODE
            Output = ($output -join "`n")
        }
    } catch {
        return [pscustomobject]@{
            ExitCode = 1
            Output = [string]$_.Exception.Message
        }
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
}

function Start-NullionScheduledTaskNow {
    param(
        [Parameter(Mandatory=$true)][string]$TaskName,
        [Parameter(Mandatory=$true)][string]$Label
    )
    $result = Invoke-SchtasksCommand @("/Run", "/TN", $TaskName)
    if ($result.ExitCode -eq 0) {
        Write-Ok "Started $Label from Task Scheduler."
        return $true
    }
    Write-Info "Could not start $Label from Task Scheduler: $($result.Output)"
    return $false
}

function Test-NullionEntrypoint {
    param(
        [Parameter(Mandatory=$true)][string]$ExePath,
        [Parameter(Mandatory=$true)][string]$Label
    )
    if (-not (Test-Path -LiteralPath $ExePath)) {
        throw "$Label was not installed at $ExePath"
    }
    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = @(& $ExePath --help 2>&1 | ForEach-Object { [string]$_ })
        if ($LASTEXITCODE -ne 0) {
            throw "$Label failed its --help check: $($output -join "`n")"
        }
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
}

function Test-NullionInstalledEntrypoints {
    Test-NullionEntrypoint $NULLION_EXE "nullion-web.exe"
    Test-NullionEntrypoint $NULLION_TRAY_EXE "nullion-tray.exe"
    Test-NullionEntrypoint (Join-Path $NULLION_SCRIPTS_DIR "nullion-webview.exe") "nullion-webview.exe"
}

function Start-NullionTrayNow {
    param([bool]$PreferScheduledTask = $false)

    if ($PreferScheduledTask) {
        [void](Start-NullionScheduledTaskNow $TRAY_TASK_NAME "Nullion tray")
        Start-Sleep -Seconds 3
        if (Get-Process -Name "nullion-tray" -ErrorAction SilentlyContinue) {
            Write-Ok "Nullion tray process is running."
            return $true
        }
        Write-Info "Tray task started, but the tray process is not visible yet. Trying a direct interactive launch."
    }

    if ($env:NULLION_INSTALLER_SMOKE -eq "true") {
        Write-Info "Skipping direct tray launch in installer smoke mode."
        return $false
    }

    if (-not (Test-Path -LiteralPath $NULLION_TRAY_EXE)) {
        Write-Err "Cannot start tray; nullion-tray.exe is missing at $NULLION_TRAY_EXE"
        return $false
    }

    $trayLogFile = Join-Path $NULLION_LOG_DIR "tray.log"
    $trayErrFile = Join-Path $NULLION_LOG_DIR "tray-error.log"
    try {
        $proc = Start-Process `
            -FilePath $NULLION_TRAY_EXE `
            -ArgumentList "--port", $NULLION_WEB_PORT, "--env-file", "$NULLION_ENV_FILE" `
            -RedirectStandardOutput $trayLogFile `
            -RedirectStandardError $trayErrFile `
            -WindowStyle Hidden `
            -PassThru
        Start-Sleep -Seconds 3
        if ($proc.HasExited) {
            Write-Err "Nullion tray exited immediately. Check the tray log:"
            Write-Host "    notepad `"$trayErrFile`"" -ForegroundColor Cyan
            return $false
        }
        Write-Ok "Started Nullion tray icon."
        return $true
    } catch {
        Write-Err "Could not start Nullion tray: $($_.Exception.Message)"
        Write-Host "    notepad `"$trayErrFile`"" -ForegroundColor Cyan
        return $false
    }
}

function Test-TruthyValue {
    param([AllowNull()][string]$Value)
    return ($Value -and $Value -match '^(1|true|yes|y)$')
}

function Remove-PathIfExists {
    param(
        [Parameter(Mandatory=$true)][string]$Path,
        [Parameter(Mandatory=$true)][string]$Label
    )
    if (-not (Test-Path -LiteralPath $Path)) { return }
    try {
        Remove-Item -LiteralPath $Path -Recurse -Force -ErrorAction Stop
        Write-Ok "Removed $Label."
    } catch {
        Write-Err "Could not remove $Label at ${Path}: $($_.Exception.Message)"
    }
}

function Stop-NullionProcesses {
    foreach ($name in @(
        "nullion-web",
        "nullion-tray",
        "nullion-telegram",
        "nullion-slack",
        "nullion-discord"
    )) {
        $processes = @(Get-Process -Name $name -ErrorAction SilentlyContinue)
        foreach ($proc in $processes) {
            try {
                Stop-Process -Id $proc.Id -Force -ErrorAction Stop
                Write-Ok "Stopped $name process $($proc.Id)."
            } catch {
                Write-Info "Could not stop $name process $($proc.Id): $($_.Exception.Message)"
            }
        }
    }
}

function Remove-NullionScheduledTasks {
    foreach ($task in @(
        $TASK_NAME,
        $TRAY_TASK_NAME,
        $TELEGRAM_TASK_NAME,
        $SLACK_TASK_NAME,
        $DISCORD_TASK_NAME,
        "Nullion Recovery"
    )) {
        [void](Invoke-SchtasksCommand @("/End", "/TN", $task))
        $result = Invoke-SchtasksCommand @("/Delete", "/TN", $task, "/F")
        if ($result.ExitCode -eq 0) {
            Write-Ok "Removed scheduled task: $task"
        }
    }
}

function Remove-NullionTempFiles {
    $tempRoot = $env:TEMP
    if (-not $tempRoot -or -not (Test-Path -LiteralPath $tempRoot)) { return }
    $seen = @{}
    foreach ($pattern in @("nullion-*", "tmp-nullion-*")) {
        $items = @(Get-ChildItem -LiteralPath $tempRoot -Force -Filter $pattern -ErrorAction SilentlyContinue)
        foreach ($item in $items) {
            if ($seen.ContainsKey($item.FullName)) { continue }
            $seen[$item.FullName] = $true
            Remove-PathIfExists $item.FullName "temporary installer item $($item.Name)"
        }
    }
}

function Reset-NullionInstallState {
    param([string]$Reason = "fresh install requested")
    Write-Header "Fresh install reset"
    Write-Info "Reason: $Reason"
    Write-Info "Removing installer-managed tasks, processes, temp files, config, runtime data, source, and virtual environment."

    Remove-NullionScheduledTasks
    Stop-NullionProcesses
    Remove-PathIfExists $NULLION_DIR "Nullion install directory"

    if ($env:LOCALAPPDATA) {
        Remove-PathIfExists (Join-Path $env:LOCALAPPDATA "Nullion") "local Nullion app data"
    }
    if ($env:APPDATA) {
        Remove-PathIfExists (Join-Path $env:APPDATA "Nullion") "roaming Nullion app data"
    }
    Remove-NullionTempFiles
    Write-Ok "Fresh install cleanup complete."
}

function Get-ExistingInstallHealthIssues {
    $issues = @()
    if (-not (Test-Path -LiteralPath $NULLION_DIR)) { return $issues }

    $sourceDir = Join-Path $NULLION_DIR "src"
    $runtimeDb = Join-Path $NULLION_DIR "runtime.db"
    $venvPython = Join-Path $NULLION_VENV_DIR "Scripts\python.exe"

    if (Test-Path -LiteralPath $NULLION_VENV_DIR) {
        if (-not (Test-Path -LiteralPath $venvPython)) {
            $issues += "The virtual environment exists but Scripts\python.exe is missing."
        } else {
            try {
                $venvVersion = & $venvPython --version 2>&1
                if ($LASTEXITCODE -ne 0) {
                    $issues += "The virtual environment Python could not start."
                } elseif (-not (Test-SupportedPythonVersionText $venvVersion)) {
                    $issues += "The virtual environment uses unsupported Python ($venvVersion)."
                }
            } catch {
                $issues += "The virtual environment Python failed to run."
            }
        }
    }

    if (Test-Path -LiteralPath $sourceDir) {
        if (-not (Test-Path -LiteralPath (Join-Path $sourceDir "pyproject.toml"))) {
            $issues += "The app source folder exists but pyproject.toml is missing."
        }
        if ((Test-Path -LiteralPath (Join-Path $sourceDir ".git")) -and (Get-Command "git" -ErrorAction SilentlyContinue)) {
            & git -C $sourceDir rev-parse --is-inside-work-tree *> $null
            if ($LASTEXITCODE -ne 0) {
                $issues += "The app source git checkout is not readable."
            }
        }
    }

    if (Test-Path -LiteralPath $NULLION_ENV_FILE) {
        try {
            [void](Get-Content -LiteralPath $NULLION_ENV_FILE -TotalCount 1 -ErrorAction Stop)
        } catch {
            $issues += "The .env configuration file cannot be read."
        }
    }

    if (Test-Path -LiteralPath $runtimeDb) {
        try {
            $db = Get-Item -LiteralPath $runtimeDb -ErrorAction Stop
            if ($db.Length -eq 0) {
                $issues += "The runtime database exists but is empty."
            }
        } catch {
            $issues += "The runtime database cannot be inspected."
        }
    }

    $wrapperPaths = @(
        "start-nullion.bat",
        "start-nullion-tray.bat",
        "start-nullion-telegram.bat",
        "start-nullion-slack.bat",
        "start-nullion-discord.bat"
    ) | ForEach-Object { Join-Path $NULLION_DIR $_ }
    $hasWrapper = @($wrapperPaths | Where-Object { Test-Path -LiteralPath $_ }).Count -gt 0
    if ($hasWrapper -and (-not (Test-Path -LiteralPath $venvPython))) {
        $issues += "Startup wrappers exist but the virtual environment is missing."
    }

    return $issues
}

function Invoke-ExistingInstallFreshResetOffer {
    $forceFresh = (Test-TruthyValue $env:NULLION_FRESH_INSTALL) -or (Test-TruthyValue $env:NULLION_RESET_INSTALL)
    if ($forceFresh) {
        Reset-NullionInstallState "NULLION_FRESH_INSTALL/NULLION_RESET_INSTALL was set"
        return
    }

    $issues = @(Get-ExistingInstallHealthIssues)
    if ($issues.Count -eq 0) { return }

    Write-Header "Previous install check"
    Write-Err "The existing Nullion install looks damaged or left over from a failed setup."
    foreach ($issue in $issues) {
        Write-Host "  - $issue" -ForegroundColor Yellow
    }
    Write-Host ""
    Write-Info "A fresh reset removes $NULLION_DIR, scheduled tasks, temp installer files, logs, runtime DB, .env, source, and venv."
    if (Confirm-PromptDefaultYes "Start completely fresh and let setup clean this machine first?") {
        Reset-NullionInstallState "previous install health check failed"
    } else {
        Write-Info "Continuing without a full reset."
    }
}

function Invoke-FreshResetAfterInstallFailure {
    param([Parameter(Mandatory=$true)][string]$Reason)
    if ($env:NULLION_FRESH_RESET_ALREADY_TRIED -eq "true") { return $false }

    Write-Host ""
    Write-Err $Reason
    Write-Info "This can happen when a previous setup left a broken source checkout, venv, config, or runtime state behind."
    if (-not (Confirm-PromptDefaultYes "Clean the old install completely and restart setup?")) {
        return $false
    }

    $env:NULLION_FRESH_RESET_ALREADY_TRIED = "true"
    $currentPath = Get-InstallerScriptPath
    $restartPath = $currentPath
    if ($currentPath -and $env:TEMP -and (Test-Path -LiteralPath $currentPath)) {
        try {
            $restartPath = Join-Path $env:TEMP ("nullion-reset-restart-{0}.ps1" -f ([guid]::NewGuid().ToString("N")))
            Copy-Item -LiteralPath $currentPath -Destination $restartPath -Force -ErrorAction Stop
        } catch {
            $restartPath = $currentPath
            Write-Info "Could not prepare a temporary restart copy: $($_.Exception.Message)"
        }
    }

    Reset-NullionInstallState $Reason

    if ($restartPath -and (Test-Path -LiteralPath $restartPath)) {
        Write-Info "Restarting installer after cleanup..."
        $env:NULLION_INSTALLER_NO_SELF_REFRESH = "true"
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $restartPath
        $restartExitCode = $LASTEXITCODE
        if ($restartPath -ne $currentPath) {
            Remove-Item -Force $restartPath -ErrorAction SilentlyContinue
        }
        exit $restartExitCode
    }

    Write-Info "Cleanup is complete. Re-run the install command to continue from a clean state."
    exit 1
}

function Read-ModelName {
    param([Parameter(Mandatory=$true)][string]$Current)
    Write-Info "Press Enter to use the default ($Current), or type a different model name."
    $modelInput = Read-Host "  Model [$Current]"
    if ($modelInput) { return $modelInput.Trim() }
    return $Current
}

function Write-Logo {
    Write-Host "  +------------------------------------------------------------+" -ForegroundColor DarkGray
    Write-Host "  |   +--------+     " -ForegroundColor DarkGray -NoNewline
    Write-Host "Nullion setup studio" -ForegroundColor White -NoNewline
    Write-Host "                         |" -ForegroundColor DarkGray
    Write-Host "  |   | o    o |     " -ForegroundColor DarkGray -NoNewline
    Write-Host "Local-first AI operator" -ForegroundColor Cyan -NoNewline
    Write-Host "                      |" -ForegroundColor DarkGray
    Write-Host "  |   |   ==   |     " -ForegroundColor DarkGray -NoNewline
    Write-Host "v$NULLION_VERSION - guided install" -ForegroundColor DarkGray -NoNewline
    Write-Host "                     |" -ForegroundColor DarkGray
    Write-Host "  |   +--------+     " -ForegroundColor DarkGray -NoNewline
    Write-Host "[ready]" -ForegroundColor Green -NoNewline
    Write-Host "                                      |" -ForegroundColor DarkGray
    Write-Host "  +------------------------------------------------------------+" -ForegroundColor DarkGray
}

function Write-MenuItem {
    param(
        [string]$Number,
        [string]$Title,
        [string]$Detail = "",
        [string]$Badge = ""
    )
    Write-Host "   $Number) " -NoNewline
    Write-Host $Title -ForegroundColor White -NoNewline
    if ($Badge) {
        Write-Host " $Badge" -ForegroundColor Green
    } else {
        Write-Host ""
    }
    if ($Detail) {
        Write-Host "      $Detail" -ForegroundColor DarkGray
    }
}

function Write-CheckItem {
    param(
        [bool]$Checked,
        [bool]$Focused,
        [string]$Title,
        [string]$Detail = "",
        [string]$Badge = ""
    )
    $cursor = if ($Focused) { ">" } else { " " }
    $mark = if ($Checked) { "x" } else { " " }
    Write-Host "  $cursor [$mark] " -NoNewline
    Write-Host $Title -ForegroundColor White -NoNewline
    if ($Badge) {
        Write-Host " $Badge" -ForegroundColor Green
    } else {
        Write-Host ""
    }
    if ($Detail) {
        Write-Host "        $Detail" -ForegroundColor DarkGray
    }
}

function Write-SetupOverview {
    Write-Host "  +-- Setup Path ----------------------------------------------+" -ForegroundColor DarkGray
    Write-Host "  | " -ForegroundColor DarkGray -NoNewline
    Write-Host "1  Python runtime" -ForegroundColor White -NoNewline
    Write-Host "        check or install Python 3.11-3.13" -ForegroundColor DarkGray
    Write-Host "  | " -ForegroundColor DarkGray -NoNewline
    Write-Host "2  Nullion app" -ForegroundColor White -NoNewline
    Write-Host "           install into $NULLION_DIR" -ForegroundColor DarkGray
    Write-Host "  | " -ForegroundColor DarkGray -NoNewline
    Write-Host "3  Capabilities" -ForegroundColor White -NoNewline
    Write-Host "           AI, chat, browser, media, skills" -ForegroundColor DarkGray
    Write-Host "  | " -ForegroundColor DarkGray -NoNewline
    Write-Host "4  Launch" -ForegroundColor White -NoNewline
    Write-Host "                 dashboard at http://localhost:$NULLION_WEB_PORT" -ForegroundColor DarkGray
    Write-Host "  +------------------------------------------------------------+" -ForegroundColor DarkGray
}

function Confirm-Prompt {
    param([string]$Prompt = "Continue?")
    $ans = Read-Host "  $Prompt [y/N]"
    return ($ans -match '^(y|yes)$')
}

function Confirm-PromptDefaultYes {
    param([string]$Prompt = "Continue?")
    $ans = Read-Host "  $Prompt [Y/n]"
    return (-not $ans -or $ans -match '^(y|yes)$')
}

function Get-EnvValue {
    param([Parameter(Mandatory=$true)][string]$Key)
    if (-not (Test-Path $NULLION_ENV_FILE)) { return "" }
    $escaped = [regex]::Escape($Key)
    $line = Get-Content $NULLION_ENV_FILE | Where-Object { $_ -match "^$escaped=" } | Select-Object -Last 1
    if (-not $line) { return "" }
    $value = ($line -split "=", 2)[1].Trim()
    if ($value.StartsWith('"') -and $value.EndsWith('"')) {
        $value = $value.Substring(1, $value.Length - 2)
    }
    return $value
}

function Set-EnvValue {
    param(
        [Parameter(Mandatory=$true)][string]$Key,
        [AllowNull()][string]$Value = "",
        [switch]$Raw
    )
    if (-not (Test-Path $NULLION_DIR)) {
        [void](New-Item -ItemType Directory -Force -Path $NULLION_DIR)
    }
    $safeValue = ([string]$Value) -replace '[\r\n]', ''
    $line = if ($Raw) { "$Key=$safeValue" } else { "$Key=`"$safeValue`"" }
    $lines = @()
    if (Test-Path $NULLION_ENV_FILE) {
        $lines = @(Get-Content $NULLION_ENV_FILE)
    }
    $escaped = [regex]::Escape($Key)
    $updated = $false
    for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -match "^$escaped=") {
            $lines[$i] = $line
            $updated = $true
        }
    }
    if (-not $updated) {
        $lines += $line
    }
    $lines | Set-Content -Path $NULLION_ENV_FILE -Encoding UTF8
}

function Protect-EnvFile {
    if (-not (Test-Path $NULLION_ENV_FILE)) { return }
    try {
        $acl = Get-Acl $NULLION_ENV_FILE
        $acl.SetAccessRuleProtection($true, $false)
        $rule = New-Object System.Security.AccessControl.FileSystemAccessRule(
            $env:USERNAME, "FullControl", "Allow")
        $acl.SetAccessRule($rule)
        Set-Acl -Path $NULLION_ENV_FILE -AclObject $acl
        Write-Ok "Restricted configuration file access to the current user."
    } catch {
        Write-Info "Could not restrict .env permissions automatically. Setup can continue; keep $NULLION_ENV_FILE private."
    }
}

function Format-BoolText {
    param([bool]$Value)
    if ($Value) { return "true" }
    return "false"
}

function Choose-KeyStorage {
    $existing = Get-EnvValue "NULLION_KEY_STORAGE"
    if ($existing) {
        Write-Info "Found existing local data key storage: $existing"
        if (Confirm-PromptDefaultYes "Keep existing encryption key storage setting?") {
            return $existing
        }
    }

    Write-Host ""
    Write-Host "  Local data encryption" -ForegroundColor White
    Write-Host "  Nullion encrypts local chat history and saved provider credentials."
    Write-Host "  On Windows, Nullion can protect the encryption key with Windows Credential Manager,"
    Write-Host "  or store it locally beside your Nullion data."
    Write-Host ""
    if (Confirm-PromptDefaultYes "Protect local data encryption key with Windows Credential Manager?") {
        return "system"
    }
    return "local"
}

function Initialize-KeyStorage {
    param([Parameter(Mandatory=$true)][string]$Storage)

    $env:NULLION_KEY_STORAGE = $Storage
    $outputFile = [System.IO.Path]::GetTempFileName()
    $errorFile = [System.IO.Path]::GetTempFileName()
    $oldPythonWarnings = $env:PYTHONWARNINGS
    try {
        $env:PYTHONWARNINGS = "ignore::RuntimeWarning"
        & $VENV_PYTHON -m nullion.secure_storage --init --storage $Storage 1> $outputFile 2> $errorFile
        if ($LASTEXITCODE -eq 0) {
            if ($Storage -eq "system") {
                Write-Ok "Local data key protected with Windows Credential Manager."
            } else {
                Write-Ok "Local data key stored at $NULLION_DIR\chat_history.key."
            }
            return
        }
        $detail = (Get-Content $errorFile -Raw -ErrorAction SilentlyContinue).Trim()
        if (-not $detail) { $detail = "unknown error" }
        if ($Storage -eq "system") {
            throw "Could not initialize Windows Credential Manager storage: $detail"
        }
        throw "Could not initialize local key storage: $detail"
    } finally {
        if ($null -eq $oldPythonWarnings) {
            Remove-Item Env:\PYTHONWARNINGS -ErrorAction SilentlyContinue
        } else {
            $env:PYTHONWARNINGS = $oldPythonWarnings
        }
        Remove-Item -Force $outputFile, $errorFile -ErrorAction SilentlyContinue
    }
}

function Get-SetupValue {
    param(
        [Parameter(Mandatory=$true)][string]$Name,
        [object]$Default = ""
    )
    $var = Get-Variable -Name $Name -ErrorAction SilentlyContinue
    if ($var) { return $var.Value }
    return $Default
}

function Get-SetupBool {
    param([Parameter(Mandatory=$true)][string]$Name)
    return [bool](Get-SetupValue $Name $false)
}

function Save-PluginCheckpoint {
    $searchProvider = [string](Get-SetupValue "SEARCH_PROVIDER" "builtin_search_provider")
    $enabledPlugins = "search_plugin,browser_plugin,workspace_plugin,media_plugin"
    $providerBindings = "search_plugin=$searchProvider,media_plugin=local_media_provider"

    if (Get-SetupBool "EMAIL_CALENDAR_ENABLED") {
        $enabledPlugins += ",email_plugin,calendar_plugin"
        $providerBindings += ",email_plugin=google_workspace_provider,calendar_plugin=google_workspace_provider"
    } elseif (Get-SetupBool "CUSTOM_EMAIL_API_ENABLED") {
        $enabledPlugins += ",email_plugin"
        $providerBindings += ",email_plugin=custom_api_provider"
    }
    Set-EnvValue "NULLION_ENABLED_PLUGINS" $enabledPlugins
    Set-EnvValue "NULLION_PROVIDER_BINDINGS" $providerBindings
}

function Save-MessagingCheckpoint {
    if (-not ($TELEGRAM_ENABLED -or $SLACK_ENABLED -or $DISCORD_ENABLED -or $SKIP_MESSAGING_SETUP)) {
        return
    }
    Set-EnvValue "NULLION_SETUP_MESSAGING_DONE" "true" -Raw
    Set-EnvValue "NULLION_WEB_PORT" "$NULLION_WEB_PORT" -Raw
    Set-EnvValue "NULLION_TELEGRAM_CHAT_ENABLED" (Format-BoolText $TELEGRAM_ENABLED) -Raw
    if ($TELEGRAM_ENABLED) {
        Set-EnvValue "NULLION_TELEGRAM_BOT_TOKEN" $BOT_TOKEN
        Set-EnvValue "NULLION_TELEGRAM_OPERATOR_CHAT_ID" $CHAT_ID
    }
    Set-EnvValue "NULLION_SLACK_ENABLED" (Format-BoolText $SLACK_ENABLED) -Raw
    if ($SLACK_ENABLED) {
        Set-EnvValue "NULLION_SLACK_BOT_TOKEN" $SLACK_BOT_TOKEN
        Set-EnvValue "NULLION_SLACK_APP_TOKEN" $SLACK_APP_TOKEN
        if ($SLACK_SIGNING_SECRET) { Set-EnvValue "NULLION_SLACK_SIGNING_SECRET" $SLACK_SIGNING_SECRET }
        if ($SLACK_OPERATOR_USER_ID) { Set-EnvValue "NULLION_SLACK_OPERATOR_USER_ID" $SLACK_OPERATOR_USER_ID }
    }
    Set-EnvValue "NULLION_DISCORD_ENABLED" (Format-BoolText $DISCORD_ENABLED) -Raw
    if ($DISCORD_ENABLED) {
        Set-EnvValue "NULLION_DISCORD_BOT_TOKEN" $DISCORD_BOT_TOKEN
    }
    Write-Ok "Messaging setup checkpoint saved to $NULLION_ENV_FILE"
}

function Save-ProviderCheckpoint {
    Set-EnvValue "NULLION_SETUP_PROVIDER_DONE" "true" -Raw
    if ($ANTHROPIC_KEY) { Set-EnvValue "ANTHROPIC_API_KEY" $ANTHROPIC_KEY }
    if ($OPENAI_KEY) { Set-EnvValue "OPENAI_API_KEY" $OPENAI_KEY }
    if ($MODEL_PROVIDER) { Set-EnvValue "NULLION_MODEL_PROVIDER" $MODEL_PROVIDER }
    if ($MODEL_BASE_URL) { Set-EnvValue "NULLION_OPENAI_BASE_URL" $MODEL_BASE_URL }
    if ($MODEL_NAME) { Set-EnvValue "NULLION_MODEL" $MODEL_NAME }
    Write-Ok "Provider setup checkpoint saved to $NULLION_ENV_FILE"
}

function Save-BrowserCheckpoint {
    Set-EnvValue "NULLION_SETUP_BROWSER_DONE" "true" -Raw
    if ($BROWSER_BACKEND) { Set-EnvValue "NULLION_BROWSER_BACKEND" $BROWSER_BACKEND }
    if ($BROWSER_CDP_URL) { Set-EnvValue "NULLION_BROWSER_CDP_URL" $BROWSER_CDP_URL }
    if ($BROWSER_PREFERRED) { Set-EnvValue "NULLION_BROWSER_PREFERRED" $BROWSER_PREFERRED }
    Write-Ok "Browser setup checkpoint saved to $NULLION_ENV_FILE"
}

function Save-SearchCheckpoint {
    Set-EnvValue "NULLION_SETUP_SEARCH_DONE" "true" -Raw
    Save-PluginCheckpoint
    if ($BRAVE_SEARCH_KEY) { Set-EnvValue "NULLION_BRAVE_SEARCH_API_KEY" $BRAVE_SEARCH_KEY }
    if ($GOOGLE_SEARCH_KEY) { Set-EnvValue "NULLION_GOOGLE_SEARCH_API_KEY" $GOOGLE_SEARCH_KEY }
    if ($GOOGLE_SEARCH_CX) { Set-EnvValue "NULLION_GOOGLE_SEARCH_CX" $GOOGLE_SEARCH_CX }
    if ($PERPLEXITY_SEARCH_KEY) { Set-EnvValue "NULLION_PERPLEXITY_API_KEY" $PERPLEXITY_SEARCH_KEY }
    Write-Ok "Search setup checkpoint saved to $NULLION_ENV_FILE"
}

function Save-AccountCheckpoint {
    Set-EnvValue "NULLION_SETUP_ACCOUNT_DONE" "true" -Raw
    Save-PluginCheckpoint
    if ($MATON_API_KEY) { Set-EnvValue "MATON_API_KEY" $MATON_API_KEY }
    if ($COMPOSIO_API_KEY) { Set-EnvValue "COMPOSIO_API_KEY" $COMPOSIO_API_KEY }
    if ($NANGO_SECRET_KEY) { Set-EnvValue "NANGO_SECRET_KEY" $NANGO_SECRET_KEY }
    if ($ACTIVEPIECES_API_KEY) { Set-EnvValue "ACTIVEPIECES_API_KEY" $ACTIVEPIECES_API_KEY }
    if ($N8N_BASE_URL) { Set-EnvValue "N8N_BASE_URL" $N8N_BASE_URL }
    if ($N8N_API_KEY) { Set-EnvValue "N8N_API_KEY" $N8N_API_KEY }
    if ($MATON_CONNECTOR_ENABLED) { Set-EnvValue "NULLION_CONNECTOR_GATEWAY" "maton" }
    if ($CUSTOM_API_BASE_URL) { Set-EnvValue "NULLION_CUSTOM_API_BASE_URL" $CUSTOM_API_BASE_URL }
    if ($CUSTOM_API_TOKEN) { Set-EnvValue "NULLION_CUSTOM_API_TOKEN" $CUSTOM_API_TOKEN }
    Write-Ok "Account/API setup checkpoint saved to $NULLION_ENV_FILE"
}

function Save-MediaCheckpoint {
    Set-EnvValue "NULLION_SETUP_MEDIA_DONE" "true" -Raw
    Save-PluginCheckpoint
    if ($MEDIA_OPENAI_KEY) { Set-EnvValue "NULLION_MEDIA_OPENAI_API_KEY" $MEDIA_OPENAI_KEY }
    if ($MEDIA_ANTHROPIC_KEY) { Set-EnvValue "NULLION_MEDIA_ANTHROPIC_API_KEY" $MEDIA_ANTHROPIC_KEY }
    if ($MEDIA_OPENROUTER_KEY) { Set-EnvValue "NULLION_MEDIA_OPENROUTER_API_KEY" $MEDIA_OPENROUTER_KEY }
    if ($MEDIA_GEMINI_KEY) { Set-EnvValue "NULLION_MEDIA_GEMINI_API_KEY" $MEDIA_GEMINI_KEY }
    if ($MEDIA_GROQ_KEY) { Set-EnvValue "NULLION_MEDIA_GROQ_API_KEY" $MEDIA_GROQ_KEY }
    if ($MEDIA_MISTRAL_KEY) { Set-EnvValue "NULLION_MEDIA_MISTRAL_API_KEY" $MEDIA_MISTRAL_KEY }
    if ($MEDIA_DEEPSEEK_KEY) { Set-EnvValue "NULLION_MEDIA_DEEPSEEK_API_KEY" $MEDIA_DEEPSEEK_KEY }
    if ($MEDIA_XAI_KEY) { Set-EnvValue "NULLION_MEDIA_XAI_API_KEY" $MEDIA_XAI_KEY }
    if ($MEDIA_TOGETHER_KEY) { Set-EnvValue "NULLION_MEDIA_TOGETHER_API_KEY" $MEDIA_TOGETHER_KEY }
    if ($MEDIA_CUSTOM_KEY) { Set-EnvValue "NULLION_MEDIA_CUSTOM_API_KEY" $MEDIA_CUSTOM_KEY }
    if ($MEDIA_CUSTOM_BASE_URL) { Set-EnvValue "NULLION_MEDIA_CUSTOM_BASE_URL" $MEDIA_CUSTOM_BASE_URL }
    if ($AUDIO_TRANSCRIBE_COMMAND) { Set-EnvValue "NULLION_AUDIO_TRANSCRIBE_COMMAND" $AUDIO_TRANSCRIBE_COMMAND }
    if ($IMAGE_OCR_COMMAND) { Set-EnvValue "NULLION_IMAGE_OCR_COMMAND" $IMAGE_OCR_COMMAND }
    if ($IMAGE_GENERATE_COMMAND) { Set-EnvValue "NULLION_IMAGE_GENERATE_COMMAND" $IMAGE_GENERATE_COMMAND }
    if ($AUDIO_TRANSCRIBE_ENABLED) { Set-EnvValue "NULLION_AUDIO_TRANSCRIBE_ENABLED" "true" -Raw }
    if ($AUDIO_TRANSCRIBE_PROVIDER) { Set-EnvValue "NULLION_AUDIO_TRANSCRIBE_PROVIDER" $AUDIO_TRANSCRIBE_PROVIDER }
    if ($AUDIO_TRANSCRIBE_MODEL) { Set-EnvValue "NULLION_AUDIO_TRANSCRIBE_MODEL" $AUDIO_TRANSCRIBE_MODEL }
    if ($IMAGE_OCR_ENABLED) { Set-EnvValue "NULLION_IMAGE_OCR_ENABLED" "true" -Raw }
    if ($IMAGE_OCR_PROVIDER) { Set-EnvValue "NULLION_IMAGE_OCR_PROVIDER" $IMAGE_OCR_PROVIDER }
    if ($IMAGE_OCR_MODEL) { Set-EnvValue "NULLION_IMAGE_OCR_MODEL" $IMAGE_OCR_MODEL }
    if ($IMAGE_GENERATE_ENABLED) { Set-EnvValue "NULLION_IMAGE_GENERATE_ENABLED" "true" -Raw }
    if ($IMAGE_GENERATE_PROVIDER) { Set-EnvValue "NULLION_IMAGE_GENERATE_PROVIDER" $IMAGE_GENERATE_PROVIDER }
    if ($IMAGE_GENERATE_MODEL) { Set-EnvValue "NULLION_IMAGE_GENERATE_MODEL" $IMAGE_GENERATE_MODEL }
    if ($VIDEO_INPUT_ENABLED) { Set-EnvValue "NULLION_VIDEO_INPUT_ENABLED" "true" -Raw }
    if ($VIDEO_INPUT_PROVIDER) { Set-EnvValue "NULLION_VIDEO_INPUT_PROVIDER" $VIDEO_INPUT_PROVIDER }
    if ($VIDEO_INPUT_MODEL) { Set-EnvValue "NULLION_VIDEO_INPUT_MODEL" $VIDEO_INPUT_MODEL }
    Write-Ok "Media setup checkpoint saved to $NULLION_ENV_FILE"
}

function Save-SkillCheckpoint {
    Set-EnvValue "NULLION_SETUP_SKILLS_DONE" "true" -Raw
    if ($ENABLED_SKILL_PACKS) {
        Set-EnvValue "NULLION_ENABLED_SKILL_PACKS" $ENABLED_SKILL_PACKS
        Set-EnvValue "NULLION_SKILL_PACK_ACCESS_ENABLED" "true" -Raw
        if (",$ENABLED_SKILL_PACKS," -like "*,nullion/connector-skills,*" -or $ENABLED_SKILL_PACKS -like "*api-gateway*") {
            Set-EnvValue "NULLION_CONNECTOR_ACCESS_ENABLED" "true" -Raw
        }
    }
    Write-Ok "Skill setup checkpoint saved to $NULLION_ENV_FILE"
}

function Format-MaskedSecret {
    param(
        [string]$Value,
        [int]$Visible = 8
    )
    if (-not $Value) { return "not set" }
    if ($Value.Length -le 4) { return "****" }
    return "****$($Value.Substring($Value.Length - 4))"
}

function Join-SummaryParts {
    param([string[]]$Parts)
    return (($Parts | Where-Object { $_ }) -join ", ")
}

function Get-ExistingMessagingSummary {
    $parts = @()
    if (($ExistingTelegramEnabled -eq "true") -or "$ExistingTelegramToken$ExistingTelegramChatId") {
        $telegram = "Telegram"
        if ($ExistingTelegramToken) { $telegram += " token $(Format-MaskedSecret $ExistingTelegramToken 12)" }
        if ($ExistingTelegramChatId) { $telegram += ", chat $ExistingTelegramChatId" }
        $parts += $telegram
    }
    if (($ExistingSlackEnabled -eq "true") -or "$ExistingSlackBotToken$ExistingSlackAppToken") {
        $slack = "Slack"
        if ($ExistingSlackBotToken) { $slack += " bot $(Format-MaskedSecret $ExistingSlackBotToken 10)" }
        if ($ExistingSlackAppToken) { $slack += ", app $(Format-MaskedSecret $ExistingSlackAppToken 10)" }
        if ($ExistingSlackOperatorUserId) { $slack += ", operator $ExistingSlackOperatorUserId" }
        $parts += $slack
    }
    if (($ExistingDiscordEnabled -eq "true") -or $ExistingDiscordBotToken) {
        $discord = "Discord"
        if ($ExistingDiscordBotToken) { $discord += " token $(Format-MaskedSecret $ExistingDiscordBotToken 10)" }
        $parts += $discord
    }
    return Join-SummaryParts $parts
}

function Get-ExistingAiProviderSummary {
    $provider = $ExistingModelProvider
    if (-not $provider) {
        if ($ExistingAnthropicKey) {
            $provider = "anthropic"
        } elseif ($ExistingModelBaseUrl) {
            $provider = "OpenAI-compatible"
        } elseif ($ExistingOpenAiKey) {
            $provider = "openai"
        } else {
            $provider = "configured"
        }
    }

    $parts = @("provider $provider")
    if ($ExistingModelName) { $parts += "model $ExistingModelName" }
    if ($ExistingModelBaseUrl) { $parts += "base URL $ExistingModelBaseUrl" }
    if ($ExistingOpenAiKey) { $parts += "OpenAI-compatible key $(Format-MaskedSecret $ExistingOpenAiKey 8)" }
    if ($ExistingAnthropicKey) { $parts += "Anthropic key $(Format-MaskedSecret $ExistingAnthropicKey 10)" }
    return Join-SummaryParts $parts
}

function Download-WhisperCppModel {
    if (Test-Path $WHISPER_CPP_MODEL) {
        Write-Ok "Found whisper.cpp base.en model."
        return $true
    }
    $modelDir = Split-Path -Parent $WHISPER_CPP_MODEL
    New-Item -ItemType Directory -Force -Path $modelDir | Out-Null
    $tmpModel = "$WHISPER_CPP_MODEL.tmp"
    try {
        Write-Info "Downloading whisper.cpp base.en model (~148 MB)..."
        Invoke-WebRequest -Uri $WHISPER_CPP_MODEL_URL -OutFile $tmpModel
        Move-Item -Force $tmpModel $WHISPER_CPP_MODEL
        Write-Ok "Downloaded whisper.cpp base.en model."
        return $true
    } catch {
        Remove-Item -Force $tmpModel -ErrorAction SilentlyContinue
        Write-Err "Could not download the whisper.cpp model: $_"
        return $false
    }
}

function Invoke-NullionRuntimeFinalization {
    Write-Info "Finalizing local runtime database..."
    $checkpointPath = Join-Path $NULLION_DIR "runtime.db"
    $code = @'
import json
import sys
from pathlib import Path

from nullion.updater import run_post_update_migrations

details = run_post_update_migrations(
    env_path=Path(sys.argv[1]),
    checkpoint_path=Path(sys.argv[2]),
    install_dir=Path(sys.argv[3]),
    overwrite_env_credentials=True,
)
print(json.dumps({"ok": True, "warnings": details.get("warnings") or []}, sort_keys=True))
'@
    try {
        $env:NULLION_ENV_FILE = $NULLION_ENV_FILE
        $env:NULLION_INSTALL_DIR = $NULLION_DIR
        $env:NULLION_CHECKPOINT_PATH = $checkpointPath
        $finalizeScript = [System.IO.Path]::ChangeExtension([System.IO.Path]::GetTempFileName(), ".py")
        try {
            Set-Content -Path $finalizeScript -Value $code -Encoding UTF8
            & $VENV_PYTHON $finalizeScript $NULLION_ENV_FILE $checkpointPath $NULLION_DIR | Out-Null
            $exitCode = $LASTEXITCODE
        } finally {
            Remove-Item -Force $finalizeScript -ErrorAction SilentlyContinue
        }
        if ($exitCode -eq 0) {
            Write-Ok "Local runtime database is ready."
        } else {
            Write-Err "Could not finalize the local runtime database. Setup can continue; run Nullion once to retry migrations."
        }
    } catch {
        Write-Err "Could not finalize the local runtime database. Setup can continue; run Nullion once to retry migrations."
    }
}

function Get-StoredCredentialValue {
    param([Parameter(Mandatory=$true)][string]$Field)
    if (-not (Test-Path $VENV_PYTHON)) { return "" }
    $code = @'
import sys
from pathlib import Path

from nullion.config import load_env_file_into_environ
from nullion.credential_store import migrate_credentials_json_to_db

field = sys.argv[1]
env_path = Path(sys.argv[2])
install_dir = Path(sys.argv[3])
if env_path.exists():
    load_env_file_into_environ(env_path)
creds = migrate_credentials_json_to_db(install_dir / "credentials.json", db_path=install_dir / "runtime.db") or {}
provider = str(creds.get("provider") or "").strip()
keys = creds.get("keys")
if not isinstance(keys, dict):
    keys = {}
api_key = str(keys.get(provider) or creds.get("api_key") or "").strip()
models = creds.get("models")
if not isinstance(models, dict):
    models = {}
values = {
    "provider": provider,
    "api_key_prefix": api_key[:8],
    "api_key": api_key,
    "model": str(creds.get("model") or models.get(provider) or "").strip(),
    "base_url": str(creds.get("base_url") or "").strip(),
}
print(values.get(field, ""))
'@
    try {
        $out = & $VENV_PYTHON -c $code $Field $NULLION_ENV_FILE $NULLION_DIR 2>$null
        if ($out) { return ([string]($out | Select-Object -Last 1)).Trim() }
    } catch {
        return ""
    }
    return ""
}

function Install-DefaultLocalMediaRuntime {
    $scoop = Get-Command "scoop" -ErrorAction SilentlyContinue
    $winget = Get-Command "winget" -ErrorAction SilentlyContinue

    if (-not (Get-Command "whisper-cli" -ErrorAction SilentlyContinue)) {
        if ($scoop) {
            try {
                Write-Info "Installing whisper.cpp with scoop..."
                scoop install whisper.cpp
                Refresh-ProcessPath
            } catch {
                Write-Err "scoop whisper.cpp install failed: $_"
            }
        } else {
            Write-Info "whisper.cpp is not installed. Install scoop or whisper-cli later to switch audio transcription to local."
        }
    }

    if (-not (Get-Command "ffmpeg" -ErrorAction SilentlyContinue)) {
        if ($scoop) {
            try {
                Write-Info "Installing ffmpeg with scoop..."
                scoop install ffmpeg
                Refresh-ProcessPath
            } catch {
                Write-Err "scoop ffmpeg install failed: $_"
            }
        } elseif ($winget) {
            try {
                Write-Info "Installing ffmpeg with winget..."
                winget install --id Gyan.FFmpeg --source winget --accept-package-agreements --accept-source-agreements -e
                Refresh-ProcessPath
            } catch {
                Write-Err "winget ffmpeg install failed: $_"
            }
        }
    }

    if (-not (Get-Command "tesseract" -ErrorAction SilentlyContinue)) {
        if ($scoop) {
            try {
                Write-Info "Installing tesseract with scoop..."
                scoop install tesseract
                Refresh-ProcessPath
            } catch {
                Write-Err "scoop tesseract install failed: $_"
            }
        } elseif ($winget) {
            try {
                Write-Info "Installing tesseract with winget..."
                winget install --id UB-Mannheim.TesseractOCR --source winget --accept-package-agreements --accept-source-agreements -e
                Refresh-ProcessPath
            } catch {
                Write-Err "winget tesseract install failed: $_"
            }
        }
    }

    if (Get-Command "whisper-cli" -ErrorAction SilentlyContinue) {
        [void](Download-WhisperCppModel)
    }
    Write-Ok "Local media runtime checked. You can switch audio/OCR to local later in Settings."
}

function Ensure-WhisperCppRuntime {
    Install-DefaultLocalMediaRuntime
    $hasWhisper = [bool](Get-Command "whisper-cli" -ErrorAction SilentlyContinue)
    $hasFfmpeg = [bool](Get-Command "ffmpeg" -ErrorAction SilentlyContinue)

    if (-not $hasFfmpeg) {
        $winget = Get-Command "winget" -ErrorAction SilentlyContinue
        if ($winget) {
            try {
                Write-Info "Installing ffmpeg with winget..."
                winget install --id Gyan.FFmpeg --source winget --accept-package-agreements --accept-source-agreements -e
                Refresh-ProcessPath
                $hasFfmpeg = [bool](Get-Command "ffmpeg" -ErrorAction SilentlyContinue)
            } catch {
                Write-Err "winget install failed: $_"
            }
        }
    }

    if (-not $hasWhisper) {
        Write-Info "Install whisper.cpp so whisper-cli is on PATH, then re-run setup or add NULLION_AUDIO_TRANSCRIBE_COMMAND."
        return $false
    }
    if (-not $hasFfmpeg) {
        Write-Info "Install ffmpeg for Telegram OGG/Opus voice note conversion."
        return $false
    }
    if (-not (Download-WhisperCppModel)) {
        Write-Info "Download ggml-base.en.bin later or add NULLION_AUDIO_TRANSCRIBE_COMMAND."
        return $false
    }
    $script:AUDIO_TRANSCRIBE_COMMAND = "whisper-cli -m `"$WHISPER_CPP_MODEL`" -f {input} -nt"
    $script:AUDIO_TRANSCRIBE_ENABLED = $true
    Write-Ok "Audio transcription will use whisper.cpp defaults."
    return $true
}

function Test-BrowserInstalled {
    param([Parameter(Mandatory=$true)][ValidateSet("brave","chrome")][string]$Browser)
    if ($Browser -eq "brave") {
        if (Get-Command "brave.exe" -ErrorAction SilentlyContinue) { return $true }
        foreach ($path in @(
            "$env:ProgramFiles\BraveSoftware\Brave-Browser\Application\brave.exe",
            "${env:ProgramFiles(x86)}\BraveSoftware\Brave-Browser\Application\brave.exe",
            "$env:LOCALAPPDATA\BraveSoftware\Brave-Browser\Application\brave.exe"
        )) {
            if ($path -and (Test-Path $path)) { return $true }
        }
        return $false
    }
    if (Get-Command "chrome.exe" -ErrorAction SilentlyContinue) { return $true }
    foreach ($path in @(
        "$env:ProgramFiles\Google\Chrome\Application\chrome.exe",
        "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
        "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
    )) {
        if ($path -and (Test-Path $path)) { return $true }
    }
    return $false
}

function Get-BrowserStatusLabel {
    param([Parameter(Mandatory=$true)][ValidateSet("brave","chrome")][string]$Browser)
    if (Test-BrowserInstalled $Browser) { return "installed" }
    return "not detected"
}

function Install-PlaywrightRuntime {
    if ($script:PLAYWRIGHT_RUNTIME_READY) { return $true }
    $pwPython = Join-Path $NULLION_VENV_DIR "Scripts\python.exe"
    $pwExe = Join-Path $NULLION_VENV_DIR "Scripts\playwright.exe"
    if (-not (Test-Path $pwPython)) {
        Write-Info "Playwright runtime will be installed after the virtual environment is ready."
        return $false
    }
    try {
        Write-Info "Installing Playwright Chromium runtime so browser automation is ready when enabled..."
        & $pwPython -m pip install --quiet --no-cache-dir playwright
        if ($LASTEXITCODE -ne 0) {
            Write-Err "Could not install Playwright Python package."
            return $false
        }
        & $pwExe install chromium
        if ($LASTEXITCODE -eq 0) {
            $script:PLAYWRIGHT_RUNTIME_READY = $true
            Write-Ok "Playwright Chromium runtime ready."
            return $true
        }
        Write-Err "Could not install Playwright Chromium. Re-run 'playwright install chromium' later if browser automation fails."
        return $false
    } catch {
        Write-Err "Could not install Playwright Chromium: $_"
        return $false
    }
}

function Install-WindowsProxyToolsWheel {
    $wheelDir = Join-Path ([System.IO.Path]::GetTempPath()) "nullion-proxy-tools-wheel"
    New-Item -ItemType Directory -Force -Path $wheelDir | Out-Null
    $builder = Join-Path $wheelDir "build_proxy_tools_wheel.py"
    @'
from __future__ import annotations

import base64
import hashlib
from pathlib import Path
import sys
import zipfile

wheel_dir = Path(sys.argv[1])
wheel_path = wheel_dir / "proxy_tools-0.1.0-py2.py3-none-any.whl"

files = {
    "proxy_tools/__init__.py": r'''"""
Proxy helpers used by pywebview. Vendored from proxy_tools 0.1.0 for
Windows installers where the legacy source distribution cannot build a wheel.
"""
import sys

PY2 = sys.version_info[0] == 2
_identity = lambda x: x

if PY2:
    def implements_bool(cls):
        cls.__nonzero__ = cls.__bool__
        del cls.__bool__
        return cls
else:
    implements_bool = _identity

@implements_bool
class Proxy(object):
    __slots__ = ("__local", "__dict__", "__name__")

    def __init__(self, local, name=None):
        object.__setattr__(self, "_Proxy__local", local)
        object.__setattr__(self, "__name__", name)

    def _get_current_object(self):
        if not hasattr(self.__local, "__release_local__"):
            return self.__local()
        try:
            return getattr(self.__local, self.__name__)
        except AttributeError:
            raise RuntimeError("no object bound to %s" % self.__name__)

    @property
    def __dict__(self):
        try:
            return self._get_current_object().__dict__
        except RuntimeError:
            raise AttributeError("__dict__")

    def __repr__(self):
        try:
            obj = self._get_current_object()
        except RuntimeError:
            return "<%s unbound>" % self.__class__.__name__
        return repr(obj)

    def __bool__(self):
        try:
            return bool(self._get_current_object())
        except RuntimeError:
            return False

    def __unicode__(self):
        try:
            return unicode(self._get_current_object())
        except RuntimeError:
            return repr(self)

    def __dir__(self):
        try:
            return dir(self._get_current_object())
        except RuntimeError:
            return []

    def __getattr__(self, name):
        if name == "__members__":
            return dir(self._get_current_object())
        return getattr(self._get_current_object(), name)

    __setattr__ = lambda x, n, v: setattr(x._get_current_object(), n, v)
    __delattr__ = lambda x, n: delattr(x._get_current_object(), n)
    __str__ = lambda x: str(x._get_current_object())
    __call__ = lambda x, *a, **kw: x._get_current_object()(*a, **kw)
    __len__ = lambda x: len(x._get_current_object())
    __getitem__ = lambda x, i: x._get_current_object()[i]
    __iter__ = lambda x: iter(x._get_current_object())
    __contains__ = lambda x, i: i in x._get_current_object()

module_property = Proxy
proxy = Proxy
''',
    "proxy_tools-0.1.0.dist-info/METADATA": """Metadata-Version: 2.1
Name: proxy-tools
Version: 0.1.0
Summary: Proxy Implementation
License: MIT
""",
    "proxy_tools-0.1.0.dist-info/WHEEL": """Wheel-Version: 1.0
Generator: nullion-installer
Root-Is-Purelib: true
Tag: py2.py3-none-any
""",
}

def digest(data: bytes) -> str:
    return "sha256=" + base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()

records = []
with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
    for name, text in files.items():
        data = text.encode("utf-8")
        archive.writestr(name, data)
        records.append((name, digest(data), str(len(data))))
    record_name = "proxy_tools-0.1.0.dist-info/RECORD"
    record = "".join(f"{name},{hash_value},{size}\n" for name, hash_value, size in records)
    record += f"{record_name},,\n"
    archive.writestr(record_name, record)
'@ | Set-Content -Path $builder -Encoding UTF8
    & $VENV_PYTHON $builder $wheelDir
    if ($LASTEXITCODE -ne 0) { throw "Could not build local proxy-tools wheel." }
    & $VENV_PYTHON -m pip install --quiet --no-cache-dir --find-links $wheelDir proxy-tools==0.1.0
    if ($LASTEXITCODE -ne 0) { throw "Could not install local proxy-tools wheel." }
}

function Test-MediaModelSupport {
    param(
        [Parameter(Mandatory=$true)][string]$Capability,
        [string]$Provider,
        [string]$Model
    )
    $providerL = ([string]$Provider).Trim().ToLowerInvariant()
    $modelL = ([string]$Model).Trim().ToLowerInvariant()
    if (-not $providerL -or -not $modelL) { return $false }
    switch ($Capability) {
        "audio" { return ($providerL -match "^(openai|groq|custom)$" -and $modelL -match "(transcribe|whisper|audio)") }
        "image_ocr" {
            return (
                $providerL -match "^(anthropic|codex)$" -or
                $modelL -match "(gpt-4o|gpt-4\.1|gpt-5|vision|vl|llava|pixtral|gemini|claude|sonnet|opus|haiku)"
            )
        }
        "image_generate" {
            if ($providerL -eq "openai") {
                return ($modelL -match "(gpt-image|dall-e|image)")
            }
            return ($modelL -match "(image|imagen|flux|stable-diffusion|sdxl)" -or $providerL -eq "custom")
        }
        "video" {
            if ($providerL -eq "openai") {
                return ($modelL -match "(gpt-4o|gpt-4\.1|gpt-5|video|sora)")
            }
            return ($modelL -match "(video|veo|gemini|vision|vl)")
        }
        default { return $false }
    }
}

function Test-CurrentMediaModelUsable {
    param([string]$Provider)
    $providerL = ([string]$Provider).Trim().ToLowerInvariant()
    $key = Get-MediaProviderKey $providerL
    if ($providerL -eq "openai") { return ([string]$key).StartsWith("sk-") }
    if ($providerL -eq "codex") { return $false }
    return -not [string]::IsNullOrWhiteSpace([string]$key)
}

function Get-MediaProviderDefaultModel {
    param([string]$Capability, [string]$Provider)
    switch ("$Capability`:$Provider") {
        "audio:openai" { return "gpt-4o-transcribe" }
        "audio:groq" { return "whisper-large-v3-turbo" }
        "image_ocr:openai" { return "gpt-4o" }
        "image_ocr:anthropic" { return "claude-sonnet-4-6" }
        "image_ocr:openrouter" { return "openai/gpt-4o" }
        "image_ocr:gemini" { return "models/gemini-2.5-flash" }
        "image_ocr:mistral" { return "pixtral-large-latest" }
        "image_generate:openai" { return "gpt-image-1" }
        "image_generate:openrouter" { return "google/gemini-3.1-flash-image-preview" }
        "image_generate:gemini" { return "gemini-3.1-flash-image-preview" }
        "image_generate:xai" { return "grok-2-image" }
        "image_generate:together" { return "black-forest-labs/FLUX.1-schnell-Free" }
        "video:openai" { return "gpt-4o" }
        "video:openrouter" { return "openai/gpt-4o" }
        "video:gemini" { return "models/gemini-2.5-flash" }
        default { return "" }
    }
}

function Set-MediaProviderKey {
    param([string]$Provider, [string]$Key)
    switch ($Provider) {
        "anthropic" { $script:MEDIA_ANTHROPIC_KEY = $Key }
        "openai" { $script:MEDIA_OPENAI_KEY = $Key }
        "openrouter" { $script:MEDIA_OPENROUTER_KEY = $Key }
        "gemini" { $script:MEDIA_GEMINI_KEY = $Key }
        "groq" { $script:MEDIA_GROQ_KEY = $Key }
        "mistral" { $script:MEDIA_MISTRAL_KEY = $Key }
        "deepseek" { $script:MEDIA_DEEPSEEK_KEY = $Key }
        "xai" { $script:MEDIA_XAI_KEY = $Key }
        "together" { $script:MEDIA_TOGETHER_KEY = $Key }
        "custom" { $script:MEDIA_CUSTOM_KEY = $Key }
    }
}

function Get-MediaProviderKey {
    param([string]$Provider)
    switch ($Provider) {
        "anthropic" { if ($MEDIA_ANTHROPIC_KEY) { return $MEDIA_ANTHROPIC_KEY }; return $ANTHROPIC_KEY }
        "openai" { if ($MEDIA_OPENAI_KEY) { return $MEDIA_OPENAI_KEY }; return $OPENAI_KEY }
        "openrouter" { return $MEDIA_OPENROUTER_KEY }
        "gemini" { return $MEDIA_GEMINI_KEY }
        "groq" { return $MEDIA_GROQ_KEY }
        "mistral" { return $MEDIA_MISTRAL_KEY }
        "deepseek" { return $MEDIA_DEEPSEEK_KEY }
        "xai" { return $MEDIA_XAI_KEY }
        "together" { return $MEDIA_TOGETHER_KEY }
        "custom" { return $MEDIA_CUSTOM_KEY }
        default { return "" }
    }
}

function Request-MediaApiKey {
    param([string]$Provider, [string]$KeyUrl)
    $key = Get-MediaProviderKey $Provider
    if ($Provider -eq "openai" -and -not ([string]$key).StartsWith("sk-")) {
        $key = ""
        Write-Info "OpenAI OAuth sign-in cannot be reused for media API calls; paste an API key for this media model."
    }
    if (-not $key) {
        Write-Host "  Get an API key at $KeyUrl" -ForegroundColor Cyan
        $secure = Read-Host "  Paste $(Get-MediaProviderLabel $Provider) media API key (hidden)" -AsSecureString
        $key = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        Set-MediaProviderKey $Provider $key
    }
}

function Request-MediaApiProvider {
    param(
        [string]$Capability,
        [string]$Title,
        [string]$DefaultProvider,
        [string]$DefaultModel,
        [bool]$OpenAiOnly = $false
    )
    Write-Host ""
    Write-Host "  $Title API provider" -ForegroundColor White
    Write-MenuItem "1" "OpenAI" "OpenAI platform API key"
    if ($Capability -eq "audio" -and -not $OpenAiOnly) {
        Write-MenuItem "2" "Groq" "OpenAI-compatible transcription API"
        Write-MenuItem "3" "Custom endpoint" "Any OpenAI-compatible audio transcription endpoint"
        $choice = Read-Host "  Enter 1-3 [1]"
    } elseif ($Capability -eq "image_generate" -and -not $OpenAiOnly) {
        Write-MenuItem "2" "OpenRouter" "OpenAI-compatible image model routing"
        Write-MenuItem "3" "Google Gemini" "Imagen through the Gemini API"
        Write-MenuItem "4" "xAI" "Image generation models"
        Write-MenuItem "5" "Together AI" "FLUX and other image models"
        Write-MenuItem "6" "Custom endpoint" "OpenAI-compatible base URL and model"
        $choice = Read-Host "  Enter 1-6 [1]"
    } elseif (-not $OpenAiOnly) {
        Write-MenuItem "2" "Anthropic" "Claude models"
        Write-MenuItem "3" "OpenRouter" "OpenAI-compatible model routing"
        Write-MenuItem "4" "Google Gemini" "OpenAI-compatible Gemini API"
        Write-MenuItem "5" "Mistral" "Mistral and Pixtral models"
        Write-MenuItem "6" "Custom endpoint" "OpenAI-compatible base URL and model"
        $choice = Read-Host "  Enter 1-6 [1]"
    } else {
        $choice = Read-Host "  Enter 1 [1]"
    }
    if (-not $choice) { $choice = "1" }
    $provider = $DefaultProvider
    $keyUrl = "https://platform.openai.com/api-keys"
    switch ($choice) {
        "2" {
            if ($Capability -eq "audio") {
                $provider = "groq"; $keyUrl = "https://console.groq.com/keys"
            } elseif ($Capability -eq "image_generate") {
                $provider = "openrouter"; $keyUrl = "https://openrouter.ai/keys"
            } else {
                $provider = "anthropic"; $keyUrl = "https://console.anthropic.com/settings/keys"
            }
        }
        "3" {
            if ($Capability -eq "audio") {
                $provider = "custom"
                $script:MEDIA_CUSTOM_BASE_URL = (Read-Host "  OpenAI-compatible base URL (e.g. http://localhost:1234/v1)").Trim()
                $keyUrl = (Read-Host "  API key setup URL (optional)").Trim()
                if (-not $keyUrl) { $keyUrl = "your provider dashboard" }
            } elseif ($Capability -eq "image_generate") {
                $provider = "gemini"; $keyUrl = "https://aistudio.google.com/app/apikey"
            } else {
                $provider = "openrouter"; $keyUrl = "https://openrouter.ai/keys"
            }
        }
        "4" {
            if ($Capability -eq "image_generate") {
                $provider = "xai"; $keyUrl = "https://console.x.ai/"
            } else {
                $provider = "gemini"; $keyUrl = "https://aistudio.google.com/app/apikey"
            }
        }
        "5" {
            if ($Capability -eq "image_generate") {
                $provider = "together"; $keyUrl = "https://api.together.xyz/settings/api-keys"
            } else {
                $provider = "mistral"; $keyUrl = "https://console.mistral.ai/api-keys/"
            }
        }
        "6" {
            $provider = "custom"
            $script:MEDIA_CUSTOM_BASE_URL = (Read-Host "  OpenAI-compatible base URL (e.g. http://localhost:1234/v1)").Trim()
            $keyUrl = (Read-Host "  API key setup URL (optional)").Trim()
            if (-not $keyUrl) { $keyUrl = "your provider dashboard" }
        }
    }
    $model = Get-MediaProviderDefaultModel $Capability $provider
    if (-not $model) { $model = $DefaultModel }
    $model = Read-ModelName $model
    if (Test-MediaModelSupport $Capability $provider $model) {
        Write-Ok "$(Get-MediaProviderLabel $provider) - $model supports $Title."
    } elseif ($provider -eq "custom") {
        Write-Info "Custom provider selected. Nullion will use this if its OpenAI-compatible endpoint supports $Title."
    } else {
        Write-Info "$(Get-MediaProviderLabel $provider) - $model is not a known default for $Title; make sure this model supports the tool."
    }
    Request-MediaApiKey $provider $keyUrl
    return @{ provider = $provider; model = $model }
}

function Get-MediaProviderLabel {
    param([string]$Provider)
    switch ($Provider) {
        "anthropic" { return "Anthropic" }
        "codex" { return "Codex" }
        "openai" { return "OpenAI" }
        "openrouter" { return "OpenRouter" }
        "gemini" { return "Gemini" }
        "ollama" { return "Ollama" }
        "groq" { return "Groq" }
        "mistral" { return "Mistral" }
        "deepseek" { return "DeepSeek" }
        "xai" { return "xAI" }
        "together" { return "Together AI" }
        default {
            if ($Provider) { return $Provider }
            return "provider"
        }
    }
}

function Ensure-Git {
    if (Get-Command "git" -ErrorAction SilentlyContinue) {
        Write-Ok "Found git."
        return
    }

    Write-Info "git not found. Attempting to install via winget..."
    $winget = Get-Command "winget" -ErrorAction SilentlyContinue
    if ($winget) {
        try {
            winget install --id Git.Git --source winget --accept-package-agreements --accept-source-agreements -e
            Refresh-ProcessPath
        } catch {
            Write-Err "winget Git install failed: $_"
        }
    }

    if (-not (Get-Command "git" -ErrorAction SilentlyContinue)) {
        Write-Err "git is required to clone Nullion."
        Write-Info "Install Git from https://git-scm.com/download/win, then re-run this script."
        exit 1
    }
    Write-Ok "git installed."
}

function Test-SupportedPythonVersionText {
    param([string]$VersionText)
    if ($VersionText -match '(\d+)\.(\d+)') {
        $maj = [int]$Matches[1]; $min = [int]$Matches[2]
        return ($maj -eq 3 -and $min -ge 11 -and $min -le 13)
    }
    return $false
}

function Checkout-InstallTarget {
    param([string]$SourceDir)

    $isShallow = (& git -C $SourceDir rev-parse --is-shallow-repository 2>$null)
    if ($isShallow -eq "true") {
        & git -C $SourceDir fetch --quiet --unshallow origin
        if ($LASTEXITCODE -ne 0) { throw "git fetch --unshallow failed" }
    } else {
        & git -C $SourceDir fetch --quiet origin main
        if ($LASTEXITCODE -ne 0) { throw "git fetch origin main failed" }
    }

    & git -C $SourceDir fetch --quiet --prune --prune-tags --force origin "refs/tags/*:refs/tags/*"
    if ($LASTEXITCODE -ne 0) { throw "git fetch release tags failed" }

    if ($script:NULLION_VERSION -eq "main") {
        & git -C $SourceDir reset --quiet --hard origin/main
        if ($LASTEXITCODE -ne 0) { throw "git reset to origin/main failed" }
        & git -C $SourceDir clean --quiet -ffd
        if ($LASTEXITCODE -ne 0) { throw "git clean failed" }
        Write-Ok "Checked out main."
        return
    }

    $latestTag = (& git -C $SourceDir describe --tags --abbrev=0 --match "v[0-9]*" origin/main)
    if (-not $latestTag) {
        Write-Err "No release tags found in Nullion repository."
        exit 1
    }

    & git -C $SourceDir reset --quiet --hard $latestTag
    if ($LASTEXITCODE -ne 0) { throw "git reset to $latestTag failed" }
    & git -C $SourceDir clean --quiet -ffd
    if ($LASTEXITCODE -ne 0) { throw "git clean failed" }

    if ($latestTag.StartsWith("v")) {
        $script:NULLION_VERSION = $latestTag.Substring(1)
    } else {
        $script:NULLION_VERSION = $latestTag
    }
    Write-Ok "Checked out latest release $latestTag."
}

function Get-PythonExe {
    # Try supported well-known names in PATH first. Python 3.14+ is skipped for
    # now because Windows tray/webview dependencies do not reliably publish
    # compatible wheels yet.
    foreach ($candidate in @("python3.13","python3.12","python3.11","python3","python")) {
        $found = Get-Command $candidate -ErrorAction SilentlyContinue
        if ($found) {
            $raw = & $candidate --version 2>&1
            if ($raw -match '(\d+)\.(\d+)') {
                if (Test-SupportedPythonVersionText $raw) { return $candidate }
            }
        }
    }
    # Also check the common py launcher, asking for supported minors explicitly.
    $py = Get-Command "py" -ErrorAction SilentlyContinue
    if ($py) {
        foreach ($minor in @("13","12","11")) {
            $raw = & py "-3.$minor" --version 2>&1
            if ($raw -match '(\d+)\.(\d+)') {
                if (Test-SupportedPythonVersionText $raw) {
                    return @("py", "-3.$minor")
                }
            }
        }
    }
    return $null
}

function Invoke-Python {
    param(
        [Parameter(Mandatory=$true)]$Python,
        [Parameter(ValueFromRemainingArguments=$true)]$Arguments
    )
    if ($Python -is [array]) {
        $exe = $Python[0]
        $baseArgs = @($Python | Select-Object -Skip 1)
        & $exe @baseArgs @Arguments
    } else {
        & $Python @Arguments
    }
}

function Format-PythonCommand {
    param([Parameter(Mandatory=$true)]$Python)
    if ($Python -is [array]) {
        return ($Python -join " ")
    }
    return [string]$Python
}

# banner
Clear-Host
Write-Host ""
Write-Logo
Write-Host ""
Write-Chip "platform" "Windows"
Write-SetupOverview
Write-Host ""

if (-not (Confirm-PromptDefaultYes "Ready to start?")) {
    Write-Host "  Cancelled."
    exit 0
}

# Step 1: Python
Write-Header "Step 1 of 4 - Python"

$PYTHON = Get-PythonExe

if (-not $PYTHON) {
    Write-Info "Python 3.11-3.13 not found. Attempting to install Python 3.12 via winget..."

    $winget = Get-Command "winget" -ErrorAction SilentlyContinue
    if ($winget) {
        try {
            winget install --id Python.Python.3.12 --source winget --accept-package-agreements --accept-source-agreements -e
            Refresh-ProcessPath
            $PYTHON = Get-PythonExe
            if ($PYTHON) {
                Write-Ok "Python installed via winget."
            }
        } catch {
            Write-Err "winget install failed: $_"
        }
    }

    if (-not $PYTHON) {
        Write-Err "Could not install Python automatically."
        Write-Host ""
        Write-Info "Please install Python 3.12 from https://python.org/downloads/release/python-312/"
        Write-Info "Do not use Python 3.14 for this install yet; some Windows wheels are not ready."
        Write-Info "Tick 'Add Python to PATH' during install, then re-run this script."
        exit 1
    }
}

$pyVersion = Invoke-Python $PYTHON --version 2>&1
$pythonDisplay = Format-PythonCommand $PYTHON
Write-Ok "Using $pythonDisplay ($pyVersion)"

Invoke-ExistingInstallFreshResetOffer

# Step 2: Install Nullion
Write-Header "Step 2 of 4 - Installing Nullion"

New-Item -ItemType Directory -Path $NULLION_DIR    -Force | Out-Null
New-Item -ItemType Directory -Path $NULLION_LOG_DIR -Force | Out-Null

# If running from inside a cloned repo, install from there. When the installer is
# piped through `irm ... | iex`, PowerShell runs it as a script block with no
# backing file path.
$scriptPath = $null
if ($MyInvocation.MyCommand.PSObject.Properties.Name -contains "Path") {
    $scriptPath = $MyInvocation.MyCommand.Path
}
$SCRIPT_DIR = if ($scriptPath) { Split-Path -Parent $scriptPath } else { $null }
$SOURCE_DIR = $null
if ($SCRIPT_DIR -and (Test-Path (Join-Path $SCRIPT_DIR "pyproject.toml"))) {
    $SOURCE_DIR = $SCRIPT_DIR
    Write-Info "Installing from local source at $SOURCE_DIR"
} else {
    Write-Info "Cloning Nullion from GitHub..."
    Ensure-Git
    $SOURCE_DIR = Join-Path $NULLION_DIR "src"
    Push-Location $NULLION_DIR
    try {
        if (Test-Path (Join-Path $SOURCE_DIR ".git")) {
            git -C $SOURCE_DIR remote set-url origin $REPO_URL 2>$null
            Checkout-InstallTarget $SOURCE_DIR
        } else {
            git clone --quiet $REPO_URL $SOURCE_DIR
            Write-Ok "Cloned."
            Checkout-InstallTarget $SOURCE_DIR
        }
    } finally {
        Pop-Location
    }
}

$VENV_PYTHON = Join-Path $NULLION_VENV_DIR "Scripts\python.exe"
if (Test-Path $NULLION_VENV_DIR) {
    $recreateVenv = $false
    if (-not (Test-Path $VENV_PYTHON)) {
        Write-Info "Existing virtual environment is incomplete. Recreating it."
        $recreateVenv = $true
    } else {
        $venvVersion = & $VENV_PYTHON --version 2>&1
        if (-not (Test-SupportedPythonVersionText $venvVersion)) {
            Write-Info "Existing virtual environment uses unsupported Python ($venvVersion). Recreating it."
            $recreateVenv = $true
        }
    }
    if ($recreateVenv) {
        Remove-Item -Recurse -Force $NULLION_VENV_DIR
    }
}

if (-not (Test-Path $NULLION_VENV_DIR)) {
    Write-Info "Creating virtual environment..."
    Invoke-Python $PYTHON -m venv $NULLION_VENV_DIR
    Write-Ok "Virtual environment created."
}

$NULLION_SCRIPTS_DIR = Join-Path $NULLION_VENV_DIR "Scripts"
$NULLION_EXE = Join-Path $NULLION_SCRIPTS_DIR "nullion-web.exe"
$NULLION_TRAY_EXE = Join-Path $NULLION_SCRIPTS_DIR "nullion-tray.exe"
$NULLION_TELEGRAM_EXE = Join-Path $NULLION_SCRIPTS_DIR "nullion-telegram.exe"
$NULLION_SLACK_EXE = Join-Path $NULLION_SCRIPTS_DIR "nullion-slack.exe"
$NULLION_DISCORD_EXE = Join-Path $NULLION_SCRIPTS_DIR "nullion-discord.exe"

try {
    Write-Info "Installing dependencies (this may take a minute)..."
    & $VENV_PYTHON -m ensurepip --upgrade
    if ($LASTEXITCODE -ne 0) { throw "Could not bootstrap pip in the virtual environment." }
    & $VENV_PYTHON -m pip install --quiet --no-cache-dir --upgrade pip
    if ($LASTEXITCODE -ne 0) { throw "Could not upgrade pip in the virtual environment." }
    Install-WindowsProxyToolsWheel
    & $VENV_PYTHON -m pip install --quiet --no-cache-dir -e $SOURCE_DIR
    if ($LASTEXITCODE -ne 0) { throw "Nullion dependency install failed." }
    & $VENV_PYTHON -c "import PIL; import pypdf"
    if ($LASTEXITCODE -ne 0) { throw "PDF runtime dependency check failed." }
} catch {
    if (-not (Invoke-FreshResetAfterInstallFailure $_.Exception.Message)) {
        throw
    }
}
Test-NullionInstalledEntrypoints
Write-Ok "Nullion installed."
Add-UserPathEntry $NULLION_SCRIPTS_DIR

[void](Install-PlaywrightRuntime)

$NULLION_KEY_STORAGE = Choose-KeyStorage
Initialize-KeyStorage $NULLION_KEY_STORAGE

# Step 3: Capabilities
Write-Header "Step 3 of 4 - Capabilities (optional)"

Write-Host ""
Write-Host "  Nullion's web dashboard runs at http://localhost:$NULLION_WEB_PORT - no setup needed."
Write-Host ""
Write-Host "  Next you can enable optional capabilities: messaging apps, AI provider,"
Write-Host "  browser/search access, account/API tools, media tools, and skill packs."
Write-Host ""
Write-Host "  First, choose any messaging apps you want to connect."
Write-Host ""

$BOT_TOKEN        = ""
$CHAT_ID          = ""
$TELEGRAM_ENABLED = $false
$SLACK_ENABLED = $false
$SLACK_BOT_TOKEN = ""
$SLACK_APP_TOKEN = ""
$SLACK_SIGNING_SECRET = ""
$SLACK_OPERATOR_USER_ID = ""
$DISCORD_ENABLED = $false
$DISCORD_BOT_TOKEN = ""
$SKIP_MESSAGING_SETUP = $false

$ExistingTelegramToken = Get-EnvValue "NULLION_TELEGRAM_BOT_TOKEN"
$ExistingTelegramChatId = Get-EnvValue "NULLION_TELEGRAM_OPERATOR_CHAT_ID"
$ExistingTelegramEnabled = Get-EnvValue "NULLION_TELEGRAM_CHAT_ENABLED"
$ExistingSlackEnabled = Get-EnvValue "NULLION_SLACK_ENABLED"
$ExistingSlackBotToken = Get-EnvValue "NULLION_SLACK_BOT_TOKEN"
$ExistingSlackAppToken = Get-EnvValue "NULLION_SLACK_APP_TOKEN"
$ExistingSlackSigningSecret = Get-EnvValue "NULLION_SLACK_SIGNING_SECRET"
$ExistingSlackOperatorUserId = Get-EnvValue "NULLION_SLACK_OPERATOR_USER_ID"
$ExistingDiscordEnabled = Get-EnvValue "NULLION_DISCORD_ENABLED"
$ExistingDiscordBotToken = Get-EnvValue "NULLION_DISCORD_BOT_TOKEN"

if ("$ExistingTelegramToken$ExistingSlackBotToken$ExistingDiscordBotToken") {
    $ExistingMessagingSummary = Get-ExistingMessagingSummary
    if (-not $ExistingMessagingSummary) { $ExistingMessagingSummary = "configured" }
    Write-Info "Found existing messaging settings in ${NULLION_ENV_FILE}: $ExistingMessagingSummary."
    if (Confirm-PromptDefaultYes "Use existing messaging setup instead of setting it up again?") {
        $SKIP_MESSAGING_SETUP = $true
        if (($ExistingTelegramEnabled -eq "true") -or "$ExistingTelegramToken$ExistingTelegramChatId") {
            $TELEGRAM_ENABLED = $true
            $BOT_TOKEN = $ExistingTelegramToken
            $CHAT_ID = $ExistingTelegramChatId
        }
        if (($ExistingSlackEnabled -eq "true") -or "$ExistingSlackBotToken$ExistingSlackAppToken") {
            $SLACK_ENABLED = $true
            $SLACK_BOT_TOKEN = $ExistingSlackBotToken
            $SLACK_APP_TOKEN = $ExistingSlackAppToken
            $SLACK_SIGNING_SECRET = $ExistingSlackSigningSecret
            $SLACK_OPERATOR_USER_ID = $ExistingSlackOperatorUserId
        }
        if (($ExistingDiscordEnabled -eq "true") -or $ExistingDiscordBotToken) {
            $DISCORD_ENABLED = $true
            $DISCORD_BOT_TOKEN = $ExistingDiscordBotToken
        }
        Write-Ok "Using existing messaging setup."
    }
}

if (-not $SKIP_MESSAGING_SETUP) {
Write-Host "  Choose messaging apps to configure:" -ForegroundColor White
Write-MenuItem "1" "Telegram" "Best mobile setup, voice notes, and direct chats" "[recommended]"
Write-MenuItem "2" "Slack" "Team workspace messaging through Slack Socket Mode"
Write-MenuItem "3" "Discord" "Server/community bot with Message Content intent"
Write-MenuItem "4" "Skip" "Set up messaging later from the web dashboard"
Write-Host ""
$MESSAGING_CHOICES = (Read-Host "  Select one or more [1]").Trim().ToLower() -replace '\s',''
if (-not $MESSAGING_CHOICES) { $MESSAGING_CHOICES = "1" }

if ($MESSAGING_CHOICES -match '4|skip|none') {
    $MESSAGING_CHOICES = ""
    Write-Ok "Skipped messaging apps. You can set them up later from the web dashboard at http://localhost:$NULLION_WEB_PORT"
}

if (($MESSAGING_CHOICES -match '1') -or ($MESSAGING_CHOICES -match 'telegram')) {
    $TELEGRAM_ENABLED = $true

    # Load existing config if present
    $ExistingToken  = ""
    $ExistingChatId = ""
    if (Test-Path $NULLION_ENV_FILE) {
        $envContent = Get-Content $NULLION_ENV_FILE -Raw
        if ($envContent -match 'NULLION_TELEGRAM_BOT_TOKEN="?([^"\r\n]+)"?') {
            $ExistingToken = $Matches[1]
        }
        if ($envContent -match 'NULLION_TELEGRAM_OPERATOR_CHAT_ID="?([^"\r\n]+)"?') {
            $ExistingChatId = $Matches[1]
        }
    }

    Write-Host ""
    Write-Host "  You need a Telegram bot token. Here's how to get one in ~2 minutes:"
    Write-Host ""
    Write-Host "  1. Open Telegram and search for @BotFather" -ForegroundColor White
    Write-Host "  2. Send: /newbot" -ForegroundColor White
    Write-Host "  3. Give your bot a name (e.g. `"My Nullion`")" -ForegroundColor White
    Write-Host "  4. Give it a username ending in 'bot' (e.g. `"my_nullion_bot`")" -ForegroundColor White
    Write-Host "  5. BotFather will send you a token that looks like:" -ForegroundColor White
    Write-Host "       1234567890:ABCdef..." -ForegroundColor Yellow
    Write-Host ""

    if ($ExistingToken) {
        Write-Info "Existing token found: $(Format-MaskedSecret $ExistingToken)"
        if (Confirm-Prompt "Keep this token?") {
            $BOT_TOKEN = $ExistingToken
        }
    }

    if (-not $BOT_TOKEN) {
        while ($true) {
            $secureBotToken = Read-Host "  Paste your bot token here (hidden)" -AsSecureString
            $BOT_TOKEN = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureBotToken))
            $BOT_TOKEN = $BOT_TOKEN.Trim()
            if ($BOT_TOKEN -match '^\d{6,}:[A-Za-z0-9_\-]{20,}$') {
                Write-Ok "Token format looks good."
                break
            }
            Write-Err "That doesn't look like a valid bot token. It should match: 123456789:ABCdef..."
        }
    }

    Write-Host ""
    Write-Host "  Now we need your Telegram chat ID so the bot knows who to talk to."
    Write-Host ""
    Write-Host "  1. In Telegram, search for @userinfobot" -ForegroundColor White
    Write-Host "  2. Open it and send: /start" -ForegroundColor White
    Write-Host "  3. Copy the numeric Id/User ID it replies with." -ForegroundColor White
    Write-Host "     That number is your chat ID." -ForegroundColor White
    Write-Host ""

    if ($ExistingChatId) {
        Write-Info "Existing chat ID found: $ExistingChatId"
        if (Confirm-Prompt "Keep this chat ID?") {
            $CHAT_ID = $ExistingChatId
        }
    }

    if (-not $CHAT_ID) {
        while ($true) {
            $CHAT_ID = (Read-Host "  Enter your Telegram chat ID (numbers only)").Trim() -replace '\s',''
            if ($CHAT_ID -match '^-?\d+$') {
                Write-Ok "Chat ID: $CHAT_ID"
                break
            }
            Write-Err "That doesn't look right - it should be a number like 123456789."
        }
    }
}

if (($MESSAGING_CHOICES -match '2') -or ($MESSAGING_CHOICES -match 'slack')) {
    $SLACK_ENABLED = $true
    Write-Host ""
    Write-Host "  Slack setup" -ForegroundColor White
    Write-Host "  Create a Slack app with Socket Mode enabled, then add bot and app-level tokens."
    Write-Host "  Required: bot token (xoxb-...) and app-level token (xapp-...)."
    Write-Host ""
    while ($true) {
        $SLACK_BOT_TOKEN = (Read-Host "  Slack bot token (xoxb-...)").Trim()
        if ($SLACK_BOT_TOKEN.StartsWith("xoxb-")) { break }
        Write-Err "That should start with xoxb-."
    }
    while ($true) {
        $SLACK_APP_TOKEN = (Read-Host "  Slack app-level token (xapp-...)").Trim()
        if ($SLACK_APP_TOKEN.StartsWith("xapp-")) { break }
        Write-Err "That should start with xapp-."
    }
    $SLACK_SIGNING_SECRET = (Read-Host "  Slack signing secret (optional)").Trim()
    $SLACK_OPERATOR_USER_ID = (Read-Host "  Operator Slack user ID (optional, e.g. U012ABCDEF)").Trim()
    Write-Ok "Slack messaging configured."
}

if (($MESSAGING_CHOICES -match '3') -or ($MESSAGING_CHOICES -match 'discord')) {
    $DISCORD_ENABLED = $true
    Write-Host ""
    Write-Host "  Discord setup" -ForegroundColor White
    Write-Host "  Create a Discord application bot, enable Message Content intent, and paste its token."
    Write-Host ""
    while ($true) {
        $DISCORD_BOT_TOKEN = (Read-Host "  Discord bot token").Trim()
        if ($DISCORD_BOT_TOKEN) { break }
        Write-Err "Discord needs a bot token."
    }
    Write-Ok "Discord messaging configured."
}
}

Save-MessagingCheckpoint

# Model provider
Write-Host ""
Write-Host "  Choose your AI provider:" -ForegroundColor White
$ANTHROPIC_KEY   = ""
$OPENAI_KEY      = ""
$MODEL_PROVIDER  = ""
$MODEL_BASE_URL  = ""
$MODEL_NAME      = ""
$SKIP_PROVIDER_SETUP = $false

$ExistingModelProvider = Get-EnvValue "NULLION_MODEL_PROVIDER"
$ExistingModelBaseUrl = Get-EnvValue "NULLION_OPENAI_BASE_URL"
$ExistingModelName = Get-EnvValue "NULLION_MODEL"
$ExistingOpenAiKey = Get-EnvValue "OPENAI_API_KEY"
$ExistingAnthropicKey = Get-EnvValue "ANTHROPIC_API_KEY"
$ExistingStoredProvider = Get-StoredCredentialValue "provider"
$ExistingStoredKey = Get-StoredCredentialValue "api_key_prefix"
if ("$ExistingModelProvider$ExistingModelName$ExistingOpenAiKey$ExistingAnthropicKey") {
    Write-Ok "Found existing AI provider settings: $(Get-ExistingAiProviderSummary)"
    if (Confirm-PromptDefaultYes "Use existing AI provider setup instead of setting it up again?") {
        $SKIP_PROVIDER_SETUP = $true
        $MODEL_PROVIDER = $ExistingModelProvider
        $MODEL_BASE_URL = $ExistingModelBaseUrl
        $MODEL_NAME = $ExistingModelName
        $OPENAI_KEY = $ExistingOpenAiKey
        $ANTHROPIC_KEY = $ExistingAnthropicKey
        Write-Ok "Using existing AI provider setup."
    }
}

if ((-not $SKIP_PROVIDER_SETUP) -and $ExistingStoredProvider -and $ExistingStoredKey) {
    Write-Ok "Found existing encrypted credentials for: $ExistingStoredProvider"
    if (Confirm-PromptDefaultYes "Keep existing credentials and skip provider setup?") {
        $SKIP_PROVIDER_SETUP = $true
        $MODEL_PROVIDER = $ExistingStoredProvider
        $MODEL_BASE_URL = Get-StoredCredentialValue "base_url"
        $MODEL_NAME = Get-StoredCredentialValue "model"
        $OPENAI_KEY = Get-StoredCredentialValue "api_key"
        Write-Ok "Using existing encrypted credentials."
    }
}

if (-not $SKIP_PROVIDER_SETUP) {
Write-MenuItem "1" "OpenAI" "GPT-5.5, GPT-4.5, GPT-4o, o4-mini..." "[recommended]"
Write-MenuItem "2" "Anthropic" "Claude Opus 4.6, Sonnet 4.6..."
Write-MenuItem "3" "OpenRouter" "GPT, Gemini, Llama, Claude, DeepSeek, and many more" "[broadest]"
Write-MenuItem "4" "Google Gemini" "Gemini models through the OpenAI-compatible API"
Write-MenuItem "5" "Ollama local" "OpenAI-compatible localhost endpoint; private and low-cost"
Write-MenuItem "6" "Groq" "Fast hosted inference"
Write-MenuItem "7" "Mistral" "Mistral and Pixtral models"
Write-MenuItem "8" "DeepSeek" "DeepSeek chat and reasoning models"
Write-MenuItem "9" "xAI" "Grok models"
Write-MenuItem "10" "Together AI" "Open-source model hosting"
Write-MenuItem "11" "Local / custom endpoint" "vLLM, LM Studio, LiteLLM, or any compatible URL"
Write-Host ""
$providerChoice = Read-Host "  Enter 1-11"
switch ($providerChoice) {
    "1" {
        $MODEL_PROVIDER = "openai"
        $MODEL_NAME = "gpt-5.5"
        Write-Host ""
        Write-Host "  How would you like to authenticate with OpenAI?" -ForegroundColor White
        Write-MenuItem "1" "API key" "Paste a key from platform.openai.com"
        Write-MenuItem "2" "OAuth" "Sign in with your OpenAI account in the browser"
        Write-Host ""
        $openAiAuthChoice = Read-Host "  Enter 1 or 2"
        Write-Host ""

        if ($openAiAuthChoice -eq "2") {
            Write-Info "Opening your browser for OpenAI sign-in..."
            $tokenFile = [System.IO.Path]::GetTempFileName()
            $oldPythonPath = $env:PYTHONPATH
            try {
                $env:PYTHONPATH = Join-Path $SOURCE_DIR "src"
                & $VENV_PYTHON -m nullion.auth --write-codex-access-token $tokenFile
                if ($LASTEXITCODE -eq 0 -and (Test-Path $tokenFile)) {
                    $token = (Get-Content -Raw $tokenFile).Trim()
                    if ($token) {
                        $MODEL_PROVIDER = "codex"
                        $OPENAI_KEY = $token
                        Write-Ok "Authenticated via OAuth."
                    }
                }
            } catch {
                Write-Err "OAuth failed: $_"
            } finally {
                if ($null -eq $oldPythonPath) {
                    Remove-Item Env:\PYTHONPATH -ErrorAction SilentlyContinue
                } else {
                    $env:PYTHONPATH = $oldPythonPath
                }
                Remove-Item -Force $tokenFile -ErrorAction SilentlyContinue
            }
            if (-not $OPENAI_KEY) {
                Write-Err "OAuth failed or did not return a token. Falling back to API key."
                $openAiAuthChoice = "1"
            }
        }

        if ($openAiAuthChoice -ne "2" -or -not $OPENAI_KEY) {
            Write-Host "  Get an API key at https://platform.openai.com/api-keys" -ForegroundColor Cyan
            while ($true) {
                $secure = Read-Host "  Paste your OpenAI API key (hidden)" -AsSecureString
                $plain  = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                              [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
                if ($plain -match '^sk-') {
                    $OPENAI_KEY = $plain
                    Write-Ok "Key accepted."
                    break
                }
                Write-Err "Key should start with 'sk-'. Try again."
            }
        }
        $MODEL_NAME = Read-ModelName $MODEL_NAME
    }
    "2" {
        $MODEL_PROVIDER = "anthropic"
        $MODEL_NAME = "claude-opus-4-6"
        Write-Host ""
        Write-Host "  Get an API key at https://console.anthropic.com/settings/keys" -ForegroundColor Cyan
        while ($true) {
            # Read-Host -AsSecureString hides input like a password
            $secure = Read-Host "  Paste your Anthropic API key (hidden)" -AsSecureString
            $plain  = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                          [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
            if ($plain -match '^sk-ant-') {
                $ANTHROPIC_KEY = $plain
                Write-Ok "Key accepted."
                break
            }
            Write-Err "Key should start with 'sk-ant-'. Try again."
        }
        $MODEL_NAME = Read-ModelName $MODEL_NAME
    }
    "3" {
        $MODEL_PROVIDER = "openrouter"
        $MODEL_BASE_URL = "https://openrouter.ai/api/v1"
        $MODEL_NAME = "openai/gpt-4o"
        Write-Host ""
        Write-Host "  Get an API key at https://openrouter.ai/keys" -ForegroundColor Cyan
        while ($true) {
            $secure = Read-Host "  Paste your OpenRouter API key (hidden)" -AsSecureString
            $plain  = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                          [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
            if ($plain -match '^sk-or-') {
                $OPENAI_KEY = $plain
                Write-Ok "Key accepted."
                break
            }
            Write-Err "Key should start with 'sk-or-'. Try again."
        }
        $MODEL_NAME = Read-ModelName $MODEL_NAME
    }
    "4" {
        $MODEL_PROVIDER = "gemini"
        $MODEL_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
        $MODEL_NAME = "models/gemini-2.5-flash"
        Write-Host ""
        Write-Host "  Get an API key at https://aistudio.google.com/app/apikey" -ForegroundColor Cyan
        $secure = Read-Host "  Paste your Gemini API key (hidden)" -AsSecureString
        $OPENAI_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        $MODEL_NAME = Read-ModelName $MODEL_NAME
        Write-Ok "Gemini selected."
    }
    "5" {
        $MODEL_PROVIDER = "ollama"
        $MODEL_BASE_URL = "http://127.0.0.1:11434/v1"
        $MODEL_NAME = "llama3.3"
        $OPENAI_KEY = "ollama-local"
        Write-Host ""
        Write-Info "Using Ollama's OpenAI-compatible endpoint at $MODEL_BASE_URL."
        Write-Info "Run 'ollama serve' and 'ollama pull $MODEL_NAME' if you have not already."
        $MODEL_NAME = Read-ModelName $MODEL_NAME
    }
    "6" {
        $MODEL_PROVIDER = "groq"
        $MODEL_BASE_URL = "https://api.groq.com/openai/v1"
        $MODEL_NAME = "llama-3.3-70b-versatile"
        Write-Host "  Get an API key at https://console.groq.com/keys" -ForegroundColor Cyan
        $secure = Read-Host "  Paste your Groq API key (hidden)" -AsSecureString
        $OPENAI_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        $MODEL_NAME = Read-ModelName $MODEL_NAME
    }
    "7" {
        $MODEL_PROVIDER = "mistral"
        $MODEL_BASE_URL = "https://api.mistral.ai/v1"
        $MODEL_NAME = "mistral-large-latest"
        Write-Host "  Get an API key at https://console.mistral.ai/api-keys/" -ForegroundColor Cyan
        $secure = Read-Host "  Paste your Mistral API key (hidden)" -AsSecureString
        $OPENAI_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        $MODEL_NAME = Read-ModelName $MODEL_NAME
    }
    "8" {
        $MODEL_PROVIDER = "deepseek"
        $MODEL_BASE_URL = "https://api.deepseek.com/v1"
        $MODEL_NAME = "deepseek-chat"
        Write-Host "  Get an API key at https://platform.deepseek.com/api_keys" -ForegroundColor Cyan
        $secure = Read-Host "  Paste your DeepSeek API key (hidden)" -AsSecureString
        $OPENAI_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        $MODEL_NAME = Read-ModelName $MODEL_NAME
    }
    "9" {
        $MODEL_PROVIDER = "xai"
        $MODEL_BASE_URL = "https://api.x.ai/v1"
        $MODEL_NAME = "grok-4"
        Write-Host "  Get an API key at https://console.x.ai/" -ForegroundColor Cyan
        $secure = Read-Host "  Paste your xAI API key (hidden)" -AsSecureString
        $OPENAI_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        $MODEL_NAME = Read-ModelName $MODEL_NAME
    }
    "10" {
        $MODEL_PROVIDER = "together"
        $MODEL_BASE_URL = "https://api.together.xyz/v1"
        $MODEL_NAME = "meta-llama/Llama-3.3-70B-Instruct-Turbo"
        Write-Host "  Get an API key at https://api.together.xyz/settings/api-keys" -ForegroundColor Cyan
        $secure = Read-Host "  Paste your Together API key (hidden)" -AsSecureString
        $OPENAI_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        $MODEL_NAME = Read-ModelName $MODEL_NAME
    }
    "11" {
        $MODEL_PROVIDER = "custom"
        Write-Host ""
        $MODEL_BASE_URL = (Read-Host "  OpenAI-compatible base URL (e.g. http://localhost:1234/v1)").Trim()
        $MODEL_NAME = (Read-Host "  Model name").Trim()
        if (Confirm-Prompt "Does this endpoint require an API key?") {
            $secure = Read-Host "  Paste API key (hidden)" -AsSecureString
            $OPENAI_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        } else {
            $OPENAI_KEY = "local"
        }
    }
    default {
        $MODEL_PROVIDER = "custom"
        Write-Info "You can finish model setup later in the web dashboard Settings."
    }
}
}

Save-ProviderCheckpoint

# Browser setup
Write-Host ""
Write-Host "  Would you like Nullion to control a browser?" -ForegroundColor White
Write-Host "  This lets it browse the web, fill forms, and take screenshots on your behalf."
Write-Host ""
$BROWSER_BACKEND = ""
$BROWSER_CDP_URL = ""
$BROWSER_PREFERRED = ""
$BrowserNote = ""
$SKIP_BROWSER_SETUP = $false

$ExistingBrowserBackend = Get-EnvValue "NULLION_BROWSER_BACKEND"
$ExistingBrowserCdpUrl = Get-EnvValue "NULLION_BROWSER_CDP_URL"
$ExistingBrowserPreferred = Get-EnvValue "NULLION_BROWSER_PREFERRED"
if ("$ExistingBrowserBackend$ExistingBrowserCdpUrl$ExistingBrowserPreferred") {
    Write-Info "Found existing browser setup: $ExistingBrowserPreferred$ExistingBrowserBackend"
    if (Confirm-PromptDefaultYes "Use existing browser setup instead of setting it up again?") {
        $SKIP_BROWSER_SETUP = $true
        $BROWSER_BACKEND = $ExistingBrowserBackend
        $BROWSER_CDP_URL = $ExistingBrowserCdpUrl
        $BROWSER_PREFERRED = $ExistingBrowserPreferred
        Write-Ok "Using existing browser setup."
    }
}

if (-not $SKIP_BROWSER_SETUP) {
$BraveStatus = Get-BrowserStatusLabel "brave"
$ChromeStatus = Get-BrowserStatusLabel "chrome"
Write-MenuItem "1" "Attach to Brave" "Uses your existing Brave window ($BraveStatus)" "[recommended]"
Write-MenuItem "2" "Attach to Chrome" "Uses your existing Chrome window ($ChromeStatus)"
Write-MenuItem "3" "Headless" "Invisible Chromium running in the background"
Write-MenuItem "4" "None" "No browser access"
Write-Host ""
$BrowserChoice = Read-Host "  Enter 1, 2, 3, or 4"
switch ($BrowserChoice) {
    "1" {
        $BROWSER_BACKEND = "auto"
        $BROWSER_CDP_URL = "http://localhost:9222"
        $BROWSER_PREFERRED = "brave"
        Write-Ok "Brave selected."
        if (-not (Test-BrowserInstalled "brave")) {
            Write-Info "Brave was not detected. Install Brave or choose another browser if attach fails."
        }
        $BrowserNote = "Browser automation will attach to Brave on port 9222 if available, otherwise Nullion will open a visible automation window."
    }
    "2" {
        $BROWSER_BACKEND = "auto"
        $BROWSER_CDP_URL = "http://localhost:9222"
        $BROWSER_PREFERRED = "chrome"
        Write-Ok "Chrome selected."
        if (-not (Test-BrowserInstalled "chrome")) {
            Write-Info "Chrome was not detected. Install Chrome or choose another browser if attach fails."
        }
        $BrowserNote = "Browser automation will attach to Chrome on port 9222 if available, otherwise Nullion will open a visible automation window."
    }
    "3" {
        $BROWSER_BACKEND = "playwright"
        if (Install-PlaywrightRuntime) {
            Write-Ok "Headless browser ready."
        } else {
            Write-Info "Headless browser selected. Install Playwright Chromium later if browser automation fails."
        }
    }
    default {
        Write-Info "No browser - skipped."
    }
}
}

Save-BrowserCheckpoint

# Search provider setup
Write-Host ""
Write-Host "  Choose your search provider:" -ForegroundColor White
$SEARCH_PROVIDER = "builtin_search_provider"
$BRAVE_SEARCH_KEY = ""
$GOOGLE_SEARCH_KEY = ""
$GOOGLE_SEARCH_CX = ""
$PERPLEXITY_SEARCH_KEY = ""
$SKIP_SEARCH_SETUP = $false

$ExistingProviderBindings = Get-EnvValue "NULLION_PROVIDER_BINDINGS"
$ExistingBraveSearchKey = Get-EnvValue "NULLION_BRAVE_SEARCH_API_KEY"
$ExistingGoogleSearchKey = Get-EnvValue "NULLION_GOOGLE_SEARCH_API_KEY"
$ExistingGoogleSearchCx = Get-EnvValue "NULLION_GOOGLE_SEARCH_CX"
$ExistingPerplexitySearchKey = Get-EnvValue "NULLION_PERPLEXITY_API_KEY"
if ($ExistingProviderBindings -match 'search_plugin=([^,]+)') {
    $ExistingSearchProvider = $Matches[1]
    Write-Info "Found existing search provider: $ExistingSearchProvider"
    if (Confirm-PromptDefaultYes "Use existing search setup instead of setting it up again?") {
        $SKIP_SEARCH_SETUP = $true
        $SEARCH_PROVIDER = $ExistingSearchProvider
        $BRAVE_SEARCH_KEY = $ExistingBraveSearchKey
        $GOOGLE_SEARCH_KEY = $ExistingGoogleSearchKey
        $GOOGLE_SEARCH_CX = $ExistingGoogleSearchCx
        $PERPLEXITY_SEARCH_KEY = $ExistingPerplexitySearchKey
        Write-Ok "Using existing search setup."
    }
}

if (-not $SKIP_SEARCH_SETUP) {
Write-MenuItem "1" "Built-in local adapter" "Default search/fetch behavior; no extra key" "[default]"
Write-MenuItem "2" "Brave Search API" "Independent web index"
Write-MenuItem "3" "Google Custom Search API" "Requires API key plus search engine ID"
Write-MenuItem "4" "Perplexity Search API" "Ranked AI-oriented web results"
Write-MenuItem "5" "DuckDuckGo Instant Answers" "Keyless, but not full web search"
Write-Host ""
$SearchChoice = Read-Host "  Enter 1, 2, 3, 4, or 5"
switch ($SearchChoice) {
    "2" {
        $SEARCH_PROVIDER = "brave_search_provider"
        Write-Host "  Get a key at https://api-dashboard.search.brave.com/" -ForegroundColor Cyan
        $secure = Read-Host "  Paste your Brave Search API key (hidden)" -AsSecureString
        $BRAVE_SEARCH_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        Write-Ok "Brave Search selected."
    }
    "3" {
        $SEARCH_PROVIDER = "google_custom_search_provider"
        Write-Host "  Custom Search docs: https://developers.google.com/custom-search/v1/overview" -ForegroundColor Cyan
        $secure = Read-Host "  Paste your Google Search API key (hidden)" -AsSecureString
        $GOOGLE_SEARCH_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        $GOOGLE_SEARCH_CX = Read-Host "  Paste your Programmable Search Engine ID (cx)"
        Write-Ok "Google Custom Search selected."
    }
    "4" {
        $SEARCH_PROVIDER = "perplexity_search_provider"
        Write-Host "  Get a key at https://www.perplexity.ai/settings/api" -ForegroundColor Cyan
        $secure = Read-Host "  Paste your Perplexity API key (hidden)" -AsSecureString
        $PERPLEXITY_SEARCH_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
        Write-Ok "Perplexity Search selected."
    }
    "5" {
        $SEARCH_PROVIDER = "duckduckgo_instant_answer_provider"
        Write-Ok "DuckDuckGo Instant Answers selected."
    }
    default {
        Write-Ok "Built-in search selected."
    }
}
}

Save-SearchCheckpoint

# Account / API tools setup
Write-Host ""
Write-Host "  Choose account/API tools to enable:" -ForegroundColor White
Write-Host "  These add account-aware tools. Native support is available for Gmail/Google"
Write-Host "  Calendar; connector gateways can bridge other apps when they expose a"
Write-Host "  compatible HTTP API."
Write-Host ""

$EMAIL_CALENDAR_ENABLED = $false
$MATON_CONNECTOR_ENABLED = $false
$CONNECTOR_SKILLS_ENABLED = $false
$CUSTOM_EMAIL_API_ENABLED = $false
$MATON_API_KEY = Get-EnvValue "MATON_API_KEY"
$COMPOSIO_API_KEY = Get-EnvValue "COMPOSIO_API_KEY"
$NANGO_SECRET_KEY = Get-EnvValue "NANGO_SECRET_KEY"
$ACTIVEPIECES_API_KEY = Get-EnvValue "ACTIVEPIECES_API_KEY"
$N8N_API_KEY = Get-EnvValue "N8N_API_KEY"
$N8N_BASE_URL = Get-EnvValue "N8N_BASE_URL"
$CUSTOM_API_BASE_URL = Get-EnvValue "NULLION_CUSTOM_API_BASE_URL"
$CUSTOM_API_TOKEN = Get-EnvValue "NULLION_CUSTOM_API_TOKEN"
$ExistingEnabledPlugins = Get-EnvValue "NULLION_ENABLED_PLUGINS"
$ExistingConnectorGateway = Get-EnvValue "NULLION_CONNECTOR_GATEWAY"
if ("$MATON_API_KEY$COMPOSIO_API_KEY$NANGO_SECRET_KEY$ACTIVEPIECES_API_KEY$N8N_API_KEY$ExistingConnectorGateway") {
    $MATON_CONNECTOR_ENABLED = $true
    $CONNECTOR_SKILLS_ENABLED = $true
}
if (",$ExistingEnabledPlugins," -match ',(email_plugin|calendar_plugin),' -or $CONNECTOR_SKILLS_ENABLED) {
    Write-Info "Found existing account/API tools setup."
    if (Confirm-PromptDefaultYes "Use existing account/API setup instead of setting it up again?") {
        if ((Get-EnvValue "NULLION_PROVIDER_BINDINGS") -match 'email_plugin=custom_api_provider') {
            $CUSTOM_EMAIL_API_ENABLED = $true
        } elseif (",$ExistingEnabledPlugins," -match ',(email_plugin|calendar_plugin),') {
            $EMAIL_CALENDAR_ENABLED = $true
        }
        if ("$MATON_API_KEY$COMPOSIO_API_KEY$NANGO_SECRET_KEY$ACTIVEPIECES_API_KEY$N8N_API_KEY$ExistingConnectorGateway") {
            $MATON_CONNECTOR_ENABLED = $true
            $CONNECTOR_SKILLS_ENABLED = $true
        }
        Write-Ok "Using existing account/API setup."
    }
}

if ((-not $EMAIL_CALENDAR_ENABLED) -and (-not $CUSTOM_EMAIL_API_ENABLED) -and (-not $MATON_CONNECTOR_ENABLED)) {
    Write-MenuItem "1" "Gmail / Google Calendar" "Local setup with Himalaya plus the Google API wrapper" "[recommended]"
    Write-MenuItem "2" "Connector skill credentials" "Maton, Composio, Nango, Activepieces, n8n, or custom gateway"
    Write-MenuItem "3" "Custom email API bridge" "Bind Nullion email tools to your own HTTP bridge"
    Write-MenuItem "4" "Skip" "Set up account/API tools later in the web UI"
    Write-Host ""
    $AccountToolsChoice = Read-Host "  Enter 1, 2, 3, or 4 [4]"
    if (-not $AccountToolsChoice) { $AccountToolsChoice = "4" }
    switch ($AccountToolsChoice) {
        "1" {
            $EMAIL_CALENDAR_ENABLED = $true
            $himalayaCmd = Get-Command "himalaya" -ErrorAction SilentlyContinue
            if ($himalayaCmd) {
                $himalayaVersion = (& himalaya --version 2>$null | Select-Object -First 1)
                Write-Ok "Found Himalaya: $himalayaVersion"
            } else {
                Write-Info "Himalaya is not installed on this machine."
                if (Get-Command "scoop" -ErrorAction SilentlyContinue) {
                    if (Confirm-Prompt "Install Himalaya now with Scoop?") {
                        scoop install himalaya
                        Write-Ok "Himalaya installed."
                    } else {
                        Write-Info "Skipped Himalaya install. Install it later with: scoop install himalaya"
                    }
                } else {
                    Write-Info "Install Himalaya later from https://github.com/pimalaya/himalaya"
                    Write-Info "Then configure a Gmail account profile and add it in Settings -> Users -> Workspace connections."
                }
            }
            Write-Host ""
            Write-Host "  After Himalaya has a Gmail account profile, open:"
            Write-Host "    Settings -> Users -> Workspace connections"
            Write-Host "  Then add a Gmail / Google Workspace connection using that profile name."
            Write-Ok "Email/calendar plugins will be enabled."
        }
        "2" {
            $MATON_CONNECTOR_ENABLED = $true
            $CONNECTOR_SKILLS_ENABLED = $true
            Write-Host ""
            Write-Host "  Connector skills are broad workflow guidance for SaaS/API gateways."
            Write-Host "  They do not grant access by themselves; setup saves credentials for the"
            Write-Host "  connector or MCP tools you choose to use."
            Write-MenuItem "1" "Maton" "API gateway and MCP toolkit for many SaaS apps" "[recommended]"
            Write-MenuItem "2" "Composio" "MCP/direct API toolkits for connected apps"
            Write-MenuItem "3" "Nango" "Open-source OAuth and integration platform"
            Write-MenuItem "4" "Activepieces" "Open-source automation pieces"
            Write-MenuItem "5" "n8n" "Self-hostable workflow automation"
            Write-MenuItem "6" "Skip credentials" "Enable the connector skills only"
            Write-Host ""
            $ConnectorChoices = Read-Host "  Select one or more [1]"
            if (-not $ConnectorChoices) { $ConnectorChoices = "1" }
            $ConnectorChoices = "," + (($ConnectorChoices -replace '[^0-9]+', ',').Trim(',')) + ","
            if ($ConnectorChoices -match ',1,') {
                $secure = Read-Host "  Maton API key (hidden)" -AsSecureString
                $MATON_API_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
            }
            if ($ConnectorChoices -match ',2,') {
                $secure = Read-Host "  Composio API key (hidden)" -AsSecureString
                $COMPOSIO_API_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
            }
            if ($ConnectorChoices -match ',3,') {
                $secure = Read-Host "  Nango secret key (hidden)" -AsSecureString
                $NANGO_SECRET_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
            }
            if ($ConnectorChoices -match ',4,') {
                $secure = Read-Host "  Activepieces API key (hidden)" -AsSecureString
                $ACTIVEPIECES_API_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
            }
            if ($ConnectorChoices -match ',5,') {
                $N8N_BASE_URL = (Read-Host "  n8n base URL (e.g. http://localhost:5678)").Trim()
                $secure = Read-Host "  n8n API key (hidden)" -AsSecureString
                $N8N_API_KEY = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
            }
            Write-Ok "Connector/API skill pack will be enabled."
        }
        "3" {
            $CUSTOM_EMAIL_API_ENABLED = $true
            Write-Host ""
            Write-Host "  Nullion's custom email provider expects:"
            Write-Host "    GET /email/search?q=...&limit=..."
            Write-Host "    GET /email/read/{id}"
            Write-Host "  A bridge can call Maton, Composio, n8n, Activepieces, Nango, or any API behind those endpoints."
            $CUSTOM_API_BASE_URL = (Read-Host "  Custom API base URL").Trim()
            $secure = Read-Host "  Custom API bearer token (hidden)" -AsSecureString
            $CUSTOM_API_TOKEN = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure))
            Write-Ok "Custom email API tools will be enabled."
        }
        default {
            Write-Info "Skipped account/API tools. You can easily enable them later in the web UI."
        }
    }
}

Save-AccountCheckpoint

# Local media tools setup
Write-Host ""
Write-Host "  Configure media tools?" -ForegroundColor White
Write-Host "  We'll set these up separately so local tools are used where they are cheap"
Write-Host "  and fast, while image/video AI can use your current provider or a media provider."
Write-Host ""

$MEDIA_ENABLED = $false
$AUDIO_TRANSCRIBE_COMMAND = ""
$IMAGE_OCR_COMMAND = ""
$IMAGE_GENERATE_COMMAND = ""
$MEDIA_OPENAI_KEY = ""
$MEDIA_ANTHROPIC_KEY = ""
$MEDIA_OPENROUTER_KEY = ""
$MEDIA_GEMINI_KEY = ""
$MEDIA_GROQ_KEY = ""
$MEDIA_MISTRAL_KEY = ""
$MEDIA_DEEPSEEK_KEY = ""
$MEDIA_XAI_KEY = ""
$MEDIA_TOGETHER_KEY = ""
$MEDIA_CUSTOM_KEY = ""
$MEDIA_CUSTOM_BASE_URL = ""
$AUDIO_TRANSCRIBE_PROVIDER = ""
$AUDIO_TRANSCRIBE_MODEL = ""
$AUDIO_TRANSCRIBE_ENABLED = $false
$IMAGE_OCR_PROVIDER = ""
$IMAGE_OCR_MODEL = ""
$IMAGE_OCR_ENABLED = $false
$IMAGE_GENERATE_PROVIDER = ""
$IMAGE_GENERATE_MODEL = ""
$IMAGE_GENERATE_ENABLED = $false
$VIDEO_INPUT_PROVIDER = ""
$VIDEO_INPUT_MODEL = ""
$VIDEO_INPUT_ENABLED = $false
Write-Info "Local media packages will only be installed if you choose local audio/OCR setup."

if (",$ExistingEnabledPlugins," -match ',media_plugin,') {
    Write-Info "Found existing media tools setup."
    if (Confirm-PromptDefaultYes "Use existing media setup instead of setting it up again?") {
        $MEDIA_ENABLED = $true
        $MEDIA_OPENAI_KEY = Get-EnvValue "NULLION_MEDIA_OPENAI_API_KEY"
        $MEDIA_ANTHROPIC_KEY = Get-EnvValue "NULLION_MEDIA_ANTHROPIC_API_KEY"
        $MEDIA_OPENROUTER_KEY = Get-EnvValue "NULLION_MEDIA_OPENROUTER_API_KEY"
        $MEDIA_GEMINI_KEY = Get-EnvValue "NULLION_MEDIA_GEMINI_API_KEY"
        $MEDIA_GROQ_KEY = Get-EnvValue "NULLION_MEDIA_GROQ_API_KEY"
        $MEDIA_MISTRAL_KEY = Get-EnvValue "NULLION_MEDIA_MISTRAL_API_KEY"
        $MEDIA_DEEPSEEK_KEY = Get-EnvValue "NULLION_MEDIA_DEEPSEEK_API_KEY"
        $MEDIA_XAI_KEY = Get-EnvValue "NULLION_MEDIA_XAI_API_KEY"
        $MEDIA_TOGETHER_KEY = Get-EnvValue "NULLION_MEDIA_TOGETHER_API_KEY"
        $MEDIA_CUSTOM_KEY = Get-EnvValue "NULLION_MEDIA_CUSTOM_API_KEY"
        $MEDIA_CUSTOM_BASE_URL = Get-EnvValue "NULLION_MEDIA_CUSTOM_BASE_URL"
        $AUDIO_TRANSCRIBE_COMMAND = Get-EnvValue "NULLION_AUDIO_TRANSCRIBE_COMMAND"
        $IMAGE_OCR_COMMAND = Get-EnvValue "NULLION_IMAGE_OCR_COMMAND"
        $IMAGE_GENERATE_COMMAND = Get-EnvValue "NULLION_IMAGE_GENERATE_COMMAND"
        $AUDIO_TRANSCRIBE_ENABLED = (Get-EnvValue "NULLION_AUDIO_TRANSCRIBE_ENABLED") -eq "true"
        $AUDIO_TRANSCRIBE_PROVIDER = Get-EnvValue "NULLION_AUDIO_TRANSCRIBE_PROVIDER"
        $AUDIO_TRANSCRIBE_MODEL = Get-EnvValue "NULLION_AUDIO_TRANSCRIBE_MODEL"
        $IMAGE_OCR_ENABLED = (Get-EnvValue "NULLION_IMAGE_OCR_ENABLED") -eq "true"
        $IMAGE_OCR_PROVIDER = Get-EnvValue "NULLION_IMAGE_OCR_PROVIDER"
        $IMAGE_OCR_MODEL = Get-EnvValue "NULLION_IMAGE_OCR_MODEL"
        $IMAGE_GENERATE_ENABLED = (Get-EnvValue "NULLION_IMAGE_GENERATE_ENABLED") -eq "true"
        $IMAGE_GENERATE_PROVIDER = Get-EnvValue "NULLION_IMAGE_GENERATE_PROVIDER"
        $IMAGE_GENERATE_MODEL = Get-EnvValue "NULLION_IMAGE_GENERATE_MODEL"
        $VIDEO_INPUT_ENABLED = (Get-EnvValue "NULLION_VIDEO_INPUT_ENABLED") -eq "true"
        $VIDEO_INPUT_PROVIDER = Get-EnvValue "NULLION_VIDEO_INPUT_PROVIDER"
        $VIDEO_INPUT_MODEL = Get-EnvValue "NULLION_VIDEO_INPUT_MODEL"
        Write-Ok "Using existing media setup."
    }
}

if ((-not $MEDIA_ENABLED) -and (Confirm-Prompt "Configure media tools now?")) {
    if ($MODEL_PROVIDER -eq "codex" -or ($MODEL_PROVIDER -eq "openai" -and -not ([string]$OPENAI_KEY).StartsWith("sk-"))) {
        Write-Info "Codex/OpenAI OAuth works for chat sign-in, but audio transcription APIs need a provider API key or custom endpoint."
    }
    Write-Host ""
    Write-Host "  Audio transcription" -ForegroundColor White
    Write-MenuItem "1" "Local whisper.cpp" "Fast, private, no per-minute API cost" "[recommended]"
    $audioCurrentSupported = Test-MediaModelSupport "audio" $MODEL_PROVIDER $MODEL_NAME
    if ($audioCurrentSupported -and (Test-CurrentMediaModelUsable $MODEL_PROVIDER)) {
        Write-MenuItem "2" "Use connected provider/model" "$(Get-MediaProviderLabel $MODEL_PROVIDER) - $MODEL_NAME supports audio transcription"
        Write-MenuItem "3" "Add/configure API transcription provider" "OpenAI, Groq, or any OpenAI-compatible endpoint"
        Write-MenuItem "4" "Skip" "Set up audio transcription later in the web UI"
        $AudioChoice = Read-Host "  Enter 1, 2, 3, or 4"
    } else {
        Write-MenuItem "2" "Add/configure API transcription provider" "OpenAI, Groq, or any OpenAI-compatible endpoint"
        Write-MenuItem "3" "Skip" "Set up audio transcription later in the web UI"
        $AudioChoice = Read-Host "  Enter 1, 2, or 3"
    }
    switch ($AudioChoice) {
        "1" {
            $MEDIA_ENABLED = $true
            if (-not (Ensure-WhisperCppRuntime)) {
                Write-Info "Default audio transcription is not fully installed."
                if (Confirm-Prompt "Configure a custom audio transcription command now?") {
                    Write-Host "  Example: whisper-cli -m `"$WHISPER_CPP_MODEL`" -f {input} -nt"
                    $AUDIO_TRANSCRIBE_COMMAND = Read-Host "  Audio command template"
                    if ($AUDIO_TRANSCRIBE_COMMAND) { $AUDIO_TRANSCRIBE_ENABLED = $true }
                }
            }
        }
        "2" {
            $MEDIA_ENABLED = $true
            if ($audioCurrentSupported -and (Test-CurrentMediaModelUsable $MODEL_PROVIDER)) {
                $AUDIO_TRANSCRIBE_PROVIDER = $MODEL_PROVIDER
                $AUDIO_TRANSCRIBE_MODEL = $MODEL_NAME
                Write-Ok "$(Get-MediaProviderLabel $MODEL_PROVIDER) - $MODEL_NAME will be used for audio transcription."
            } else {
                $selection = Request-MediaApiProvider "audio" "Audio transcription" "openai" "gpt-4o-transcribe" $false
                $AUDIO_TRANSCRIBE_PROVIDER = $selection.provider
                $AUDIO_TRANSCRIBE_MODEL = $selection.model
            }
            $AUDIO_TRANSCRIBE_ENABLED = $true
        }
        "3" {
            if ($audioCurrentSupported -and (Test-CurrentMediaModelUsable $MODEL_PROVIDER)) {
                $MEDIA_ENABLED = $true
                $selection = Request-MediaApiProvider "audio" "Audio transcription" "openai" "gpt-4o-transcribe" $false
                $AUDIO_TRANSCRIBE_PROVIDER = $selection.provider
                $AUDIO_TRANSCRIBE_MODEL = $selection.model
                $AUDIO_TRANSCRIBE_ENABLED = $true
            }
        }
    }

    Write-Host ""
    Write-Host "  Image text extraction / OCR" -ForegroundColor White
    Write-MenuItem "1" "Local Tesseract" "Fast, private, no image API cost" "[recommended]"
    $ocrCurrentSupported = Test-MediaModelSupport "image_ocr" $MODEL_PROVIDER $MODEL_NAME
    if ($ocrCurrentSupported -and (Test-CurrentMediaModelUsable $MODEL_PROVIDER)) {
        Write-MenuItem "2" "Use current provider" "$(Get-MediaProviderLabel $MODEL_PROVIDER) - $MODEL_NAME"
        Write-MenuItem "3" "Add/configure API vision provider" "OpenAI, Anthropic, OpenRouter, Gemini, Mistral, or custom"
        Write-MenuItem "4" "Skip" "Set up image text extraction later in the web UI"
        $OcrChoice = Read-Host "  Enter 1, 2, 3, or 4"
    } else {
        Write-MenuItem "2" "Add/configure API vision provider" "OpenAI, Anthropic, OpenRouter, Gemini, Mistral, or custom"
        Write-MenuItem "3" "Skip" "Set up image text extraction later in the web UI"
        $OcrChoice = Read-Host "  Enter 1, 2, or 3"
    }
    switch ($OcrChoice) {
        "1" {
            $MEDIA_ENABLED = $true
            if (Get-Command "tesseract" -ErrorAction SilentlyContinue) {
                $IMAGE_OCR_COMMAND = "tesseract {input} stdout"
                Write-Ok "Image OCR will use tesseract."
            } else {
                Write-Info "Tesseract not found. Install it later or configure NULLION_IMAGE_OCR_COMMAND in Settings."
            }
        }
        "2" {
            $MEDIA_ENABLED = $true
            if ($ocrCurrentSupported -and (Test-CurrentMediaModelUsable $MODEL_PROVIDER)) {
                $IMAGE_OCR_PROVIDER = $MODEL_PROVIDER
                $IMAGE_OCR_MODEL = $MODEL_NAME
            } else {
                $selection = Request-MediaApiProvider "image_ocr" "Image text extraction" "openai" "gpt-4o" $false
                $IMAGE_OCR_PROVIDER = $selection.provider
                $IMAGE_OCR_MODEL = $selection.model
            }
            $IMAGE_OCR_ENABLED = $true
        }
        "3" {
            if ($ocrCurrentSupported -and (Test-CurrentMediaModelUsable $MODEL_PROVIDER)) {
                $MEDIA_ENABLED = $true
                $selection = Request-MediaApiProvider "image_ocr" "Image text extraction" "openai" "gpt-4o" $false
                $IMAGE_OCR_PROVIDER = $selection.provider
                $IMAGE_OCR_MODEL = $selection.model
                $IMAGE_OCR_ENABLED = $true
            }
        }
    }

    Write-Host ""
    Write-Host "  Image generation" -ForegroundColor White
    $imageGenCurrentSupported = Test-MediaModelSupport "image_generate" $MODEL_PROVIDER $MODEL_NAME
    $imageGenCurrentUsable = Test-CurrentMediaModelUsable $MODEL_PROVIDER
    if ($imageGenCurrentSupported -and $imageGenCurrentUsable) {
        Write-MenuItem "1" "Use current provider" "$(Get-MediaProviderLabel $MODEL_PROVIDER) - $MODEL_NAME"
        Write-MenuItem "2" "Add/configure API image generation provider" "OpenAI, OpenRouter, Gemini, xAI, Together, or custom"
        Write-MenuItem "3" "Skip" "Set up image generation later in the web UI"
        $ImageGenChoice = Read-Host "  Enter 1, 2, or 3"
    } else {
        Write-MenuItem "1" "Add/configure API image generation provider" "OpenAI, OpenRouter, Gemini, xAI, Together, or custom"
        Write-MenuItem "2" "Skip" "Set up image generation later in the web UI"
        $ImageGenChoice = Read-Host "  Enter 1 or 2"
    }
    if ($ImageGenChoice -eq "1" -and $imageGenCurrentSupported -and $imageGenCurrentUsable) {
        $MEDIA_ENABLED = $true
        $IMAGE_GENERATE_PROVIDER = $MODEL_PROVIDER
        $IMAGE_GENERATE_MODEL = $MODEL_NAME
        $IMAGE_GENERATE_ENABLED = $true
    } elseif (($ImageGenChoice -eq "2" -and $imageGenCurrentSupported -and $imageGenCurrentUsable) -or ($ImageGenChoice -eq "1" -and (-not $imageGenCurrentSupported -or -not $imageGenCurrentUsable))) {
        $MEDIA_ENABLED = $true
        $selection = Request-MediaApiProvider "image_generate" "Image generation" "openai" "gpt-image-1" $false
        $IMAGE_GENERATE_PROVIDER = $selection.provider
        $IMAGE_GENERATE_MODEL = $selection.model
        $IMAGE_GENERATE_ENABLED = $true
    }

    Write-Host ""
    Write-Host "  Video / rich image understanding" -ForegroundColor White
    $videoCurrentSupported = Test-MediaModelSupport "video" $MODEL_PROVIDER $MODEL_NAME
    if ($videoCurrentSupported -and (Test-CurrentMediaModelUsable $MODEL_PROVIDER)) {
        Write-MenuItem "1" "Use current provider" "$(Get-MediaProviderLabel $MODEL_PROVIDER) - $MODEL_NAME"
        Write-MenuItem "2" "Add/configure API vision/video provider" "OpenAI, OpenRouter, Gemini, or custom"
        Write-MenuItem "3" "Skip" "Set up video understanding later in the web UI"
        $VideoChoice = Read-Host "  Enter 1, 2, or 3"
    } else {
        Write-MenuItem "1" "Add/configure API vision/video provider" "OpenAI, OpenRouter, Gemini, or custom"
        Write-MenuItem "2" "Skip" "Set up video understanding later in the web UI"
        $VideoChoice = Read-Host "  Enter 1 or 2"
    }
    if ($VideoChoice -eq "1" -and $videoCurrentSupported -and (Test-CurrentMediaModelUsable $MODEL_PROVIDER)) {
        $MEDIA_ENABLED = $true
        $VIDEO_INPUT_PROVIDER = $MODEL_PROVIDER
        $VIDEO_INPUT_MODEL = $MODEL_NAME
        $VIDEO_INPUT_ENABLED = $true
    } elseif (($VideoChoice -eq "2" -and $videoCurrentSupported) -or ($VideoChoice -eq "1" -and (-not $videoCurrentSupported -or -not (Test-CurrentMediaModelUsable $MODEL_PROVIDER)))) {
        $MEDIA_ENABLED = $true
        $selection = Request-MediaApiProvider "video" "Video understanding" "openai" "gpt-4o" $false
        $VIDEO_INPUT_PROVIDER = $selection.provider
        $VIDEO_INPUT_MODEL = $selection.model
        $VIDEO_INPUT_ENABLED = $true
    }
} elseif (-not $MEDIA_ENABLED) {
    Write-Info "Skipped media tools. You can easily set them up later in the web UI."
}

Save-MediaCheckpoint

# Skill pack setup
Write-Host ""
Write-Host "  Choose skill packs to enable:" -ForegroundColor White
Write-Host "  All built-in skill packs ship with Nullion and are selected by default."
Write-Host "  Skill packs add workflow guidance only; account access still requires"
Write-Host "  workspace-scoped provider connections and enabled tools."
Write-Host ""

$selectedSkillPacks = New-Object System.Collections.Generic.List[string]
function Add-SkillPackChoice {
    param([string]$PackId)
    if ($PackId -and -not $selectedSkillPacks.Contains($PackId)) {
        [void]$selectedSkillPacks.Add($PackId)
    }
}

function Install-CustomSkillPackNow {
    param([string]$Source, [string]$PackId = "")
    $pythonExe = Join-Path $NULLION_VENV_DIR "Scripts\python.exe"
    $code = @'
import sys
from nullion.skill_pack_installer import install_skill_pack
source = sys.argv[1]
pack_id = sys.argv[2] or None
pack = install_skill_pack(source, pack_id=pack_id, force=True)
print(pack.pack_id)
'@
    $result = & $pythonExe -c $code $Source $PackId
    if ($LASTEXITCODE -ne 0) { throw "skill pack install failed" }
    return ([string]$result).Trim()
}

function Request-SkillPackChoices {
    if ([Console]::IsInputRedirected -or [Console]::IsOutputRedirected) {
        Write-Info "No interactive terminal detected; using all default skill packs."
        return @("1", "2", "3", "4", "5", "6", "7", "8", "9")
    }

    $items = @(
        @{ Title = "Web research"; Detail = "Search, fetch, source-backed answers"; Badge = ""; Choice = "1" },
        @{ Title = "Browser automation"; Detail = "Web navigation, forms, screenshots"; Badge = ""; Choice = "2" },
        @{ Title = "Files and documents"; Detail = "Local files, docs, sheets, decks"; Badge = ""; Choice = "3" },
        @{ Title = "PDF documents"; Detail = "PDF generation, conversion, verification, delivery"; Badge = ""; Choice = "4" },
        @{ Title = "Email and calendar"; Detail = "Inbox triage, replies, scheduling"; Badge = ""; Choice = "5" },
        @{ Title = "GitHub and code review"; Detail = "Repos, PRs, issues, release notes"; Badge = ""; Choice = "6" },
        @{ Title = "Local media"; Detail = "Audio transcription, OCR, image workflows"; Badge = ""; Choice = "7" },
        @{ Title = "Productivity and memory"; Detail = "Tasks, routines, preferences, reminders"; Badge = ""; Choice = "8" },
        @{ Title = "Connector/API skills"; Detail = "Maton, Composio, Nango, Activepieces, n8n, custom APIs"; Badge = ""; Choice = "9" },
        @{ Title = "Install custom skill pack"; Detail = "Git URL, GitHub folder, or local folder with SKILL.md"; Badge = ""; Choice = "10" },
        @{ Title = "No default skill packs"; Detail = "Start with no enabled reference packs"; Badge = ""; Choice = "11" }
    )

    foreach ($item in $items) {
        $badge = $item.Badge
        if (-not $badge -and [int]$item.Choice -le 9) { $badge = "[default]" }
        Write-MenuItem $item.Choice $item.Title $item.Detail $badge
    }
    Write-Host ""
    Write-Info "Press Enter to use the default packs (1-9), or enter choices like 1,3,6."
    $rawChoices = (Read-Host "  Skill packs [1,2,3,4,5,6,7,8,9]").Trim()
    if (-not $rawChoices) { $rawChoices = "1,2,3,4,5,6,7,8,9" }

    $choices = New-Object System.Collections.Generic.List[string]
    foreach ($choice in ($rawChoices -split '[^0-9]+')) {
        if (-not $choice) { continue }
        if ($choice -eq "11") {
            $choices.Clear()
            [void]$choices.Add("11")
            break
        }
        if (($items | Where-Object { $_.Choice -eq $choice }) -and -not $choices.Contains($choice)) {
            [void]$choices.Add($choice)
        }
    }
    if ($choices.Count -eq 0) { [void]$choices.Add("11") }
    return $choices.ToArray()
}

$ExistingSkillPacks = Get-EnvValue "NULLION_ENABLED_SKILL_PACKS"
$SKIP_SKILL_SETUP = $false
if ($ExistingSkillPacks) {
    Write-Info "Found existing skill packs: $ExistingSkillPacks"
    if (Confirm-PromptDefaultYes "Use existing skill packs instead of choosing them again?") {
        $SKIP_SKILL_SETUP = $true
        foreach ($pack in ($ExistingSkillPacks -split ',')) {
            Add-SkillPackChoice $pack
        }
        Write-Ok "Using existing skill packs."
    }
}

if (-not $SKIP_SKILL_SETUP) {
$SkillChoices = Request-SkillPackChoices

if ($SkillChoices -contains "11") {
    Write-Info "Skipped default skill packs. You can enable them later in Settings."
} else {
    foreach ($choice in $SkillChoices) {
        switch ($choice) {
            "1" { Add-SkillPackChoice "nullion/web-research" }
            "2" { Add-SkillPackChoice "nullion/browser-automation" }
            "3" { Add-SkillPackChoice "nullion/files-and-docs" }
            "4" { Add-SkillPackChoice "nullion/pdf-documents" }
            "5" { Add-SkillPackChoice "nullion/email-calendar" }
            "6" { Add-SkillPackChoice "nullion/github-code" }
            "7" { Add-SkillPackChoice "nullion/media-local" }
            "8" { Add-SkillPackChoice "nullion/productivity-memory" }
            "9" { Add-SkillPackChoice "nullion/connector-skills" }
            "10" {
                $CustomSkillPackSource = (Read-Host "  Skill pack source URL/path").Trim()
                $CustomSkillPackId = (Read-Host "  Pack id [auto]").Trim()
                if ($CustomSkillPackSource) {
                    try {
                        $CustomInstalledPackId = Install-CustomSkillPackNow $CustomSkillPackSource $CustomSkillPackId
                        Add-SkillPackChoice $CustomInstalledPackId
                        Write-Ok "Installed skill pack: $CustomInstalledPackId"
                    } catch {
                        Write-Err "Could not install custom skill pack. You can add it later in Settings."
                    }
                }
            }
            ""  { }
            default { Write-Info "Ignoring unknown skill choice: $choice" }
        }
    }
}
}

$ENABLED_SKILL_PACKS = ($selectedSkillPacks -join ",")
if ($ENABLED_SKILL_PACKS) {
    Write-Ok "Skill packs enabled: $ENABLED_SKILL_PACKS"
}

Save-SkillCheckpoint

# Write .env
$envLines = @(
    "# Nullion configuration - generated by install.ps1 on $(Get-Date)"
    "NULLION_WEB_PORT=$NULLION_WEB_PORT"
    "NULLION_KEY_STORAGE=$NULLION_KEY_STORAGE"
    "NULLION_SETUP_MESSAGING_DONE=true"
    "NULLION_SETUP_PROVIDER_DONE=true"
    "NULLION_SETUP_BROWSER_DONE=true"
    "NULLION_SETUP_SEARCH_DONE=true"
    "NULLION_SETUP_ACCOUNT_DONE=true"
    "NULLION_SETUP_MEDIA_DONE=true"
    "NULLION_SETUP_SKILLS_DONE=true"
)
if ($TELEGRAM_ENABLED) {
    $envLines += "NULLION_TELEGRAM_BOT_TOKEN=`"$BOT_TOKEN`""
    $envLines += "NULLION_TELEGRAM_OPERATOR_CHAT_ID=`"$CHAT_ID`""
    $envLines += "NULLION_TELEGRAM_CHAT_ENABLED=true"
} else {
    $envLines += "NULLION_TELEGRAM_CHAT_ENABLED=false"
}
if ($SLACK_ENABLED) {
    $envLines += "NULLION_SLACK_ENABLED=true"
    $envLines += "NULLION_SLACK_BOT_TOKEN=`"$SLACK_BOT_TOKEN`""
    $envLines += "NULLION_SLACK_APP_TOKEN=`"$SLACK_APP_TOKEN`""
    if ($SLACK_SIGNING_SECRET) { $envLines += "NULLION_SLACK_SIGNING_SECRET=`"$SLACK_SIGNING_SECRET`"" }
    if ($SLACK_OPERATOR_USER_ID) { $envLines += "NULLION_SLACK_OPERATOR_USER_ID=`"$SLACK_OPERATOR_USER_ID`"" }
} else {
    $envLines += "NULLION_SLACK_ENABLED=false"
}
if ($DISCORD_ENABLED) {
    $envLines += "NULLION_DISCORD_ENABLED=true"
    $envLines += "NULLION_DISCORD_BOT_TOKEN=`"$DISCORD_BOT_TOKEN`""
} else {
    $envLines += "NULLION_DISCORD_ENABLED=false"
}
if ($ANTHROPIC_KEY)    { $envLines += "ANTHROPIC_API_KEY=`"$ANTHROPIC_KEY`"" }
if ($OPENAI_KEY)       { $envLines += "OPENAI_API_KEY=`"$OPENAI_KEY`"" }
if ($MODEL_PROVIDER)   { $envLines += "NULLION_MODEL_PROVIDER=`"$MODEL_PROVIDER`"" }
if ($MODEL_BASE_URL)   { $envLines += "NULLION_OPENAI_BASE_URL=`"$MODEL_BASE_URL`"" }
if ($MODEL_NAME)       { $envLines += "NULLION_MODEL=`"$MODEL_NAME`"" }
if ($BROWSER_BACKEND)  { $envLines += "NULLION_BROWSER_BACKEND=`"$BROWSER_BACKEND`"" }
if ($BROWSER_CDP_URL)  { $envLines += "NULLION_BROWSER_CDP_URL=`"$BROWSER_CDP_URL`"" }
if ($BROWSER_PREFERRED){ $envLines += "NULLION_BROWSER_PREFERRED=`"$BROWSER_PREFERRED`"" }
if ($BRAVE_SEARCH_KEY) { $envLines += "NULLION_BRAVE_SEARCH_API_KEY=`"$BRAVE_SEARCH_KEY`"" }
if ($GOOGLE_SEARCH_KEY){ $envLines += "NULLION_GOOGLE_SEARCH_API_KEY=`"$GOOGLE_SEARCH_KEY`"" }
if ($GOOGLE_SEARCH_CX) { $envLines += "NULLION_GOOGLE_SEARCH_CX=`"$GOOGLE_SEARCH_CX`"" }
if ($PERPLEXITY_SEARCH_KEY){ $envLines += "NULLION_PERPLEXITY_API_KEY=`"$PERPLEXITY_SEARCH_KEY`"" }
if ($MATON_API_KEY)        { $envLines += "MATON_API_KEY=`"$MATON_API_KEY`"" }
if ($COMPOSIO_API_KEY)     { $envLines += "COMPOSIO_API_KEY=`"$COMPOSIO_API_KEY`"" }
if ($NANGO_SECRET_KEY)     { $envLines += "NANGO_SECRET_KEY=`"$NANGO_SECRET_KEY`"" }
if ($ACTIVEPIECES_API_KEY) { $envLines += "ACTIVEPIECES_API_KEY=`"$ACTIVEPIECES_API_KEY`"" }
if ($N8N_BASE_URL)         { $envLines += "N8N_BASE_URL=`"$N8N_BASE_URL`"" }
if ($N8N_API_KEY)          { $envLines += "N8N_API_KEY=`"$N8N_API_KEY`"" }
if ($MATON_CONNECTOR_ENABLED) { $envLines += "NULLION_CONNECTOR_GATEWAY=`"maton`"" }
if ($CUSTOM_API_BASE_URL)  { $envLines += "NULLION_CUSTOM_API_BASE_URL=`"$CUSTOM_API_BASE_URL`"" }
if ($CUSTOM_API_TOKEN)     { $envLines += "NULLION_CUSTOM_API_TOKEN=`"$CUSTOM_API_TOKEN`"" }
$enabledPlugins = "search_plugin,browser_plugin,workspace_plugin,media_plugin"
$providerBindings = "search_plugin=$SEARCH_PROVIDER,media_plugin=local_media_provider"
if ($EMAIL_CALENDAR_ENABLED) {
    $enabledPlugins += ",email_plugin,calendar_plugin"
    $providerBindings += ",email_plugin=google_workspace_provider,calendar_plugin=google_workspace_provider"
} elseif ($CUSTOM_EMAIL_API_ENABLED) {
    $enabledPlugins += ",email_plugin"
    $providerBindings += ",email_plugin=custom_api_provider"
}
$envLines += "NULLION_ENABLED_PLUGINS=`"$enabledPlugins`""
$envLines += "NULLION_PROVIDER_BINDINGS=`"$providerBindings`""
$envLines += "NULLION_ACTIVITY_TRACE_ENABLED=true"
$envLines += "NULLION_TASK_PLANNER_FEED_MODE=task"
$envLines += "NULLION_TASK_PLANNER_FEED_ENABLED=true"
if ($MEDIA_ENABLED) {
    if ($MEDIA_OPENAI_KEY)            { $envLines += "NULLION_MEDIA_OPENAI_API_KEY=`"$MEDIA_OPENAI_KEY`"" }
    if ($MEDIA_ANTHROPIC_KEY)         { $envLines += "NULLION_MEDIA_ANTHROPIC_API_KEY=`"$MEDIA_ANTHROPIC_KEY`"" }
    if ($MEDIA_OPENROUTER_KEY)        { $envLines += "NULLION_MEDIA_OPENROUTER_API_KEY=`"$MEDIA_OPENROUTER_KEY`"" }
    if ($MEDIA_GEMINI_KEY)            { $envLines += "NULLION_MEDIA_GEMINI_API_KEY=`"$MEDIA_GEMINI_KEY`"" }
    if ($MEDIA_GROQ_KEY)              { $envLines += "NULLION_MEDIA_GROQ_API_KEY=`"$MEDIA_GROQ_KEY`"" }
    if ($MEDIA_MISTRAL_KEY)           { $envLines += "NULLION_MEDIA_MISTRAL_API_KEY=`"$MEDIA_MISTRAL_KEY`"" }
    if ($MEDIA_DEEPSEEK_KEY)          { $envLines += "NULLION_MEDIA_DEEPSEEK_API_KEY=`"$MEDIA_DEEPSEEK_KEY`"" }
    if ($MEDIA_XAI_KEY)               { $envLines += "NULLION_MEDIA_XAI_API_KEY=`"$MEDIA_XAI_KEY`"" }
    if ($MEDIA_TOGETHER_KEY)          { $envLines += "NULLION_MEDIA_TOGETHER_API_KEY=`"$MEDIA_TOGETHER_KEY`"" }
    if ($MEDIA_CUSTOM_KEY)            { $envLines += "NULLION_MEDIA_CUSTOM_API_KEY=`"$MEDIA_CUSTOM_KEY`"" }
    if ($MEDIA_CUSTOM_BASE_URL)       { $envLines += "NULLION_MEDIA_CUSTOM_BASE_URL=`"$MEDIA_CUSTOM_BASE_URL`"" }
    if ($AUDIO_TRANSCRIBE_COMMAND) { $envLines += "NULLION_AUDIO_TRANSCRIBE_COMMAND=`"$AUDIO_TRANSCRIBE_COMMAND`"" }
    if ($IMAGE_OCR_COMMAND)       { $envLines += "NULLION_IMAGE_OCR_COMMAND=`"$IMAGE_OCR_COMMAND`"" }
    if ($IMAGE_GENERATE_COMMAND)  { $envLines += "NULLION_IMAGE_GENERATE_COMMAND=`"$IMAGE_GENERATE_COMMAND`"" }
    if ($AUDIO_TRANSCRIBE_ENABLED)   { $envLines += "NULLION_AUDIO_TRANSCRIBE_ENABLED=true" }
    if ($AUDIO_TRANSCRIBE_PROVIDER)  { $envLines += "NULLION_AUDIO_TRANSCRIBE_PROVIDER=`"$AUDIO_TRANSCRIBE_PROVIDER`"" }
    if ($AUDIO_TRANSCRIBE_MODEL)     { $envLines += "NULLION_AUDIO_TRANSCRIBE_MODEL=`"$AUDIO_TRANSCRIBE_MODEL`"" }
    if ($IMAGE_OCR_ENABLED)          { $envLines += "NULLION_IMAGE_OCR_ENABLED=true" }
    if ($IMAGE_OCR_PROVIDER)         { $envLines += "NULLION_IMAGE_OCR_PROVIDER=`"$IMAGE_OCR_PROVIDER`"" }
    if ($IMAGE_OCR_MODEL)            { $envLines += "NULLION_IMAGE_OCR_MODEL=`"$IMAGE_OCR_MODEL`"" }
    if ($IMAGE_GENERATE_ENABLED)     { $envLines += "NULLION_IMAGE_GENERATE_ENABLED=true" }
    if ($IMAGE_GENERATE_PROVIDER)    { $envLines += "NULLION_IMAGE_GENERATE_PROVIDER=`"$IMAGE_GENERATE_PROVIDER`"" }
    if ($IMAGE_GENERATE_MODEL)       { $envLines += "NULLION_IMAGE_GENERATE_MODEL=`"$IMAGE_GENERATE_MODEL`"" }
    if ($VIDEO_INPUT_ENABLED)        { $envLines += "NULLION_VIDEO_INPUT_ENABLED=true" }
    if ($VIDEO_INPUT_PROVIDER)       { $envLines += "NULLION_VIDEO_INPUT_PROVIDER=`"$VIDEO_INPUT_PROVIDER`"" }
    if ($VIDEO_INPUT_MODEL)          { $envLines += "NULLION_VIDEO_INPUT_MODEL=`"$VIDEO_INPUT_MODEL`"" }
}
if ($ENABLED_SKILL_PACKS) {
    $envLines += "NULLION_ENABLED_SKILL_PACKS=`"$ENABLED_SKILL_PACKS`""
    $envLines += "NULLION_SKILL_PACK_ACCESS_ENABLED=true"
    if (",$ENABLED_SKILL_PACKS," -like "*,nullion/connector-skills,*" -or $ENABLED_SKILL_PACKS -like "*api-gateway*") {
        $envLines += "NULLION_CONNECTOR_ACCESS_ENABLED=true"
    }
}
$envLines += "NULLION_LOG_LEVEL=INFO"

$envLines | Set-Content -Path $NULLION_ENV_FILE -Encoding UTF8

Protect-EnvFile

Write-Ok "Configuration saved to $NULLION_ENV_FILE"
Invoke-NullionRuntimeFinalization

# Step 4: Auto-start via Task Scheduler
Write-Header "Step 4 of 4 - Auto-start"

Write-Host ""
Write-Host "  Nullion can start automatically when you log in to Windows."
Write-Host "  This uses Task Scheduler - no admin rights required."
Write-Host ""

$AUTOSTART_CONFIGURED = $false
$TRAY_AUTOSTART_CONFIGURED = $false
$NULLION_STARTED_NOW = $false

if (Confirm-PromptDefaultYes "Set up auto-start at login?") {
    # Build a wrapper bat that sources the env file then launches the bot
    $wrapperBat = Join-Path $NULLION_DIR "start-nullion.bat"
    $trayWrapperBat = Join-Path $NULLION_DIR "start-nullion-tray.bat"
    $telegramWrapperBat = Join-Path $NULLION_DIR "start-nullion-telegram.bat"
    $slackWrapperBat = Join-Path $NULLION_DIR "start-nullion-slack.bat"
    $discordWrapperBat = Join-Path $NULLION_DIR "start-nullion-discord.bat"
    $logFile     = Join-Path $NULLION_LOG_DIR "nullion.log"
    $errFile     = Join-Path $NULLION_LOG_DIR "nullion-error.log"
    $trayLogFile = Join-Path $NULLION_LOG_DIR "tray.log"
    $trayErrFile = Join-Path $NULLION_LOG_DIR "tray-error.log"
    $telegramLogFile = Join-Path $NULLION_LOG_DIR "telegram.log"
    $telegramErrFile = Join-Path $NULLION_LOG_DIR "telegram-error.log"
    $slackLogFile = Join-Path $NULLION_LOG_DIR "slack.log"
    $slackErrFile = Join-Path $NULLION_LOG_DIR "slack-error.log"
    $discordLogFile = Join-Path $NULLION_LOG_DIR "discord.log"
    $discordErrFile = Join-Path $NULLION_LOG_DIR "discord-error.log"

    @"
@echo off
for /f "usebackq tokens=1,* delims==" %%A in ("$NULLION_ENV_FILE") do (
    if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
)
"$NULLION_EXE" --port $NULLION_WEB_PORT --checkpoint "$NULLION_DIR\runtime.db" >> "$logFile" 2>> "$errFile"
"@ | Set-Content -Path $wrapperBat -Encoding ASCII

    @"
@echo off
for /f "usebackq tokens=1,* delims==" %%A in ("$NULLION_ENV_FILE") do (
    if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
)
"$NULLION_TRAY_EXE" --port $NULLION_WEB_PORT --env-file "$NULLION_ENV_FILE" >> "$trayLogFile" 2>> "$trayErrFile"
"@ | Set-Content -Path $trayWrapperBat -Encoding ASCII

    if ($TELEGRAM_ENABLED) {
        @"
@echo off
for /f "usebackq tokens=1,* delims==" %%A in ("$NULLION_ENV_FILE") do (
    if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
)
"$NULLION_TELEGRAM_EXE" --checkpoint "$NULLION_DIR\runtime.db" --env-file "$NULLION_ENV_FILE" >> "$telegramLogFile" 2>> "$telegramErrFile"
"@ | Set-Content -Path $telegramWrapperBat -Encoding ASCII
    }

    if ($SLACK_ENABLED) {
        @"
@echo off
for /f "usebackq tokens=1,* delims==" %%A in ("$NULLION_ENV_FILE") do (
    if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
)
"$NULLION_SLACK_EXE" --checkpoint "$NULLION_DIR\runtime.db" --env-file "$NULLION_ENV_FILE" >> "$slackLogFile" 2>> "$slackErrFile"
"@ | Set-Content -Path $slackWrapperBat -Encoding ASCII
    }

    if ($DISCORD_ENABLED) {
        @"
@echo off
for /f "usebackq tokens=1,* delims==" %%A in ("$NULLION_ENV_FILE") do (
    if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
)
"$NULLION_DISCORD_EXE" --checkpoint "$NULLION_DIR\runtime.db" --env-file "$NULLION_ENV_FILE" >> "$discordLogFile" 2>> "$discordErrFile"
"@ | Set-Content -Path $discordWrapperBat -Encoding ASCII
    }

    # Register with schtasks - runs at logon, hidden window
    $taskArgs = @(
        "/Create", "/F",
        "/TN", $TASK_NAME,
        "/SC", "ONLOGON",
        "/TR", "`"$wrapperBat`"",
        "/RL", "LIMITED",
        "/IT"
    )
    $result = Invoke-SchtasksCommand $taskArgs
    if ($result.ExitCode -eq 0) {
        Write-Ok "Auto-start task registered in Task Scheduler."
        $AUTOSTART_CONFIGURED = $true
    } else {
        Write-Err "schtasks failed: $($result.Output)"
        Write-Info "You can start Nullion manually - see instructions below."
    }

    $trayTaskArgs = @(
        "/Create", "/F",
        "/TN", $TRAY_TASK_NAME,
        "/SC", "ONLOGON",
        "/TR", "`"$trayWrapperBat`"",
        "/RL", "LIMITED",
        "/IT"
    )
    $trayResult = Invoke-SchtasksCommand $trayTaskArgs
    if ($trayResult.ExitCode -eq 0) {
        Write-Ok "Tray icon auto-start task registered in Task Scheduler."
        $TRAY_AUTOSTART_CONFIGURED = $true
    } else {
        Write-Err "Tray schtasks failed: $($trayResult.Output)"
    }

    if ($TELEGRAM_ENABLED) {
        $telegramTaskArgs = @(
            "/Create", "/F",
            "/TN", $TELEGRAM_TASK_NAME,
            "/SC", "ONLOGON",
            "/TR", "`"$telegramWrapperBat`"",
            "/RL", "LIMITED",
            "/IT"
        )
        $telegramResult = Invoke-SchtasksCommand $telegramTaskArgs
        if ($telegramResult.ExitCode -eq 0) {
            Write-Ok "Telegram auto-start task registered in Task Scheduler."
        } else {
            Write-Err "Telegram schtasks failed: $($telegramResult.Output)"
        }
    }

    if ($SLACK_ENABLED) {
        $slackTaskArgs = @(
            "/Create", "/F",
            "/TN", $SLACK_TASK_NAME,
            "/SC", "ONLOGON",
            "/TR", "`"$slackWrapperBat`"",
            "/RL", "LIMITED",
            "/IT"
        )
        $slackResult = Invoke-SchtasksCommand $slackTaskArgs
        if ($slackResult.ExitCode -eq 0) {
            Write-Ok "Slack auto-start task registered in Task Scheduler."
        } else {
            Write-Err "Slack schtasks failed: $($slackResult.Output)"
        }
    }

    if ($DISCORD_ENABLED) {
        $discordTaskArgs = @(
            "/Create", "/F",
            "/TN", $DISCORD_TASK_NAME,
            "/SC", "ONLOGON",
            "/TR", "`"$discordWrapperBat`"",
            "/RL", "LIMITED",
            "/IT"
        )
        $discordResult = Invoke-SchtasksCommand $discordTaskArgs
        if ($discordResult.ExitCode -eq 0) {
            Write-Ok "Discord auto-start task registered in Task Scheduler."
        } else {
            Write-Err "Discord schtasks failed: $($discordResult.Output)"
        }
    }
}

if ($AUTOSTART_CONFIGURED) {
    $NULLION_STARTED_NOW = Start-NullionScheduledTaskNow $TASK_NAME "Nullion web dashboard"
    if ($TRAY_AUTOSTART_CONFIGURED) {
        [void](Start-NullionTrayNow -PreferScheduledTask $true)
    }
}

if (-not $AUTOSTART_CONFIGURED) {
    Write-Info "Skipped auto-start. To start manually, run:"
    Write-Host ""
    Write-Host "    $NULLION_EXE --port $NULLION_WEB_PORT --checkpoint $NULLION_DIR\runtime.db" -ForegroundColor Cyan
    Write-Host "    $NULLION_TRAY_EXE --port $NULLION_WEB_PORT --env-file $NULLION_ENV_FILE" -ForegroundColor Cyan
    if ($TELEGRAM_ENABLED) {
        Write-Host ""
        Write-Info "Telegram was configured. Start it manually with:"
        Write-Host ""
        Write-Host "    nullion-telegram --checkpoint $NULLION_DIR\runtime.db --env-file $NULLION_ENV_FILE" -ForegroundColor Cyan
    }
    if ($SLACK_ENABLED) {
        Write-Host ""
        Write-Info "Slack was configured. Start it manually with:"
        Write-Host ""
        Write-Host "    nullion-slack --checkpoint $NULLION_DIR\runtime.db --env-file $NULLION_ENV_FILE" -ForegroundColor Cyan
    }
    if ($DISCORD_ENABLED) {
        Write-Host ""
        Write-Info "Discord was configured. Start it manually with:"
        Write-Host ""
        Write-Host "    nullion-discord --checkpoint $NULLION_DIR\runtime.db --env-file $NULLION_ENV_FILE" -ForegroundColor Cyan
    }
    Write-Host ""
}

# Start now
Write-Host ""
Write-Header "All done!"
Write-Host ""
Write-Ok "Nullion v$NULLION_VERSION is installed."
Write-Host ""

if (Confirm-Prompt "Open Nullion in your browser now?") {
    if ($NULLION_STARTED_NOW) {
        Write-Info "Opening the running Nullion dashboard..."
        Start-Sleep -Seconds 2
        Start-Process "http://localhost:$NULLION_WEB_PORT"
    } else {
        Write-Info "Starting Nullion..."
        # Load env vars into current session
        Get-Content $NULLION_ENV_FILE | ForEach-Object {
            if ($_ -match '^([^#=]+)="?([^"]*)"?$') {
                [System.Environment]::SetEnvironmentVariable($Matches[1].Trim(), $Matches[2].Trim(), "Process")
            }
        }

        $proc = Start-Process `
            -FilePath $NULLION_EXE `
            -ArgumentList "--port", $NULLION_WEB_PORT, "--checkpoint", "$NULLION_DIR\runtime.db" `
            -RedirectStandardOutput (Join-Path $NULLION_LOG_DIR "nullion.log") `
            -RedirectStandardError  (Join-Path $NULLION_LOG_DIR "nullion-error.log") `
            -WindowStyle Hidden `
            -PassThru

        Start-Sleep -Seconds 2
        if (-not $proc.HasExited) {
            Write-Ok "Nullion is running (PID $($proc.Id))"
            [void](Start-NullionTrayNow -PreferScheduledTask $false)
            Write-Host ""
            Write-Host "  --> http://localhost:$NULLION_WEB_PORT" -ForegroundColor Green
            Write-Host ""
            Start-Process "http://localhost:$NULLION_WEB_PORT"
        } else {
            Write-Err "Nullion exited unexpectedly. Check the log:"
            Write-Host "    notepad `"$(Join-Path $NULLION_LOG_DIR 'nullion-error.log')`""
        }
    }
} else {
    Write-Host ""
    Write-Info "To start manually:"
    Write-Host "    $NULLION_EXE --port $NULLION_WEB_PORT --checkpoint $NULLION_DIR\runtime.db" -ForegroundColor Cyan
    Write-Host "    $NULLION_TRAY_EXE --port $NULLION_WEB_PORT --env-file $NULLION_ENV_FILE" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Then open:  http://localhost:$NULLION_WEB_PORT" -ForegroundColor Green
    Write-Host ""
}

Write-Host ""
if ($BrowserNote) {
    Write-Host ""
    Write-Host "  Browser note:" -ForegroundColor Yellow
    Write-Host "  $BrowserNote"
}

Write-Host "  Logs:    $NULLION_LOG_DIR\nullion.log" -ForegroundColor Cyan
Write-Host "  Tray:    $NULLION_LOG_DIR\tray-error.log" -ForegroundColor Cyan
Write-Host "  Config:  $NULLION_ENV_FILE" -ForegroundColor Cyan
if ($AUTOSTART_CONFIGURED) {
    Write-Host "  To stop: Open Task Scheduler and disable the '$TASK_NAME' task" -ForegroundColor Cyan
    Write-Host "     and the '$TRAY_TASK_NAME' task" -ForegroundColor Cyan
    Write-Host "     or run: schtasks /Delete /TN `"$TASK_NAME`" /F; schtasks /Delete /TN `"$TRAY_TASK_NAME`" /F" -ForegroundColor Cyan
}
Write-Host ""
