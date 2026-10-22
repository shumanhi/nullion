# escape=`
FROM python:3.11-windowsservercore-ltsc2022

SHELL ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command"]

ENV PYTHONDONTWRITEBYTECODE=1 `
    PYTHONUNBUFFERED=1 `
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN Set-Variable -Name ProgressPreference -Value SilentlyContinue; `
    Invoke-WebRequest `
        -UseBasicParsing `
        -Uri 'https://github.com/git-for-windows/git/releases/download/v2.54.0.windows.1/MinGit-2.54.0-64-bit.zip' `
        -OutFile C:\mingit.zip; `
    Expand-Archive -Path C:\mingit.zip -DestinationPath C:\mingit; `
    Remove-Item -Force C:\mingit.zip; `
    C:\mingit\cmd\git.exe --version

ENV PATH="C:\mingit\cmd;C:\mingit\usr\bin;C:\Python;C:\Python\Scripts;C:\Windows\system32;C:\Windows;C:\Windows\System32\Wbem;C:\Windows\System32\WindowsPowerShell\v1.0"

WORKDIR C:\workspace
