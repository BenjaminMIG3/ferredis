# Désinstalle le service Windows Ferredis.
# Exécuter en Administrateur : .\uninstall-service.ps1 [-RemoveFiles]
param(
    [string]$InstallDir = "$env:ProgramFiles\Ferredis",
    [string]$ServiceName = "Ferredis",
    [switch]$RemoveFiles
)

$ErrorActionPreference = "Stop"

function Test-Admin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    $p = New-Object Security.Principal.WindowsPrincipal($id)
    return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Admin)) {
    Write-Error "Exécuter ce script en Administrateur."
}

$svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($svc) {
    if ($svc.Status -eq "Running") {
        Stop-Service -Name $ServiceName -Force
    }
    sc.exe delete $ServiceName | Out-Null
    Write-Host "Service $ServiceName supprimé."
} else {
    Write-Host "Service $ServiceName absent."
}

if ($RemoveFiles -and (Test-Path $InstallDir)) {
    Remove-Item -Recurse -Force $InstallDir
    Write-Host "Dossier $InstallDir supprimé."
}
