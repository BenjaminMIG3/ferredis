# Installe Ferredis en tant que service Windows (équivalent systemctl enable/start).
# Exécuter en Administrateur :
#   .\install-service.ps1 [-InstallDir "C:\Program Files\Ferredis"] [-Start]
param(
    [string]$InstallDir = "$env:ProgramFiles\Ferredis",
    [string]$DataDir = "$env:ProgramData\Ferredis\data",
    [string]$ServiceName = "Ferredis",
    [switch]$Start
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

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Split-Path -Parent (Split-Path -Parent $ScriptDir)

# Sources binaires : dossier courant du zip, ou target\release
$Candidates = @(
    $ScriptDir,
    (Join-Path $Root "dist\windows"),
    (Join-Path $Root "target\release")
)
$Src = $null
foreach ($c in $Candidates) {
    if (Test-Path (Join-Path $c "ferredis-server.exe")) {
        $Src = $c
        break
    }
}
if (-not $Src) {
    Write-Error "ferredis-server.exe introuvable. Build release ou extrayez le zip packaging."
}

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null

Copy-Item -Force (Join-Path $Src "ferredis-server.exe") $InstallDir
if (Test-Path (Join-Path $Src "ferredis-cli.exe")) {
    Copy-Item -Force (Join-Path $Src "ferredis-cli.exe") $InstallDir
}
if (Test-Path (Join-Path $Src "ferredis-gui.exe")) {
    Copy-Item -Force (Join-Path $Src "ferredis-gui.exe") $InstallDir
}

$ConfDst = Join-Path $InstallDir "ferredis.conf"
$ConfSrc = Join-Path $ScriptDir "ferredis.conf"
if (-not (Test-Path $ConfDst)) {
    if (Test-Path $ConfSrc) {
        Copy-Item -Force $ConfSrc $ConfDst
    }
    # Normalise le chemin data dans la conf installée
    $conf = Get-Content $ConfDst -Raw
    $conf = $conf -replace 'dir .*', ("dir " + ($DataDir -replace '\\', '/'))
    Set-Content -Path $ConfDst -Value $conf -Encoding UTF8
}

$exe = Join-Path $InstallDir "ferredis-server.exe"
$binPath = "`"$exe`" --config `"$ConfDst`""

$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
if ($existing) {
    if ($existing.Status -eq "Running") {
        Stop-Service -Name $ServiceName -Force
    }
    # Met à jour le chemin binaire
    sc.exe config $ServiceName binPath= $binPath start= auto | Out-Null
} else {
    New-Service -Name $ServiceName `
        -BinaryPathName $binPath `
        -DisplayName "Ferredis Server" `
        -Description "Serveur compatible Redis (Ferredis)" `
        -StartupType Automatic | Out-Null
}

# Redémarrage auto en cas d'échec
sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/10000/restart/30000 | Out-Null

if ($Start) {
    Start-Service -Name $ServiceName
}

Write-Host ""
Write-Host "Service Windows installé : $ServiceName"
Write-Host "  Start-Service $ServiceName"
Write-Host "  Stop-Service $ServiceName"
Write-Host "  Get-Service $ServiceName"
Write-Host "  sc.exe start $ServiceName"
Write-Host "Config : $ConfDst"
Write-Host "Data   : $DataDir"
