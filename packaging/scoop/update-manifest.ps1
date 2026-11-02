# Met à jour bucket/ferredis.json (version + hash SHA256 du zip Windows).
# Usage:
#   pwsh -File packaging/scoop/update-manifest.ps1 -Version 0.1.0 -ZipPath dist/ferredis-windows-x86_64.zip
param(
    [Parameter(Mandatory = $true)]
    [string]$Version,
    [Parameter(Mandatory = $true)]
    [string]$ZipPath,
    [string]$Repo = "BenjaminMIG3/ferredis"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$ManifestPath = Join-Path $Root "bucket\ferredis.json"

if (-not (Test-Path $ZipPath)) {
    throw "Zip introuvable: $ZipPath"
}

$Version = $Version.TrimStart("v")
$Hash = (Get-FileHash -Path $ZipPath -Algorithm SHA256).Hash.ToLowerInvariant()
$Url = "https://github.com/$Repo/releases/download/v$Version/ferredis-windows-x86_64.zip"

$Manifest = Get-Content -Raw -Path $ManifestPath | ConvertFrom-Json
$Manifest.version = $Version
$Manifest.architecture.'64bit'.url = $Url
$Manifest.architecture.'64bit'.hash = $Hash

$Json = $Manifest | ConvertTo-Json -Depth 8
# ConvertTo-Json utilise une indentation 2 espaces ; Scoop accepte les deux.
[System.IO.File]::WriteAllText($ManifestPath, $Json + "`n")
Write-Host "Scoop manifest mis à jour: $ManifestPath"
Write-Host "  version=$Version"
Write-Host "  hash=$Hash"
Write-Host "  url=$Url"
