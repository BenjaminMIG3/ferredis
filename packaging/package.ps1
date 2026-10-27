# Packaging Windows natif (zip) — server / cli / gui.
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Out = Join-Path $Root "dist\windows"
New-Item -ItemType Directory -Force -Path $Out | Out-Null
Set-Location $Root
cargo build --release -p ferredis-server -p ferredis-cli
cargo build --release -p ferredis-gui
Copy-Item -Force "target\release\ferredis-server.exe" $Out
Copy-Item -Force "target\release\ferredis-cli.exe" $Out
if (Test-Path "target\release\ferredis-gui.exe") {
  Copy-Item -Force "target\release\ferredis-gui.exe" $Out
}
if (Test-Path "LICENSE") { Copy-Item -Force "LICENSE" $Out }
$Zip = Join-Path $Root "dist\ferredis-windows-x86_64.zip"
if (Test-Path $Zip) { Remove-Item $Zip }
Compress-Archive -Path (Join-Path $Out "*") -DestinationPath $Zip
Write-Host "Artefact: $Zip"
