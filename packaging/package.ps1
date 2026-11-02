# Packaging Windows natif (zip) — binaires + scripts service Windows.
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Out = Join-Path $Root "dist\windows"
New-Item -ItemType Directory -Force -Path $Out | Out-Null
Set-Location $Root
cargo build --release -p ferredis-server -p ferredis-cli -p ferredis-gui
Copy-Item -Force "target\release\ferredis-server.exe" $Out
Copy-Item -Force "target\release\ferredis-cli.exe" $Out
Copy-Item -Force "target\release\ferredis-gui.exe" $Out
if (Test-Path "LICENSE") { Copy-Item -Force "LICENSE" $Out }
Copy-Item -Force "packaging\windows\ferredis.conf" $Out
Copy-Item -Force "packaging\windows\install-service.ps1" $Out
Copy-Item -Force "packaging\windows\uninstall-service.ps1" $Out
$Zip = Join-Path $Root "dist\ferredis-windows-x86_64.zip"
if (Test-Path $Zip) { Remove-Item $Zip }
Compress-Archive -Path (Join-Path $Out "*") -DestinationPath $Zip
Write-Host "Artefact: $Zip"
Write-Host "Install service (Admin): Expand zip, puis .\install-service.ps1 -Start"
