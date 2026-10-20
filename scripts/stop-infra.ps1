. "$PSScriptRoot/common.ps1"
Invoke-Compose @('stop')
Write-Host 'Only lab containers stopped. Data and checkpoints retained.'
