param([string]$RunId = (Get-Date -Format 'MMddHHmmss'))
. "$PSScriptRoot/common.ps1"
foreach ($chapter in @(1,2,5,6)) {
    & "$PSScriptRoot/run-lab.ps1" -Chapter $chapter -RunId $RunId
}
Write-Host "PASS: normal flow, duplicate payment, post-commit retry and stale result. RunId=$RunId"
