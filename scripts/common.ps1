$ErrorActionPreference = 'Stop'
$script:LabRoot = Split-Path $PSScriptRoot -Parent
$script:ComposeFile = Join-Path $script:LabRoot 'compose.yaml'
$script:ApiUrl = 'http://127.0.0.1:18080'
$script:FlinkUrl = 'http://127.0.0.1:18081'

function Invoke-LabCommand {
    param([string]$File, [string[]]$Arguments)
    & $File @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$File failed (exit $LASTEXITCODE)." }
}
function Invoke-Compose {
    param([string[]]$Arguments)
    Invoke-LabCommand docker (@('compose', '-p', 'flink-payment-lab', '-f', $script:ComposeFile) + $Arguments)
}
function Wait-LabCondition {
    param([scriptblock]$Check, [string]$Description, [int]$TimeoutSeconds = 180)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastFailure = ''
    do {
        try { if (& $Check) { return } } catch { $lastFailure = $_.Exception.Message }
        Start-Sleep -Milliseconds 750
    } while ((Get-Date) -lt $deadline)
    throw "Timeout: $Description. Last error: $lastFailure"
}
function Send-LabJson {
    param([string]$Path, [object]$Body)
    Invoke-RestMethod -Uri "$script:ApiUrl$Path" -Method Post -ContentType 'application/json' `
        -Body ($Body | ConvertTo-Json -Depth 8 -Compress) -TimeoutSec 15
}
function Get-LabJob {
    $jobs = (Invoke-RestMethod "$script:FlinkUrl/jobs/overview" -TimeoutSec 10).jobs |
        Where-Object { $_.name -eq 'payment-aggregation' -and $_.state -notin @('FINISHED','CANCELED','FAILED') }
    if (@($jobs).Count -gt 1) { throw 'More than one lab job is active. Stop duplicate jobs before continuing.' }
    $jobs | Select-Object -First 1
}
