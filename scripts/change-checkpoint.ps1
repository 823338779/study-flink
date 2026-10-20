param([long]$IntervalMs = 30000)
. "$PSScriptRoot/common.ps1"
if ($IntervalMs -le 0) { throw 'IntervalMs must be positive.' }
$job = Get-LabJob
if (!$job -or $job.state -ne 'RUNNING') { throw 'A RUNNING lab job is required.' }
$request = Invoke-RestMethod "$script:FlinkUrl/jobs/$($job.jid)/stop" -Method Post `
    -ContentType 'application/json' -Body '{"drain":false,"targetDirectory":"file:///opt/flink/checkpoints/savepoints"}'
$script:RestoreLocation = $null
Wait-LabCondition -Description 'stop with savepoint' -TimeoutSeconds 180 -Check {
    $status = Invoke-RestMethod "$script:FlinkUrl/jobs/$($job.jid)/savepoints/$($request.'request-id')" -TimeoutSec 10
    if ($status.status.id -eq 'COMPLETED') {
        if ($status.operation.'failure-cause') { throw ($status.operation.'failure-cause' | ConvertTo-Json -Depth 10) }
        $script:RestoreLocation = $status.operation.location
        return !!$script:RestoreLocation
    }
    return $false
}
$recordDir = Join-Path $script:LabRoot '.lab'
New-Item -ItemType Directory -Force -Path $recordDir | Out-Null
$script:RestoreLocation | Set-Content -LiteralPath (Join-Path $recordDir 'last-savepoint.txt')
Wait-LabCondition -Description 'old job stopped' -Check { !(Get-LabJob) }
Write-Host "Restore from $script:RestoreLocation with interval=$IntervalMs ms"
& "$PSScriptRoot/submit-job.ps1" -RestorePath $script:RestoreLocation -CheckpointIntervalMs $IntervalMs
