. "$PSScriptRoot/common.ps1"
$job = Get-LabJob
if (!$job) { throw 'Start the lab job first.' }
$checkpoint = (Invoke-RestMethod "$script:FlinkUrl/jobs/$($job.jid)/checkpoints").latest.completed
if (!$checkpoint) { throw 'No successful checkpoint; refusing recovery experiment.' }
Write-Host "Before restart: checkpoint=$($checkpoint.id), path=$($checkpoint.external_path)"
Invoke-Compose @('restart', 'taskmanager')
Wait-LabCondition -Description 'TaskManager recovery' -TimeoutSeconds 240 -Check {
    $detail = Invoke-RestMethod "$script:FlinkUrl/jobs/$($job.jid)"
    $history = Invoke-RestMethod "$script:FlinkUrl/jobs/$($job.jid)/checkpoints"
    $detail.state -eq 'RUNNING' -and $history.counts.restored -ge 1 `
        -and $history.latest.completed.id -gt $checkpoint.id
}
Invoke-RestMethod "$script:FlinkUrl/jobs/$($job.jid)/checkpoints" |
    Select-Object counts, @{Name='restored'; Expression={$_.latest.restored}}
