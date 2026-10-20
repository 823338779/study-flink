param([long]$CheckpointIntervalMs = 10000, [string]$RestorePath)
. "$PSScriptRoot/common.ps1"
if ($CheckpointIntervalMs -le 0) { throw 'CheckpointIntervalMs must be positive.' }
$existing = Get-LabJob
if ($existing) {
    Write-Host "Existing job $($existing.jid) state=$($existing.state). No duplicate submitted; options unchanged."
    return
}
if (!$RestorePath) {
    $offsets = Invoke-Compose @('exec','-T','kafka','/opt/kafka/bin/kafka-get-offsets.sh',
        '--bootstrap-server','kafka:9092','--topic','payment-events','--time','latest')
    $hasData = @($offsets | Where-Object { $_ -match '^payment-events:\d+:(\d+)$' -and [long]$Matches[1] -gt 0 }).Count -gt 0
    if ($hasData) {
        throw 'Input data already exists. Supply -RestorePath from a checkpoint/savepoint. Refusing an empty-state replay.'
    }
    $count = Invoke-RestMethod "$script:ApiUrl/api/processed-events/count" -TimeoutSec 10
    if ($count.count -gt 0) { throw 'Old result data exists. Restore matching state or reset the entire lab explicitly.' }
}
$jar = Join-Path $script:LabRoot 'flink-job/target/flink-job.jar'
if (!(Test-Path -LiteralPath $jar)) { throw 'Build first: mvn -B verify' }
Invoke-Compose @('cp', $jar, 'jobmanager:/opt/flink/flink-job.jar')
$arguments = @('exec','-T','jobmanager','/opt/flink/bin/flink','run','-d')
if ($RestorePath) { $arguments += @('-s', $RestorePath) }
$arguments += @('/opt/flink/flink-job.jar','--checkpoint-interval-ms',"$CheckpointIntervalMs")
Invoke-Compose $arguments
Wait-LabCondition -Description 'job RUNNING' -Check { (Get-LabJob).state -eq 'RUNNING' }
Wait-LabCondition -Description 'first successful checkpoint' -Check {
    $job = Get-LabJob
    (Invoke-RestMethod "$script:FlinkUrl/jobs/$($job.jid)/checkpoints" -TimeoutSec 10).counts.completed -gt 0
}
Write-Host 'Job ready. Checkpoints and Kafka transactions enabled.'
