. "$PSScriptRoot/common.ps1"
Invoke-LabCommand docker @('info', '--format', '{{.ServerVersion}}')
Invoke-Compose @('config', '--quiet')
Invoke-Compose @('up', '-d')
Wait-LabCondition -Description 'PostgreSQL and Kafka healthy' -Check {
    $status = Invoke-Compose @('ps', '--format', 'json')
    $rows = @($status | ForEach-Object { $_ | ConvertFrom-Json })
    @($rows | Where-Object { $_.Service -in @('kafka','postgres') -and $_.Health -eq 'healthy' }).Count -eq 2
}
Invoke-Compose @('run', '--rm', 'init-topics')
Wait-LabCondition -Description 'Flink has two slots' -Check {
    (Invoke-RestMethod "$script:FlinkUrl/overview" -TimeoutSec 5).'slots-total' -ge 2
}
Write-Host 'Infrastructure ready. Kafka localhost:19092; PostgreSQL localhost:15432; Flink http://localhost:18081'
