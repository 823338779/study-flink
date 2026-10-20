param([ValidateRange(1,6)][int]$Chapter = 1, [string]$RunId = (Get-Date -Format 'MMddHHmmss'))
. "$PSScriptRoot/common.ps1"
if ($RunId -notmatch '^[A-Za-z0-9_-]{1,24}$') { throw 'RunId must be 1-24 ASCII letters/digits/_/-.' }
Write-Host "Chapter=$Chapter RunId=$RunId (reuse this RunId for chapters 1 and 2)"
$base = "lab-$RunId"
function Pay([string]$shop, [string]$id, [long]$amount) {
    Send-LabJson '/api/payments' @{
        eventId=$id; shopId=$shop; amountCents=$amount; occurredAt='2026-10-20T10:00:00Z'
    } | Out-Host
}
function Expect([string]$shop, [long]$amount, [long]$version) {
    Wait-LabCondition -Description "$shop amount=$amount version=$version" -TimeoutSeconds 240 -Check {
        $result = Invoke-RestMethod "$script:ApiUrl/api/shops/$shop/stats" -TimeoutSec 10
        $result.totalAmountCents -eq $amount -and $result.version -eq $version -and $result.paymentCount -eq $version
    }
    Invoke-RestMethod "$script:ApiUrl/api/shops/$shop/stats"
}
function WaitNewCheckpoint([long]$afterTime) {
    $job = Get-LabJob
    Wait-LabCondition -Description 'checkpoint triggered after event' -Check {
        $latest = (Invoke-RestMethod "$script:FlinkUrl/jobs/$($job.jid)/checkpoints").latest.completed
        $latest -and $latest.trigger_timestamp -gt $afterTime
    }
}
function WaitConsumed([int]$partition, [long]$offset) {
    Wait-LabCondition -Description "consumer committed partition=$partition offset>$offset" -Check {
        $rows = Invoke-Compose @('exec','-T','kafka','/opt/kafka/bin/kafka-consumer-groups.sh',
            '--bootstrap-server','kafka:9092','--group','payment-lab-projector','--describe')
        @($rows | Where-Object {
            $_ -match "shop-payment-totals\s+$partition\s+(\d+)" -and [long]$Matches[1] -gt $offset
        }).Count -gt 0
    }
}
switch ($Chapter) {
    1 {
        Pay "$base-A" 'payment-001' 10000
        Pay "$base-B" 'payment-002' 5000
        Pay "$base-A" 'payment-003' 8000
        Expect "$base-A" 18000 2
        Expect "$base-B" 5000 1
    }
    2 {
        Expect "$base-A" 18000 2
        Pay "$base-A" 'payment-001' 10000
        WaitNewCheckpoint ([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())
        Expect "$base-A" 18000 2
        Write-Host 'Check TaskManager logs for PAYMENT_DUPLICATE. No new aggregate version.'
    }
    3 {
        & "$PSScriptRoot/change-checkpoint.ps1" -IntervalMs 30000
        $started = Get-Date
        Pay "$base-visible" 'visible-001' 1000
        Expect "$base-visible" 1000 1
        Write-Host "Observed query delay: $((Get-Date)-$started). Compare CALCULATED and DATABASE_COMMITTED timestamps."
        & "$PSScriptRoot/change-checkpoint.ps1" -IntervalMs 10000
    }
    4 {
        Pay "$base-recovery" 'recovery-0' 10000
        Expect "$base-recovery" 10000 1
        for ($i=1; $i -le 20; $i++) {
            Pay "$base-recovery" "recovery-$i" 100
            if ($i -eq 5) { & "$PSScriptRoot/restart-taskmanager.ps1" }
        }
        Expect "$base-recovery" 12000 21
    }
    5 {
        $shop = "$base-failure"
        # 本实验要观察首次业务写入，使用未出现过的店铺。
        try {
            $null = Invoke-RestMethod "$script:ApiUrl/api/shops/$shop/stats"
            throw 'Shop already exists; choose a new RunId for chapter 5.'
        } catch {
            if (!$_.Exception.Response -or [int]$_.Exception.Response.StatusCode -ne 404) { throw }
        }
        Send-LabJson '/api/lab/fail-after-commit' @{resultId="${shop}:1"} | Out-Host
        Pay $shop 'failure-001' 700
        Expect $shop 700 1
        Wait-LabCondition -Description 'fault was redelivered as DUPLICATE' -Check {
            $probe = Invoke-RestMethod "$script:ApiUrl/api/lab/faults/${shop}:1"
            $probe.replayed -and $probe.outcome -eq 'DUPLICATE'
        }
        $probe = Invoke-RestMethod "$script:ApiUrl/api/lab/faults/${shop}:1"
        WaitConsumed $probe.partition $probe.offset
        $count = Invoke-Compose @('exec','-T','postgres','sh','-c',
            'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$1"', 'lab-query',
            "SELECT count(*) FROM processed_event WHERE shop_id='$shop'")
        if ([long]("$count".Trim()) -ne 1) { throw 'Expected exactly one processed_event row.' }
        Expect $shop 700 1
        Write-Host "Verified DUPLICATE replay, committed offset and one processed row for ${shop}:1."
    }
    6 {
        $shop = "$base-old"
        Send-LabJson '/api/lab/results' @{
            resultId="${shop}:3";shopId=$shop;totalAmountCents=23000;paymentCount=3;version=3;triggerEventId='old-3'
        } | Out-Host
        Expect $shop 23000 3
        $sent = Send-LabJson '/api/lab/results' @{
            resultId="${shop}:2";shopId=$shop;totalAmountCents=18000;paymentCount=2;version=2;triggerEventId='old-2'
        }
        WaitConsumed $sent.partition $sent.offset
        Expect $shop 23000 3
        Write-Host 'The old event is registered as processed, but cannot overwrite version 3.'
    }
}
