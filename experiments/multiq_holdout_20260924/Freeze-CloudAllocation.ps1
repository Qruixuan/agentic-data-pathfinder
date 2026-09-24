$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$Snapshot = Join-Path $RepoRoot 'artifacts/h48-cloud-rate-snapshot-v1'
$Target = Join-Path $RepoRoot 'artifacts/h48-cloud-allocation-v1'
if (Test-Path -LiteralPath $Target) { throw 'Allocation snapshot already exists.' }
$Inventory = Get-Content -Raw (Join-Path $Snapshot 'servers.json') | ConvertFrom-Json
$Prices = Get-Content -Raw (Join-Path $Snapshot 'prices.json') | ConvertFrom-Json
$Prior = Get-Content -Raw (Join-Path $RepoRoot 'artifacts/rsi-exam-full-path-cost-20260923-v1/upcloud-rate-card.json') | ConvertFrom-Json
$Zone = @($Prices.prices.zone | Where-Object name -eq 'sg-sin1')[0]
$Credential = Import-Clixml -LiteralPath (Join-Path $env:USERPROFILE '.config/pathfinder/upcloud-token.credential.xml')
$Headers = @{Authorization='Bearer ' + $Credential.GetNetworkCredential().Password}
try {
    $Rows = @()
    foreach ($Vm in $Inventory) {
        $Detail = (Invoke-RestMethod -Uri ('https://api.upcloud.com/1.3/server/' + $Vm.uuid) -Headers $Headers -TimeoutSec 30).server
        if ($Detail.plan -ne $Vm.plan -or $Detail.state -ne 'started') { throw 'Server inventory changed.' }
        $Rate = $Zone.('server_plan_' + $Vm.plan)
        if ($Rate.amount -ne 1) { throw 'Unexpected plan amount.' }
        $Usd = [decimal]$Rate.price / 100
        $Previous = @($Prior.plans | Where-Object plan -eq $Vm.plan)[0]
        if ($Usd -ne [decimal]$Previous.usd_per_hour -or !$Prior.hourly_rate_calibrated_against_full_day_n3_charge) { throw 'Rate unit calibration differs.' }
        $Rows += [ordered]@{
            host=$Vm.hostname; plan=$Vm.plan; usd_per_hour=$Usd.ToString([cultureinfo]::InvariantCulture)
            storage=@($Detail.storage_devices.storage_device | ForEach-Object {
                [ordered]@{size=$_.storage_size; part_of_plan=$_.part_of_plan; type=$_.type}
            })
            addresses=@($Detail.ip_addresses.ip_address | ForEach-Object {
                [ordered]@{family=$_.family; access=$_.access; part_of_plan=$_.part_of_plan}
            })
        }
    }
    $Report = [ordered]@{
        schema_version='pathfinder.experiment-cloud-allocation/v1'
        retrieved_utc=[DateTime]::UtcNow.ToString('o'); currency='USD'; hosts=$Rows
        rate_unit='API cents/hour divided by 100, matched to the prior invoice-calibrated rate card'
        calibration_sha256=(Get-FileHash (Join-Path $RepoRoot 'artifacts/rsi-exam-full-path-cost-20260923-v1/upcloud-rate-card.json')).Hash.ToLowerInvariant()
        pricing_snapshot_sha256=(Get-FileHash (Join-Path $Snapshot 'prices.json')).Hash.ToLowerInvariant()
        allocation='Measured batch wall time allocated in proportion to measured route wall times; not rounded invoice charges.'
        build_allocation='Measured execution-host wall time; query projection construction and verification overhead reported separately.'
        storage_policy='Plan-included disk storage is included in VM time, not added a second time. Persistent retention after the measured interval is excluded.'
        network_policy='Private SDN and within-plan public transfer have zero incremental charge; provider ingress does not add a per-byte API fee.'
        invoice_payment_claimed=$false; credentials_recorded=$false
    }
    $null = New-Item -ItemType Directory -Path $Target
    $Utf8 = [System.Text.UTF8Encoding]::new($false)
    $Json = ($Report | ConvertTo-Json -Depth 15) -replace "`r`n", "`n"
    [IO.File]::WriteAllText((Join-Path $Target 'allocation.json'), $Json + "`n", $Utf8)
    $Hash = (Get-FileHash (Join-Path $Target 'allocation.json')).Hash.ToLowerInvariant()
    [IO.File]::WriteAllText((Join-Path $Target 'SHA256SUMS'), "$Hash  allocation.json`n", $Utf8)
    Write-Output 'CLOUD_ALLOCATION_FROZEN: 9 hosts, current plan membership recorded; no credentials.'
} catch {
    Write-Output ('CLOUD_ALLOCATION_FAILED: ' + $_.Exception.GetType().Name)
    exit 2
} finally {
    $Headers.Clear()
    Remove-Variable Credential, Headers -ErrorAction SilentlyContinue
}
