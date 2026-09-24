$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$RateRoot = Join-Path $RepoRoot 'artifacts/h48-cloud-rate-snapshot-v1'
if (Test-Path -LiteralPath $RateRoot) { throw 'Immutable rate snapshot already exists.' }
$TokenPath = Join-Path $env:USERPROFILE '.config/pathfinder/upcloud-token.credential.xml'
$Credential = Import-Clixml -LiteralPath $TokenPath
$ApiHeaders = @{ Authorization = 'Bearer ' + $Credential.GetNetworkCredential().Password }
try {
    $Prices = Invoke-RestMethod -Uri 'https://api.upcloud.com/1.3/price' -Headers $ApiHeaders -TimeoutSec 30
    $ServerResponse = Invoke-RestMethod -Uri 'https://api.upcloud.com/1.3/server' -Headers $ApiHeaders -TimeoutSec 30
    $Servers = @($ServerResponse.servers.server | Where-Object {
        $_.hostname -match '^pathfinder-(root|n[1-8])$'
    } | ForEach-Object {
        [ordered]@{uuid=$_.uuid; hostname=$_.hostname; plan=$_.plan;
                   zone=$_.zone; core_number=$_.core_number;
                   memory_amount=$_.memory_amount; state=$_.state}
    })
    if ($Servers.Count -ne 9) { throw 'Expected exactly nine experiment VMs.' }
    $null = New-Item -ItemType Directory -Path $RateRoot
    $Utf8 = [System.Text.UTF8Encoding]::new($false)
    $Documents = @{
        'prices.json' = $Prices
        'servers.json' = $Servers
        'snapshot.json' = [ordered]@{
            schema_version='pathfinder.cloud-price-snapshot/v1'
            retrieved_at_utc=[DateTime]::UtcNow.ToString('o')
            source='https://api.upcloud.com/1.3/price'
            vm_count=$Servers.Count
            credentials_recorded=$false
            allocation='active-VM-and-Root-hourly-rate-times-measured-experiment-seconds'
            invoice_cost_claimed=$false
        }
    }
    foreach ($Name in $Documents.Keys) {
        $Payload = ($Documents[$Name] | ConvertTo-Json -Depth 50) -replace "`r`n", "`n"
        [System.IO.File]::WriteAllText((Join-Path $RateRoot $Name), $Payload + "`n", $Utf8)
    }
    $Lines = @(Get-ChildItem -LiteralPath $RateRoot -File | Sort-Object Name | ForEach-Object {
        (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant() + '  ' + $_.Name
    })
    [System.IO.File]::WriteAllText((Join-Path $RateRoot 'SHA256SUMS'), ($Lines -join "`n") + "`n", $Utf8)
    Write-Output 'CLOUD_RATE_SNAPSHOT_FROZEN: 9 experiment VMs; credentials_recorded=false'
} catch {
    Write-Output ('CLOUD_RATE_SNAPSHOT_FAILED: ' + $_.Exception.GetType().Name)
    exit 2
} finally {
    $ApiHeaders.Clear()
    Remove-Variable Credential, ApiHeaders -ErrorAction SilentlyContinue
}
