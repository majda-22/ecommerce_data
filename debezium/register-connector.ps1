param(
    [string]$ConnectUrl = "http://localhost:8083",
    [string]$ConfigPath = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
    $ConfigPath = Join-Path $PSScriptRoot "connector-config.json"
}

if (-not (Test-Path -LiteralPath $ConfigPath)) {
    throw "Connector config not found: $ConfigPath"
}

$ConfigPath = (Resolve-Path -LiteralPath $ConfigPath).Path
$ConnectorDefinition = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json
$ConnectorName = $ConnectorDefinition.name

if ([string]::IsNullOrWhiteSpace($ConnectorName)) {
    throw "Connector config must contain a top-level 'name' field."
}

function Invoke-ConnectRest {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Method,

        [Parameter(Mandatory = $true)]
        [string]$Uri,

        [string]$Body
    )

    try {
        if ($PSBoundParameters.ContainsKey("Body")) {
            return Invoke-RestMethod `
                -Method $Method `
                -Uri $Uri `
                -ContentType "application/json" `
                -Body $Body
        }

        return Invoke-RestMethod -Method $Method -Uri $Uri
    }
    catch {
        if (-not [string]::IsNullOrWhiteSpace($_.ErrorDetails.Message)) {
            throw "Kafka Connect request failed: $Method $Uri`n$($_.ErrorDetails.Message)"
        }

        $Response = $_.Exception.Response
        if ($null -ne $Response) {
            $Reader = New-Object System.IO.StreamReader($Response.GetResponseStream())
            $ResponseBody = $Reader.ReadToEnd()
            throw "Kafka Connect request failed: $Method $Uri`n$ResponseBody"
        }

        throw
    }
}

Write-Host "Checking Kafka Connect at $ConnectUrl..."
Invoke-ConnectRest -Method Get -Uri "$ConnectUrl/connectors" | Out-Null

Write-Host "Registering Debezium connector '$ConnectorName'..."

$ExistingConnectors = Invoke-ConnectRest -Method Get -Uri "$ConnectUrl/connectors"

if ($ExistingConnectors -contains $ConnectorName) {
    Write-Host "Connector already exists. Updating configuration..."

    $ConfigOnlyJson = $ConnectorDefinition.config | ConvertTo-Json -Depth 20
    $Response = Invoke-ConnectRest `
        -Method Put `
        -Uri "$ConnectUrl/connectors/$ConnectorName/config" `
        -Body $ConfigOnlyJson
}
else {
    Write-Host "Connector does not exist. Creating it..."

    $FullConfigJson = Get-Content -LiteralPath $ConfigPath -Raw
    $Response = Invoke-ConnectRest `
        -Method Post `
        -Uri "$ConnectUrl/connectors" `
        -Body $FullConfigJson
}

Write-Host "Connector registration/update request completed."
Write-Host ($Response | ConvertTo-Json -Depth 20)

Write-Host "`nConnector status:"
$Status = $null
for ($Attempt = 1; $Attempt -le 10; $Attempt++) {
    try {
        $Status = Invoke-ConnectRest -Method Get -Uri "$ConnectUrl/connectors/$ConnectorName/status"
        break
    }
    catch {
        if ($Attempt -eq 10) {
            throw
        }

        Start-Sleep -Seconds 1
    }
}

Write-Host ($Status | ConvertTo-Json -Depth 20)
