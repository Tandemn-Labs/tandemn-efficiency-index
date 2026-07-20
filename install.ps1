$ErrorActionPreference = "Stop"

if ($env:PROCESSOR_ARCHITECTURE -ne "AMD64") {
    throw "Only Windows x86-64 is currently supported."
}

$asset = "tei-windows-amd64.zip"
$baseUrl = "https://github.com/Tandemn-Labs/tandemn-efficiency-index/releases/latest/download"
$installDir = if ($env:TEI_INSTALL_DIR) {
    $env:TEI_INSTALL_DIR
} else {
    Join-Path $HOME ".local\bin"
}
$tempDir = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid().ToString())

New-Item -ItemType Directory -Path $tempDir | Out-Null

try {
    $archivePath = Join-Path $tempDir $asset
    $checksumsPath = Join-Path $tempDir "checksums.txt"

    Invoke-WebRequest "$baseUrl/$asset" -OutFile $archivePath
    Invoke-WebRequest "$baseUrl/checksums.txt" -OutFile $checksumsPath

    $checksumLine = Get-Content $checksumsPath | Where-Object { $_ -match "\s$([regex]::Escape($asset))$" }
    if (-not $checksumLine) {
        throw "No checksum found for $asset"
    }

    $expected = ($checksumLine -split "\s+")[0].ToLowerInvariant()
    $actual = (Get-FileHash -Algorithm SHA256 $archivePath).Hash.ToLowerInvariant()
    if ($expected -ne $actual) {
        throw "Checksum verification failed for $asset"
    }

    Expand-Archive -Path $archivePath -DestinationPath $tempDir
    New-Item -ItemType Directory -Force -Path $installDir | Out-Null
    Copy-Item (Join-Path $tempDir "tei.exe") (Join-Path $installDir "tei.exe") -Force

    Write-Host "Installed tei to $(Join-Path $installDir 'tei.exe')"
} finally {
    Remove-Item -Recurse -Force $tempDir
}
