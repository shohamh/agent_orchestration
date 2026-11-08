[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

if (-not (Get-Command New-SelfSignedCertificate -ErrorAction SilentlyContinue) -or
    -not (Get-Command Export-PfxCertificate -ErrorAction SilentlyContinue)) {
    throw 'Certificate creation requires the Windows PowerShell certificate cmdlets.'
}
if ([string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
    throw 'LOCALAPPDATA is unavailable; cannot choose a PFX location outside the repository.'
}

$repoRoot = [System.IO.Path]::GetFullPath((Split-Path -Parent (Split-Path -Parent $PSScriptRoot)))
$repoRoot = $repoRoot.TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)
$pfxDirectory = [System.IO.Path]::GetFullPath((Join-Path $env:LOCALAPPDATA 'AgentOrchestration'))
$repoPrefix = $repoRoot + [System.IO.Path]::DirectorySeparatorChar
if ($pfxDirectory.Equals($repoRoot, [System.StringComparison]::OrdinalIgnoreCase) -or
    $pfxDirectory.StartsWith($repoPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw 'The PFX location must be outside the repository.'
}
$password = Read-Host 'Choose a password to protect the signing PFX' -AsSecureString
if ($password.Length -eq 0) {
    throw 'The PFX password cannot be empty.'
}

$certificate = New-SelfSignedCertificate `
    -Type CodeSigningCert `
    -Subject 'CN=Agent Orchestration' `
    -CertStoreLocation 'Cert:\CurrentUser\My' `
    -KeyAlgorithm RSA `
    -KeyLength 3072 `
    -HashAlgorithm SHA256 `
    -KeyExportPolicy Exportable `
    -NotAfter (Get-Date).AddYears(3)

$pfxPath = Join-Path $pfxDirectory "AgentWorkerSigning-$($certificate.Thumbprint).pfx"

New-Item -ItemType Directory -Force -Path $pfxDirectory | Out-Null
Export-PfxCertificate -Cert $certificate -FilePath $pfxPath -Password $password -ErrorAction Stop | Out-Null

Write-Host 'Created a self-signed code-signing certificate in CurrentUser\My.'
Write-Host "Certificate subject: $($certificate.Subject)"
Write-Host "Signer thumbprint: $($certificate.Thumbprint)"
Write-Host "Protected PFX: $pfxPath"
Write-Host 'Keep the PFX and its password outside the repository.'
