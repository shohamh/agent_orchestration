[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Path,
    [switch]$RequireSigning,
    [string]$Thumbprint
)

$ErrorActionPreference = 'Stop'

$resolvedPath = (Resolve-Path -LiteralPath $Path -ErrorAction Stop).Path
if (-not [System.IO.File]::Exists($resolvedPath)) {
    throw "The Windows worker executable does not exist: $resolvedPath"
}

$pfxBase64 = $env:WORKER_SIGNING_PFX_BASE64
$pfxPassword = $env:WORKER_SIGNING_PFX_PASSWORD
$hasPfx = -not [string]::IsNullOrEmpty($pfxBase64)
$hasPassword = -not [string]::IsNullOrEmpty($pfxPassword)
if ($hasPfx -xor $hasPassword) {
    throw 'Signing PFX configuration is incomplete; set both WORKER_SIGNING_PFX_BASE64 and WORKER_SIGNING_PFX_PASSWORD.'
}

$requestedThumbprint = $Thumbprint
if ($hasPfx -and -not [string]::IsNullOrWhiteSpace($requestedThumbprint)) {
    throw 'Choose either the CI PFX environment variables or a CurrentUser\My thumbprint, not both.'
}

$tempPfx = $null
$pfxBytes = $null
$certificate = $null
try {
    if ($hasPfx) {
        try {
            $pfxBytes = [System.Convert]::FromBase64String($pfxBase64)
        } catch {
            throw 'WORKER_SIGNING_PFX_BASE64 is not valid Base64.'
        }
        if ($pfxBytes.Length -eq 0) {
            throw 'WORKER_SIGNING_PFX_BASE64 decoded to an empty file.'
        }

        $tempPfx = [System.IO.Path]::GetTempFileName()
        [System.IO.File]::WriteAllBytes($tempPfx, $pfxBytes)
        [System.Array]::Clear($pfxBytes, 0, $pfxBytes.Length)
        $pfxBytes = $null
        $keyFlags = [System.Security.Cryptography.X509Certificates.X509KeyStorageFlags]::Exportable
        try {
            $certificate = [System.Security.Cryptography.X509Certificates.X509Certificate2]::new(
                $tempPfx,
                $pfxPassword,
                $keyFlags
            )
        } catch {
            throw 'The signing PFX could not be opened. Check the PFX contents and WORKER_SIGNING_PFX_PASSWORD.'
        }
    } else {
        $storeCertificates = @(Get-ChildItem -Path 'Cert:\CurrentUser\My' -ErrorAction Stop)
        if (-not [string]::IsNullOrWhiteSpace($requestedThumbprint)) {
            $normalizedThumbprint = ($requestedThumbprint -replace '\s', '').ToUpperInvariant()
            $candidates = @($storeCertificates | Where-Object {
                (($_.Thumbprint -replace '\s', '').ToUpperInvariant() -eq $normalizedThumbprint)
            })
        } else {
            $candidates = @($storeCertificates | Where-Object {
                $_.Subject -ieq 'CN=Agent Orchestration' -and $_.HasPrivateKey
            })
        }

        if ($candidates.Count -eq 0) {
            if ($RequireSigning) {
                throw 'No signing certificate was found. Create one with worker/ci/new-signing-certificate.ps1 or configure the CI PFX secrets.'
            }
            Write-Host 'No signing certificate configured; leaving this Windows worker build unsigned.'
            return
        }
        if ($candidates.Count -gt 1) {
            throw 'Multiple matching signing certificates were found; specify one with -Thumbprint.'
        }
        $certificate = $candidates[0]
    }

    if (-not $certificate.HasPrivateKey) {
        throw 'The selected signing certificate has no private key.'
    }
    if ($certificate.Subject -ine 'CN=Agent Orchestration') {
        throw 'The signing certificate subject must be CN=Agent Orchestration.'
    }
    $now = Get-Date
    if ($certificate.NotBefore -gt $now -or $certificate.NotAfter -le $now) {
        throw 'The signing certificate is not currently valid.'
    }
    $ekuExtension = $certificate.Extensions | Where-Object { $_.Oid.Value -eq '2.5.29.37' } | Select-Object -First 1
    if (-not $ekuExtension) {
        throw 'The signing certificate has no code-signing enhanced key usage.'
    }
    $enhancedKeyUsage = [System.Security.Cryptography.X509Certificates.X509EnhancedKeyUsageExtension]$ekuExtension
    $codeSigningUsage = $enhancedKeyUsage.EnhancedKeyUsages | Where-Object { $_.Value -eq '1.3.6.1.5.5.7.3.3' }
    if (-not $codeSigningUsage) {
        throw 'The signing certificate is not valid for code signing.'
    }

    $expectedThumbprint = ($certificate.Thumbprint -replace '\s', '').ToUpperInvariant()
    Set-AuthenticodeSignature -FilePath $resolvedPath -Certificate $certificate -HashAlgorithm SHA256 -ErrorAction Stop | Out-Null

    $verification = Get-AuthenticodeSignature -FilePath $resolvedPath -ErrorAction Stop
    $status = [string]$verification.Status
    if ($status -eq 'HashMismatch') {
        throw 'Authenticode verification failed: the signed executable hash does not match.'
    }
    if ($status -eq 'NotSigned') {
        throw 'Authenticode verification failed: the executable is not signed.'
    }
    if ($status -ne 'Valid' -and $status -ne 'NotTrusted' -and $status -ne 'UnknownError') {
        throw "Authenticode verification failed with status '$status'."
    }
    if (-not $verification.SignerCertificate) {
        throw 'Authenticode verification did not return a signer certificate.'
    }
    $actualThumbprint = ($verification.SignerCertificate.Thumbprint -replace '\s', '').ToUpperInvariant()
    if ($actualThumbprint -ne $expectedThumbprint) {
        throw 'Authenticode verification returned a different signer certificate than the one used to sign.'
    }

    $untrustedRoot = $false
    if ($status -eq 'NotTrusted' -or $status -eq 'UnknownError') {
        $chain = New-Object System.Security.Cryptography.X509Certificates.X509Chain
        try {
            $chain.ChainPolicy.RevocationMode = [System.Security.Cryptography.X509Certificates.X509RevocationMode]::NoCheck
            $chainTrusted = $chain.Build($verification.SignerCertificate)
            $chainStatuses = @($chain.ChainStatus | ForEach-Object { $_.Status })
            $untrustedRootFlag = [System.Security.Cryptography.X509Certificates.X509ChainStatusFlags]::UntrustedRoot
            if ($chainTrusted -or $chainStatuses.Count -ne 1 -or $chainStatuses[0] -ne $untrustedRootFlag) {
                throw "Authenticode verification failed with status '$status'; signer chain is not only an untrusted root."
            }
            $untrustedRoot = $true
        } finally {
            $chain.Dispose()
        }
    }

    if ($untrustedRoot) {
        Write-Host 'Authenticode signature integrity and signer identity verified; this Windows host does not trust the signer chain.'
    } else {
        Write-Host 'Authenticode signature integrity and signer identity verified.'
    }
    Write-Host "Signed executable: $resolvedPath"
    Write-Host "Signer thumbprint: $expectedThumbprint"
} finally {
    if ($pfxBytes) {
        [System.Array]::Clear($pfxBytes, 0, $pfxBytes.Length)
    }
    if ($certificate) {
        $certificate.Dispose()
    }
    if ($tempPfx -and [System.IO.File]::Exists($tempPfx)) {
        Remove-Item -LiteralPath $tempPfx -Force -ErrorAction Stop
    }
}
