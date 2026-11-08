# Build the worker for Windows x86_64 (MSVC, C runtime linked statically) and write it, with a checksum, to dist\worker\.
# CI (GitHub Actions and GitLab CI) and developers run this same script:
#   worker\ci\build.ps1
# Output: dist\worker\agent-worker-x86_64-pc-windows-msvc.exe and its .sha256
# Native tools (rustup, cargo) write progress to stderr, which Windows PowerShell 5.1 would turn into errors: exit codes decide instead.
[CmdletBinding()]
param(
    [switch]$RequireSigning,
    [string]$SignerThumbprint
)

$ErrorActionPreference = 'Continue'
$worker = Split-Path -Parent $PSScriptRoot
$root = Split-Path -Parent $worker
$target = 'x86_64-pc-windows-msvc'
$name = "agent-worker-$target.exe"
$out = if ($env:OUT) { $env:OUT } else { Join-Path $root 'dist\worker' }

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) { throw 'cargo is not installed (https://rustup.rs)' }
Push-Location $worker
try {
    # rustup writes its progress to stderr; that is not an error
    & rustup target add $target *> $null
    & cargo build --release --locked --target $target
    if ($LASTEXITCODE -ne 0) { throw 'cargo build failed' }
} finally { Pop-Location }
New-Item -ItemType Directory -Force -Path $out | Out-Null
$binaryPath = Join-Path $out $name
Copy-Item -Force (Join-Path $worker "target\$target\release\agent-worker.exe") $binaryPath

$isGitHubRelease = $env:GITHUB_REF -like 'refs/tags/worker-v*'
$isGitLabRelease = $env:CI_COMMIT_TAG -like 'worker-v*'
$isCi = $env:GITHUB_ACTIONS -eq 'true' -or $env:GITLAB_CI -eq 'true'
if ($isGitHubRelease -or $isGitLabRelease) {
    $missingSecrets = @()
    if ([string]::IsNullOrWhiteSpace($env:WORKER_SIGNING_PFX_BASE64)) {
        $missingSecrets += 'WORKER_SIGNING_PFX_BASE64'
    }
    if ([string]::IsNullOrWhiteSpace($env:WORKER_SIGNING_PFX_PASSWORD)) {
        $missingSecrets += 'WORKER_SIGNING_PFX_PASSWORD'
    }
    if ($missingSecrets.Count -gt 0) {
        throw "Windows worker release builds require CI signing secrets: $($missingSecrets -join ', ')."
    }
    $RequireSigning = $true
}

if ($RequireSigning -or $SignerThumbprint -or -not $isCi) {
$signingArguments = @{ Path = $binaryPath }
if ($RequireSigning) { $signingArguments.RequireSigning = $true }
if (-not [string]::IsNullOrWhiteSpace($SignerThumbprint)) {
    $signingArguments.Thumbprint = $SignerThumbprint
}
& (Join-Path $PSScriptRoot 'sign-windows.ps1') @signingArguments
} else {
    Write-Host 'Skipping Authenticode signing for non-release CI builds.'
}

# Hash the final bytes: release checksums must include the Authenticode signature.
$hash = (Get-FileHash -Algorithm SHA256 $binaryPath).Hash.ToLower()
"$hash  $name" | Set-Content -Encoding ascii (Join-Path $out "$name.sha256")
Write-Host "Built $binaryPath"
