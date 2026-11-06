# Build the worker for Windows x86_64 (MSVC, C runtime linked statically) and write it, with a checksum, to dist\worker\.
# CI (GitHub Actions and GitLab CI) and developers run this same script:
#   worker\ci\build.ps1
# Output: dist\worker\agent-worker-x86_64-pc-windows-msvc.exe and its .sha256
# Native tools (rustup, cargo) write progress to stderr, which Windows PowerShell 5.1 would turn into errors: exit codes decide instead.
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
Copy-Item -Force (Join-Path $worker "target\$target\release\agent-worker.exe") (Join-Path $out $name)
$hash = (Get-FileHash -Algorithm SHA256 (Join-Path $out $name)).Hash.ToLower()
"$hash  $name" | Set-Content -Encoding ascii (Join-Path $out "$name.sha256")
Write-Host "Built $(Join-Path $out $name)"
