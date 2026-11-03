# Run the Trigger.dev dev worker that hosts local coding-agent sessions on this PC.
#
# - Authenticates with TRIGGER_API_URL / TRIGGER_ACCESS_TOKEN from the ignored .env.local (a personal
#   access token), so no interactive `trigger login` is needed.
# - Pipes output so the CLI runs non-interactively: trigger.dev 4.8.3 ignores --skip-mcp-install
#   (it reads options.skipMCPInstall) and its first-run MCP question would block an unattended start.
Set-Location (Split-Path -Parent $PSScriptRoot)
Get-Content .env.local | Where-Object { $_ -match '^\s*([A-Z_]+)=(.*)$' } | ForEach-Object {
    [Environment]::SetEnvironmentVariable($Matches[1], $Matches[2].Trim(), 'Process')
}
npx.cmd trigger.dev dev --skip-update-check 2>&1 | ForEach-Object { $_ }
