# Installs this PC as an execution worker for the agent orchestration server, as the current user.
# Generated for one install link: it works once and expires. Nothing here needs Administrator or an open port.
#   $env:AGENT_INSTALL_PREFIX      install folder (default %LOCALAPPDATA%\AgentWorker)
#   $env:AGENT_INSTALL_NO_SERVICE  set to 1 to skip the startup task and the registration (testing)
#   $env:AGENT_INSTALL_PYTHON      the Python to use (default: py -3, python, python3)
& {
    $ErrorActionPreference = 'Stop'
    $ProgressPreference = 'SilentlyContinue'
    $Server = @@SERVER@@; $Code = @@CODE@@; $Name = @@NAME@@; $Slots = @@SLOTS@@; $TargetOs = @@OS@@
    $Prefix = if ($env:AGENT_INSTALL_PREFIX) { $env:AGENT_INSTALL_PREFIX } else { Join-Path $env:LOCALAPPDATA 'AgentWorker' }
    $Port = 8096
    $State = @{ Step = 'check' }

    function Report([string]$step, [string]$state = 'running', [string]$message = '') {
        try {
            $body = @{ step = $step; state = $state; message = $message } | ConvertTo-Json -Compress
            Invoke-RestMethod -Method Post -Uri "$Server/install/$Code/progress" -Body $body -ContentType 'application/json' -TimeoutSec 10 | Out-Null
        } catch { }
    }
    function Step([string]$id, [string]$text) { $State.Step = $id; Write-Host "==> $text"; Report $id }
    function Check-Exit([string]$message) { if ($LASTEXITCODE -ne 0) { throw $message } }
    function Find-Python {
        $candidates = if ($env:AGENT_INSTALL_PYTHON) { @(@{ Exe = $env:AGENT_INSTALL_PYTHON; Args = @() }) }
                      else { @(@{ Exe = 'py'; Args = @('-3') }, @{ Exe = 'python'; Args = @() }, @{ Exe = 'python3'; Args = @() }) }
        foreach ($candidate in $candidates) {
            if (-not (Get-Command $candidate.Exe -ErrorAction SilentlyContinue)) { continue }
            $extra = $candidate.Args
            try { $version = (& $candidate.Exe @extra -c "import sys; print('%d.%d' % sys.version_info[:2])" 2>$null | Select-Object -First 1) } catch { continue }
            if ($LASTEXITCODE -eq 0 -and $version -match '^(\d+)\.(\d+)$' -and ([int]$Matches[1] -gt 3 -or ([int]$Matches[1] -eq 3 -and [int]$Matches[2] -ge 10))) { return $candidate }
        }
        return $null
    }

    try {
        Step 'check' 'Checking this PC'
        if ($PSVersionTable.PSEdition -ne 'Core' -and [Environment]::OSVersion.Platform -ne 'Win32NT') { throw 'This script is for Windows; use the bash command on Linux and macOS' }
        $isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
        if ($isAdmin) { throw 'Run this in a normal PowerShell window, not as Administrator: the worker installs for your own user' }
        if ($TargetOs -ne 'windows') { Write-Host "Note: the form said $TargetOs but this PC is Windows; continuing" }
        if (-not (Get-Command tar.exe -ErrorAction SilentlyContinue)) { throw 'tar.exe is required (Windows 10 version 1803 or newer)' }
        $py = Find-Python
        if (-not $py) { throw 'Python 3.10 or newer is required. Install it, for example: winget install Python.Python.3.12 (then open a new terminal)' }
        if (-not (Get-Command node -ErrorAction SilentlyContinue) -or -not (Get-Command npm.cmd -ErrorAction SilentlyContinue)) { throw 'Node.js 18 or newer is required. Install it, for example: winget install OpenJS.NodeJS.LTS (then open a new terminal)' }
        $nodeMajor = [int](((& node --version) -replace '^v', '').Split('.')[0])
        if ($nodeMajor -lt 18) { throw 'Node.js 18 or newer is required' }

        Step 'download' "Downloading the worker from $Server"
        New-Item -ItemType Directory -Force -Path $Prefix | Out-Null
        $bundle = Join-Path $Prefix 'bundle.tgz'
        try { Invoke-WebRequest -UseBasicParsing -Uri "$Server/install/$Code/bundle.tgz" -OutFile $bundle } catch { throw "Could not download the worker from $Server (is the link expired or the server unreachable?)" }
        & tar.exe -xzf $bundle -C $Prefix; Check-Exit 'The downloaded worker is damaged'
        Remove-Item -Force $bundle

        Step 'python' 'Installing Python packages (from PyPI)'
        $pyExtra = $py.Args
        & $py.Exe @pyExtra -m venv (Join-Path $Prefix 'venv'); Check-Exit 'Could not create the Python environment'
        $venvPython = Join-Path $Prefix 'venv\Scripts\python.exe'
        $env:PIP_DISABLE_PIP_VERSION_CHECK = '1'
        & $venvPython -m pip install --quiet -r (Join-Path $Prefix 'agents\requirements-worker.txt'); Check-Exit "pip could not install the worker's packages (is PyPI reachable?)"

        Step 'npm' 'Installing the coding agent adapters (from npm)'
        & npm.cmd ci --prefix (Join-Path $Prefix 'agents') --no-audit --no-fund --loglevel=error; Check-Exit 'npm could not install the agent adapters (is the npm registry reachable?)'

        Step 'token' "Creating this PC's token"
        $token = (& $venvPython -c 'import secrets; print(secrets.token_urlsafe(32))' | Select-Object -First 1).Trim()
        $tokenFile = Join-Path $Prefix 'token'
        [IO.File]::WriteAllText($tokenFile, $token)
        $me = [Security.Principal.WindowsIdentity]::GetCurrent().Name
        & icacls.exe $tokenFile /inheritance:r /grant:r "${me}:(R,W)" | Out-Null; Check-Exit 'Could not restrict the token file to this user'
        $config = @{ id = $Name; slots = $Slots; server = $Server; port = $Port } | ConvertTo-Json -Compress
        [IO.File]::WriteAllText((Join-Path $Prefix 'worker.json'), $config)
        $launcher = Join-Path $Prefix 'run-worker.ps1'
        $launcherText = @(
            "`$p = Start-Process -FilePath '$venvPython' -ArgumentList @('-u', '$(Join-Path $Prefix 'agents\worker_runner.py')', '--home', '$Prefix', '--log', '$(Join-Path $Prefix 'worker.log')') -WorkingDirectory '$(Join-Path $Prefix 'agents')' -WindowStyle Hidden -Wait -PassThru",
            'exit $p.ExitCode') -join "`r`n"
        [IO.File]::WriteAllText($launcher, $launcherText)
        $uninstall = "Unregister-ScheduledTask -TaskName 'AgentWorker-$Name' -Confirm:`$false -ErrorAction SilentlyContinue`r`nStop-ScheduledTask -TaskName 'AgentWorker-$Name' -ErrorAction SilentlyContinue`r`nRemove-Item -Recurse -Force '$Prefix'"
        [IO.File]::WriteAllText((Join-Path $Prefix 'uninstall.ps1'), $uninstall)

        $runArgs = "-u `"$(Join-Path $Prefix 'agents\worker_runner.py')`" --home `"$Prefix`" --log `"$(Join-Path $Prefix 'worker.log')`""
        if ($env:AGENT_INSTALL_NO_SERVICE -eq '1') {
            Write-Host "Installed in $Prefix (AGENT_INSTALL_NO_SERVICE=1: no startup task, not registered)."
            Write-Host "Start it with: & '$venvPython' $runArgs"
            return
        }

        Step 'service' 'Starting the worker'
        $taskName = "AgentWorker-$Name"
        $action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument "-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$launcher`""
        $trigger = New-ScheduledTaskTrigger -AtLogOn -User $me
        $settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)
        $principal = New-ScheduledTaskPrincipal -UserId $me -LogonType Interactive -RunLevel Limited
        Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null
        Start-ScheduledTask -TaskName $taskName
        $waited = 0
        while ($true) {
            try { Invoke-RestMethod -Uri "http://127.0.0.1:$Port/health" -Headers @{ Authorization = "Bearer $token" } -TimeoutSec 3 | Out-Null; break } catch { }
            $waited++; if ($waited -gt 40) { throw "The worker did not start; see $(Join-Path $Prefix 'worker.log')" }
            Start-Sleep -Seconds 1
        }

        Step 'register' 'Registering this PC with the server'
        $body = @{ token = $token; hostname = $env:COMPUTERNAME } | ConvertTo-Json -Compress
        try { $reply = Invoke-RestMethod -Method Post -Uri "$Server/install/$Code/complete" -Body $body -ContentType 'application/json' -TimeoutSec 30 }
        catch {
            $detail = $_.ErrorDetails.Message; if (-not $detail) { $detail = $_.Exception.Message }
            throw "The server refused the registration: $detail"
        }
        if (-not $reply.ok) { throw 'The server refused the registration' }

        Step 'connect' 'Waiting for the connection'
        $waited = 0
        while ($true) {
            try { if ((Invoke-RestMethod -Uri "$Server/install/$Code/state" -TimeoutSec 5).connected) { break } } catch { }
            $waited++; if ($waited -gt 60) { throw "Registered, but the worker is not connected yet; see $(Join-Path $Prefix 'worker.log')" }
            Start-Sleep -Seconds 1
        }
        Write-Host ''
        Write-Host "Done. $Name is connected and will start again when you log in."
        Write-Host "Remove it later with: powershell -File '$(Join-Path $Prefix 'uninstall.ps1')'"
    } catch {
        $message = $_.Exception.Message
        Write-Host "ERROR: $message" -ForegroundColor Red
        Report $State.Step 'failed' $message
    }
}
