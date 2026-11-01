<# Run the scoped cloud daily profile and both original delivery gates.
   Configure .microcap_index_cache/local_daily_launcher.json with AutomationRoot,
   or pass -AutomationRoot. No global environment is changed.
#>
[CmdletBinding()]
param(
    [string]$AutomationRoot,
    [string]$Python = 'python'
)
$ErrorActionPreference = 'Stop'
$StrategyRoot = Split-Path -Parent $PSScriptRoot
$ConfigPath = Join-Path $StrategyRoot '.microcap_index_cache/local_daily_launcher.json'
if ([string]::IsNullOrWhiteSpace($AutomationRoot)) {
    if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
        throw 'Pass -AutomationRoot or configure .microcap_index_cache/local_daily_launcher.json.'
    }
    $Config = Get-Content -LiteralPath $ConfigPath -Raw -Encoding utf8 | ConvertFrom-Json
    $AutomationRoot = [string]$Config.AutomationRoot
}
if ([string]::IsNullOrWhiteSpace($AutomationRoot)) { throw 'AutomationRoot is required.' }
$AutomationRoot = (Resolve-Path -LiteralPath $AutomationRoot).Path
$CompatRoot = Join-Path $AutomationRoot 'scripts/microcap_csv_compat'
foreach ($Name in @('sitecustomize.py', 'notice_boundary_adapter.py', 'exact_non_st_addition_certificate.json')) {
    if (-not (Test-Path -LiteralPath (Join-Path $CompatRoot $Name) -PathType Leaf)) {
        throw "Automation compatibility profile is incomplete: $Name"
    }
}
$Certificate = Join-Path $CompatRoot 'exact_non_st_addition_certificate.json'
$ExpectedCertificate = '79e957bb3dcfd5b09927da67f917ae1a6d03ae5478baeff28ec85cbe5014f318'
if ((Get-FileHash -LiteralPath $Certificate -Algorithm SHA256).Hash.ToLowerInvariant() -ne $ExpectedCertificate) {
    throw 'Automation reviewed-notice certificate identity mismatch.'
}
$EnvironmentNames = @('PYTHONPATH', 'MICROCAP_CSV_LINE_TERMINATOR', 'MICROCAP_REVIEWED_NON_ST_BOUNDARY', 'MICROCAP_LOCAL_COMPAT_ROOT')
$PreviousEnvironment = @{}
foreach ($Name in $EnvironmentNames) { $PreviousEnvironment[$Name] = [Environment]::GetEnvironmentVariable($Name, 'Process') }
$Runner = @'
import os, sys
from pathlib import Path
import sitecustomize
expected = Path(os.environ['MICROCAP_LOCAL_COMPAT_ROOT']).resolve()
if Path(sitecustomize.__file__).resolve() != expected / 'sitecustomize.py':
    raise RuntimeError('Expected automation startup profile was not loaded')
if os.linesep != '\r\n' or not any(type(f).__module__ == 'notice_boundary_adapter' for f in sys.meta_path):
    raise RuntimeError('Automation compatibility profile did not initialize')
from scripts import top100_delivery
for command in ('refresh-all', 'check'):
    result = top100_delivery.main([command])
    if result:
        raise SystemExit(result)
'@
$ExitCode = 1
Push-Location -LiteralPath $StrategyRoot
try {
    $env:PYTHONPATH = $CompatRoot
    if ($PreviousEnvironment.PYTHONPATH) { $env:PYTHONPATH += [IO.Path]::PathSeparator + $PreviousEnvironment.PYTHONPATH }
    $env:MICROCAP_CSV_LINE_TERMINATOR = 'crlf'
    $env:MICROCAP_REVIEWED_NON_ST_BOUNDARY = '20261030-exact-v1'
    $env:MICROCAP_LOCAL_COMPAT_ROOT = $CompatRoot
    & $Python -B -X utf8 -c $Runner
    $ExitCode = $LASTEXITCODE
}
finally {
    foreach ($Name in $EnvironmentNames) { [Environment]::SetEnvironmentVariable($Name, $PreviousEnvironment[$Name], 'Process') }
    Pop-Location
}
if ($ExitCode -ne 0) { throw "Local daily delivery gates failed (exit $ExitCode)." }
