<# Run the two original delivery gates using native strategy code.
   AutomationRoot is retained as an optional backwards-compatible argument.
#>
[CmdletBinding()]
param([string]$AutomationRoot, [string]$Python = 'python')
$ErrorActionPreference = 'Stop'
$StrategyRoot = Split-Path -Parent $PSScriptRoot
$Runner = @'
import os
os.environ.setdefault('MICROCAP_DAILY_STOCK_PROVIDER', 'tencent')
from scripts import top100_delivery
for command in ('refresh-all', 'check'):
    result = top100_delivery.main([command])
    if result:
        raise SystemExit(result)
'@
$ExitCode = 1
Push-Location -LiteralPath $StrategyRoot
try {
    & $Python -B -X utf8 -c $Runner
    $ExitCode = $LASTEXITCODE
}
finally { Pop-Location }
if ($ExitCode -ne 0) { throw "Local daily delivery gates failed (exit $ExitCode)." }
