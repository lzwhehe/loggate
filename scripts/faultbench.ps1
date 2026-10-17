# Build the benchmark binaries and run fault-injection scenarios (Windows).
#
#   .\scripts\faultbench.ps1                       # quick profile, all scenarios
#   .\scripts\faultbench.ps1 -BenchProfile full -Reps 5
#   .\scripts\faultbench.ps1 -Scenarios S1,S3 -Out bench\results\my-run
#
# S5 (disk full) is Linux-only and is reported as skipped on Windows.
# Windows results are smoke tests; final numbers come from Linux.
param(
    [string]$BenchProfile = "quick",
    [int]$Reps = 0,
    [string[]]$Scenarios = @(),
    [string]$Out = ""
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$bin = Join-Path $root "bin"
New-Item -ItemType Directory -Force $bin | Out-Null
foreach ($c in "loggate", "mocksink", "loadgen", "reconcile", "faultbench") {
    & go build -o (Join-Path $bin "$c.exe") "./cmd/$c"
    if ($LASTEXITCODE -ne 0) { throw "go build $c failed" }
}

$files = @()
if ($Scenarios.Count -eq 0) {
    $files = Get-ChildItem bench\scenarios\S*.yaml | Sort-Object Name | ForEach-Object { $_.FullName }
} else {
    foreach ($s in $Scenarios) {
        $files += (Get-ChildItem "bench\scenarios\$($s)_*.yaml" | Select-Object -First 1).FullName
    }
}
if ($Out -eq "") {
    $Out = "bench\results\$(Get-Date -Format 'yyyyMMdd-HHmmss')-windows-$BenchProfile"
}
& (Join-Path $bin "faultbench.exe") -bin $bin -profile $BenchProfile -reps $Reps -out $Out @files
exit $LASTEXITCODE
