<#
.SYNOPSIS
  The README bench matrix for ratelimitd: build, start the daemon, run ratelimitctl bench for
  each row, stop the daemon, write results/bench/<yyyyMMdd-HHmmss>.md.

.DESCRIPTION
  Rows (each -Duration long after -Warmup):
    grpc  redis  keys=10000   the headline number (plan: >= 50k/s, p99 < 5 ms, ratio within 1 %, 0 errors)
    grpc  redis  2pol         same, with the global policy on top (two keys per call, dry-run + commit)
    grpc  redis  keys=1       the hot-key ceiling (one Redis key, one Go shard)
    grpc  redis  deny         one key against tenant benchdeny (gcra 20000/s burst 2000): the allowed ratio must
                              match the closed form, i.e. the deny path is exercised and accounted
    http  redis  keys=10000   the JSON adapter
    grpc  local  keys=10000   the degraded path: daemon started with REDIS_ADDR on a closed port,
                              FALLBACK_MODE=local, so the breaker opens at startup and every decision is local
  The policy under test is tenant `bench` / policy `load` in policies.example.yaml (gcra, rate 100000/s,
  burst 10000) plus the global_ingress policy, so every call is the two-policy all-or-nothing path.

  Requires: go on PATH, redis up on 127.0.0.1:6380 (task redis:up), ports 50053/8093/50054/9130 free.

.EXAMPLE
  pwsh -NoProfile -File scripts/bench.ps1
.EXAMPLE
  pwsh -NoProfile -File scripts/bench.ps1 -Rps 80000 -Duration 20s -Rows grpc-redis,grpc-local
#>
[CmdletBinding()]
param(
    [int]$Rps = 60000,
    [string]$Duration = '30s',
    [string]$Warmup = '5s',
    [int]$Workers = 0,
    [int]$Batch = 8,
    [string[]]$Rows = @('grpc-redis', 'grpc-redis-2pol', 'grpc-redis-hot', 'grpc-redis-deny', 'http-redis', 'grpc-local'),
    [string]$RedisAddr = '127.0.0.1:6380'
)

Set-StrictMode -Version 2
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot
$bin = Join-Path $RepoRoot 'bin'
New-Item -ItemType Directory -Force $bin | Out-Null
$daemon = Join-Path $bin 'ratelimitd.exe'
$ctl = Join-Path $bin 'ratelimitctl.exe'
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$outDir = Join-Path $RepoRoot 'results\bench'
New-Item -ItemType Directory -Force $outDir | Out-Null
$outFile = Join-Path $outDir "$stamp.md"
$benchPolicies = Join-Path $RepoRoot 'policies.bench.yaml'     # one gcra policy per call
$examplePolicies = Join-Path $RepoRoot 'policies.example.yaml' # bench tenant + global_ingress: two keys per call

$version = (git describe --tags --always --dirty 2>$null)
if (-not $version) { $version = 'dev' }
Write-Host "==> building ($version)"
go build -trimpath -ldflags="-s -w -X main.version=$version" -o $daemon ./cmd/ratelimitd
if ($LASTEXITCODE -ne 0) { throw 'build ratelimitd failed' }
go build -trimpath -ldflags="-s -w" -o $ctl ./cmd/ratelimitctl
if ($LASTEXITCODE -ne 0) { throw 'build ratelimitctl failed' }

function Start-Daemon {
    param([string]$Redis, [string]$Mode, [string]$Log, [string]$Policies)
    $env:REDIS_ADDR = $Redis
    $env:FALLBACK_MODE = $Mode
    $env:POLICY_FILE = $Policies
    $env:LOG_FORMAT = 'text'
    $env:LOG_LEVEL = 'warn'
    $env:BREAKER_MIN = '5'
    $p = Start-Process -FilePath $daemon -PassThru -WindowStyle Hidden -RedirectStandardOutput $Log
    $deadline = (Get-Date).AddSeconds(15)
    while ((Get-Date) -lt $deadline) {
        try {
            $r = Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:9130/readyz' -TimeoutSec 1
            if ($r.StatusCode -eq 200) { return $p }
        } catch { }
        if ($p.HasExited) { throw "ratelimitd exited early; see $Log" }
        Start-Sleep -Milliseconds 250
    }
    Stop-Process -Id $p.Id -Force
    throw "ratelimitd not ready after 15 s; see $Log"
}

function Stop-Daemon {
    param($Proc)
    if ($null -eq $Proc -or $Proc.HasExited) { return }
    # No console signal from here (the Ctrl-Break check is a separate manual step); a hard stop is fine for a bench.
    Stop-Process -Id $Proc.Id -Force
    $Proc.WaitForExit(5000) | Out-Null
}

$cpu = (Get-CimInstance Win32_Processor | Select-Object -First 1).Name.Trim()
$goVersion = (go version)
$header = @(
    "# ratelimitd bench"
    ""
    "_$(Get-Date -Format 'yyyy-MM-dd HH:mm') | $cpu | $goVersion | $([System.Environment]::OSVersion.VersionString) | ratelimitd ${version}_"
    ""
    "- offered: $Rps decisions/s, $Duration after $Warmup warmup, workers=$Workers (0 = GOMAXPROCS), batch=$Batch subjects per tick, open-loop pacer (missed ticks reported, never hidden)"
    "- policy: tenant ``bench`` / ``load`` gcra rate 100000/s burst 10000 (policies.bench.yaml, one key per call); the 2pol row adds ``global_ingress`` gcra 200000/s burst 20000 from policies.example.yaml (two keys per call, dry-run then commit)"
    "- redis: $RedisAddr (redis:7-alpine in docker, REDIS_POOL_SIZE default 64, REDIS_TIMEOUT 50ms); local row: REDIS_ADDR=127.0.0.1:1 FALLBACK_MODE=local BREAKER_MIN=5"
    ""
    "| row | proto | backend | offered rps | keys | workers | batch | achieved/s | p50 | p99 | max | allowed (expected) | errors | missed ticks | degraded | gate |"
    "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"
)
$lines = [System.Collections.Generic.List[string]]::new()
$header | ForEach-Object { $lines.Add($_) }
$raw = [System.Collections.Generic.List[string]]::new()

$rowSpecs = @{
    'grpc-redis'      = @{ Proto = 'grpc'; Backend = 'redis'; Keys = 10000; Redis = $RedisAddr; Mode = 'local'; Policies = $benchPolicies }
    'grpc-redis-2pol' = @{ Proto = 'grpc'; Backend = 'redis'; Keys = 10000; Redis = $RedisAddr; Mode = 'local'; Policies = $examplePolicies }
    'grpc-redis-hot'  = @{ Proto = 'grpc'; Backend = 'redis'; Keys = 1; Redis = $RedisAddr; Mode = 'local'; Policies = $benchPolicies }
    'grpc-redis-deny' = @{ Proto = 'grpc'; Backend = 'redis'; Keys = 1; Redis = $RedisAddr; Mode = 'local'; Policies = $benchPolicies; Tenant = 'benchdeny'; Rate = 20000; Burst = 2000 }
    'http-redis'      = @{ Proto = 'http'; Backend = 'redis'; Keys = 10000; Redis = $RedisAddr; Mode = 'local'; Policies = $benchPolicies }
    'grpc-local'      = @{ Proto = 'grpc'; Backend = 'local'; Keys = 10000; Redis = '127.0.0.1:1'; Mode = 'local'; Policies = $benchPolicies }
}

$Rows = @($Rows | ForEach-Object { $_ -split ',' } | Where-Object { $_ })  # -File passes 'a,b' as one string
$started = Get-Date
foreach ($name in $Rows) {
    if (-not $rowSpecs.ContainsKey($name)) { throw "unknown row '$name' (known: $($rowSpecs.Keys -join ','))" }
    $spec = $rowSpecs[$name]
    $log = Join-Path $outDir "$stamp-$name.log"
    Write-Host "==> row $name : starting daemon (redis=$($spec.Redis))"
    $proc = Start-Daemon -Redis $spec.Redis -Mode $spec.Mode -Log $log -Policies $spec.Policies
    try {
        Write-Host "==> row $name : bench proto=$($spec.Proto) keys=$($spec.Keys) rps=$Rps"
        $tenant = if ($spec.ContainsKey('Tenant')) { $spec.Tenant } else { 'bench' }
        $rate = if ($spec.ContainsKey('Rate')) { "$($spec.Rate)" } else { '100000' }
        $burst = if ($spec.ContainsKey('Burst')) { "$($spec.Burst)" } else { '10000' }
        $benchArgs = @('bench', '-tenant', $tenant, '-proto', $spec.Proto, '-backend', $spec.Backend, '-keys', "$($spec.Keys)",
            '-rps', "$Rps", '-duration', $Duration, '-warmup', $Warmup, '-workers', "$Workers", '-batch', "$Batch", '-rate', $rate, '-burst', $burst)
        $out = & $ctl @benchArgs 2>&1
        if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne 3) { throw "ratelimitctl bench failed for $name`n$($out -join "`n")" }
        $raw.Add("### $name"); $raw.Add('```'); $out | ForEach-Object { $raw.Add([string]$_) }; $raw.Add('```'); $raw.Add('')
        $tableRow = $out | Where-Object { $_ -match '^\| (grpc|http) \|' } | Select-Object -Last 1
        $gate = if ($LASTEXITCODE -eq 0) { 'PASS' } else { 'FAIL' }
        if ($tableRow) { $lines.Add("| $name $($tableRow.Substring(1)) $gate |") }
        $out | ForEach-Object { Write-Host "    $_" }
    } finally {
        Stop-Daemon -Proc $proc
        Start-Sleep -Milliseconds 500
    }
}
$elapsed = (Get-Date) - $started
$lines.Add('')
$lines.Add("_total wall time: $([math]::Round($elapsed.TotalMinutes, 1)) min. Raw ratelimitctl output per row below; daemon logs next to this file (gitignored)._")
$lines.Add('')
$raw | ForEach-Object { $lines.Add($_) }
Set-Content -Path $outFile -Value ($lines -join "`n") -Encoding utf8
Write-Host "==> $outFile"
Get-Content $outFile | Select-Object -First 16
