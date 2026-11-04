param([switch]$SkipBuild, [switch]$Dashboard, [switch]$KeepData)
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$root = (Get-Location).Path
$env:GOCACHE = Join-Path $root '.cache/go-build'
$env:GOMODCACHE = Join-Path $root '.cache/modules'
if (!$SkipBuild) {
    foreach ($command in @('coordinator', 'storage-node', 'dfsctl')) {
        go build -o "bin/$command.exe" "./cmd/$command"
        if ($LASTEXITCODE) { throw "build failed: $command" }
    }
}
$work = Join-Path $root ('.cache/smoke-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $work -Force | Out-Null
$token = 'smoke-only-internal-token'
$processes = [System.Collections.Generic.List[System.Diagnostics.Process]]::new()
$nodeProcesses = @{}
function Start-ServiceProcess($binary, $configuration, $label) {
    $p = Start-Process -FilePath (Join-Path $root "bin/$binary.exe") -ArgumentList @('-config', ('"' + $configuration + '"')) -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $work "$label.log") -RedirectStandardError (Join-Path $work "$label.err")
    $processes.Add($p)
    return $p
}
function Wait-Until($description, [scriptblock]$predicate) {
    $deadline = [DateTime]::UtcNow.AddSeconds(55)
    do {
        try { if (& $predicate) { return } } catch { }
        Start-Sleep -Milliseconds 250
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out: $description. Logs: $work"
}
function Invoke-Dfs([string[]]$cliArgs) {
    $result = & (Join-Path $root 'bin/dfsctl.exe') @cliArgs
    if ($LASTEXITCODE) { throw "dfsctl failed: $cliArgs" }
    return $result
}
function Download-And-Verify($id, $source, $label) {
    $destination = Join-Path $work "$label.download"
    Invoke-Dfs @('download', $id, '--output', $destination) | Out-Null
    if ((Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash -ne (Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash) { throw "checksum mismatch: $label" }
}
$previousAddress = $env:DFS_ADDRESS
$env:DFS_ADDRESS = 'http://127.0.0.1:18080'
$coordinatorConfig = Join-Path $work 'coordinator.yaml'
$db = (Join-Path $work 'metadata.db').Replace('\', '/')
@"
listen: 127.0.0.1:18080
database: '$db'
internal_token: $token
chunk_size: 1048576
heartbeat_interval: 1s
failure_timeout: 2s
repair_interval: 1s
gc_grace: 0s
allowed_node_addresses:
  - http://127.0.0.1:19001
  - http://127.0.0.1:19002
  - http://127.0.0.1:19003
  - http://127.0.0.1:19004
"@ | Set-Content -LiteralPath $coordinatorConfig -Encoding UTF8
function Start-Node([int]$i) {
    $configuration = Join-Path $work "node-$i.yaml"
    $directory = (Join-Path $work "node-$i").Replace('\', '/')
    $port = 19000 + $i
@"
listen: 127.0.0.1:$port
node_id: smoke-$i
advertise_address: http://127.0.0.1:$port
coordinator_address: http://127.0.0.1:18080
data_dir: '$directory'
capacity_bytes: 536870912
internal_token: $token
heartbeat_interval: 1s
failure_timeout: 2s
"@ | Set-Content -LiteralPath $configuration -Encoding UTF8
    $nodeProcesses[$i] = Start-ServiceProcess 'storage-node' $configuration "node-$i-$([guid]::NewGuid().ToString('N'))"
}
try {
    $coordinatorProcess = Start-ServiceProcess 'coordinator' $coordinatorConfig 'coordinator'
    Wait-Until 'coordinator health' { (Invoke-RestMethod "$env:DFS_ADDRESS/health").status -eq 'ok' }
    1..3 | ForEach-Object { Start-Node $_ }
    Wait-Until 'three healthy nodes' { (Invoke-RestMethod "$env:DFS_ADDRESS/api/v1/cluster/status").healthy_nodes -eq 3 }
    if ($Dashboard) {
        $previousProxy = $env:DFS_API_PROXY
        try {
            $env:DFS_API_PROXY = $env:DFS_ADDRESS
            $vitePath = Join-Path $root 'web/node_modules/vite/bin/vite.js'
            $preview = Start-Process -FilePath (Get-Command node).Source -ArgumentList @(('"' + $vitePath + '"'), '--host', '127.0.0.1', '--port', '15173', '--strictPort') -WorkingDirectory (Join-Path $root 'web') -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $work 'dashboard.log') -RedirectStandardError (Join-Path $work 'dashboard.err')
            $processes.Add($preview)
            Wait-Until 'dashboard page and live API proxy' { (Invoke-WebRequest 'http://127.0.0.1:15173/' -UseBasicParsing).StatusCode -eq 200 -and @((Invoke-RestMethod 'http://127.0.0.1:15173/api/v1/nodes')).Count -eq 3 }
        } finally { $env:DFS_API_PROXY = $previousProxy }
    }
    $small = Join-Path $work 'small.txt'
    'small process-level smoke test' | Set-Content -LiteralPath $small
    $large = Join-Path $work 'streamed.bin'
    $stream = [IO.File]::Create($large)
    try { $buffer = New-Object byte[] 1048576; $random = [Random]::new(42); for ($i=0; $i -lt 32; $i++) { $random.NextBytes($buffer); $stream.Write($buffer, 0, $buffer.Length) } } finally { $stream.Dispose() }
    $smallMeta = (Invoke-Dfs @('upload', $small)) -join "`n" | ConvertFrom-Json
    $meta = (Invoke-Dfs @('upload', $large)) -join "`n" | ConvertFrom-Json
    if ($meta.chunks.Count -ne 32) { throw 'expected 32 streamed chunks' }
    foreach ($ch in $meta.chunks) { if (($ch.replica_nodes | Select-Object -Unique).Count -ne 3) { throw 'expected three distinct replicas' } }
    Download-And-Verify $smallMeta.id $small 'small'
    Download-And-Verify $meta.id $large 'large'
    Stop-Process -Id $nodeProcesses[1].Id
    Wait-Until 'under-replication after node failure' { (Invoke-RestMethod "$env:DFS_ADDRESS/api/v1/replication/status").under_replicated_chunks -gt 0 }
    Download-And-Verify $meta.id $large 'fallback'
    Start-Node 4
    Wait-Until 'replacement repair' { (Invoke-RestMethod "$env:DFS_ADDRESS/api/v1/cluster/status").healthy_nodes -eq 3 -and (Invoke-RestMethod "$env:DFS_ADDRESS/api/v1/replication/status").under_replicated_chunks -eq 0 }
    Download-And-Verify $meta.id $large 'repaired'
    Stop-Process -Id $coordinatorProcess.Id
    $coordinatorProcess.WaitForExit()
    $coordinatorProcess = Start-ServiceProcess 'coordinator' $coordinatorConfig 'coordinator-restarted'
    Wait-Until 'coordinator restart reconciliation' { (Invoke-RestMethod "$env:DFS_ADDRESS/api/v1/cluster/status").healthy_nodes -ge 3 }
    Download-And-Verify $meta.id $large 'restart'
    Stop-Process -Id $nodeProcesses[2].Id
    $nodeProcesses[2].WaitForExit()
    $restartAt = [DateTime]::UtcNow
    Start-Node 2
    Wait-Until 'node restart reconciliation' { @((Invoke-RestMethod "$env:DFS_ADDRESS/api/v1/nodes") | Where-Object { $_.node_id -eq 'smoke-2' -and $_.status -eq 'HEALTHY' -and [DateTime]$_.last_heartbeat -gt $restartAt }).Count -eq 1 }
    Download-And-Verify $meta.id $large 'node-restart'
    Wait-Until 'ready for strict benchmark writes' { (Invoke-RestMethod "$env:DFS_ADDRESS/ready").status -eq 'ready' }
    Invoke-Dfs @('benchmark', '--size', '1048576', '--requests', '3', '--concurrency', '2') | Set-Content -LiteralPath (Join-Path $work 'benchmark.json')
    [pscustomobject]@{ status='PASS'; bytes=33554432; chunks=32; replicas=3; checks=@('registration','heartbeats','small upload','streamed upload','SHA-256 downloads','node failure','fallback','degraded status','replacement repair','coordinator restart','node restart','CLI benchmark'); artifacts=$work } | ConvertTo-Json -Depth 4
} finally {
    foreach ($p in $processes) { if (!$p.HasExited) { Stop-Process -Id $p.Id -ErrorAction SilentlyContinue }; $p.WaitForExit() }
    $env:DFS_ADDRESS = $previousAddress
    if (!$KeepData) {
        # Clean only payloads created by this invocation, retaining logs and benchmark JSON.
        $ownedRoot = [IO.Path]::GetFullPath($work)
        foreach ($entry in (Get-ChildItem -LiteralPath $work)) {
            if ($entry.Name -notmatch '^(node-[1-4]|streamed\.bin|small\.txt|.*\.download)$') { continue }
            $cleanupPath = [IO.Path]::GetFullPath($entry.FullName)
            if (!$cleanupPath.StartsWith($ownedRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) { throw 'Refusing cleanup outside this smoke run' }
            Remove-Item -LiteralPath $cleanupPath -Recurse -Force
        }
    }
}


