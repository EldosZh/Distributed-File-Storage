$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$env:GOCACHE = Join-Path (Get-Location) '.cache/go-build'
$env:GOMODCACHE = Join-Path (Get-Location) '.cache/modules'
go mod tidy
if ($LASTEXITCODE) { throw 'go mod tidy failed' }
gofmt -w cmd internal pkg
go vet ./...
if ($LASTEXITCODE) { throw 'go vet failed' }
go test ./...
if ($LASTEXITCODE) { throw 'go test failed' }
go test -race ./...
if ($LASTEXITCODE) { throw 'race check failed (requires CGO and a C compiler)' }
go build ./...
if ($LASTEXITCODE) { throw 'build failed' }
npm --prefix web ci --no-audit --no-fund
if ($LASTEXITCODE) { throw 'npm ci failed' }
npm --prefix web run build
if ($LASTEXITCODE) { throw 'dashboard build failed' }
