$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $root

$targetLine = rustc -vV | Where-Object { $_ -match '^host: ' } | Select-Object -First 1
if ($LASTEXITCODE -ne 0 -or -not $targetLine) { throw 'could not determine the Rust host target' }
$target = $targetLine -replace '^host: ', ''

$metadataJson = cargo metadata --manifest-path Cargo.toml --format-version 1 --no-deps
if ($LASTEXITCODE -ne 0) { throw 'cargo metadata failed' }
$metadata = $metadataJson | ConvertFrom-Json
$packages = @($metadata.packages | Where-Object { $_.name -eq 'tinychannels-module' })
if ($packages.Count -ne 1) { throw 'expected exactly one tinychannels-module package' }
$version = $packages[0].version
$outputDir = 'target/module-e2e'

& (Join-Path $PSScriptRoot 'build-module.ps1') -Crate tinychannels-module -Version $version -Target $target -HostKey ci -OutputDir $outputDir
if ($LASTEXITCODE -ne 0) { throw 'tinychannels module build failed' }
& (Join-Path $PSScriptRoot 'verify-module.ps1') -Archive "$outputDir/tinychannels-module-$version-ci.zip"
if ($LASTEXITCODE -ne 0) { throw 'tinychannels module end-to-end verification failed' }
