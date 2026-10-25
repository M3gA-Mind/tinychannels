param([Parameter(Mandatory = $true)][string]$Archive)
$ErrorActionPreference = 'Stop'
$resolvedArchive = (Resolve-Path -LiteralPath $Archive).Path
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $root
$targetRoot = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $root 'target' }
New-Item -ItemType Directory -Force $targetRoot | Out-Null
$targetRoot = (Resolve-Path -LiteralPath $targetRoot).Path
$rootPrefix = $root.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
if (-not $targetRoot.StartsWith($rootPrefix, [StringComparison]::OrdinalIgnoreCase)) { throw 'CARGO_TARGET_DIR must stay inside the repository' }
$work = Join-Path $targetRoot ("tinychannels-module-verify-$([System.Guid]::NewGuid().ToString('N'))")
New-Item -ItemType Directory $work | Out-Null
try {
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [System.IO.Compression.ZipFile]::OpenRead($resolvedArchive)
    try {
        foreach ($entry in $zip.Entries) {
            $parts = $entry.FullName -split '[\\/]'
            $mode = ($entry.ExternalAttributes -shr 16) -band 0xF000
            if ([IO.Path]::IsPathRooted($entry.FullName) -or ($parts -contains '..') -or ($parts -contains '.') -or $mode -eq 0xA000) {
                throw "Archive member is unsafe: $($entry.FullName)"
            }
        }
    } finally {
        $zip.Dispose()
    }
    # TinyBus only admits a Windows bundle private to the runner account.
    $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $security = [System.Security.AccessControl.DirectorySecurity]::new()
    $security.SetOwner($identity.User)
    $security.SetAccessRuleProtection($true, $false)
    foreach ($sidValue in @($identity.User.Value, 'S-1-5-18', 'S-1-5-32-544')) {
        $rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
            [System.Security.Principal.SecurityIdentifier]::new($sidValue),
            [System.Security.AccessControl.FileSystemRights]::FullControl,
            [System.Security.AccessControl.InheritanceFlags]'ContainerInherit, ObjectInherit',
            [System.Security.AccessControl.PropagationFlags]::None,
            [System.Security.AccessControl.AccessControlType]::Allow)
        [void]$security.AddAccessRule($rule)
    }
    Set-Acl -LiteralPath $work -AclObject $security
    Expand-Archive -LiteralPath $resolvedArchive -DestinationPath $work
    $library = Join-Path $work 'tinychannels_module.dll'
    if (-not (Test-Path (Join-Path $work 'modules.toml'))) { throw 'modules.toml missing from archive' }
    $env:TINYCHANNELS_MODULE_ARTIFACT = $library
    # The loader admits one dynamic module per test process. Keep each module
    # artifact case separate while preserving the routing and runtime targets.
    cargo test --locked --release --manifest-path Cargo.toml --package tinychannels-module --test module_artifact compiled_artifact_exposes_confidential_controller_proxy_and_websocket_lifecycle -- --ignored
    if ($LASTEXITCODE -ne 0) { throw 'tinychannels module lifecycle artifact verification failed' }
    cargo test --locked --release --manifest-path Cargo.toml --package tinychannels-module --test module_artifact compiled_artifact_ingests_scoped_media_and_sends_its_opaque_reference -- --ignored
    if ($LASTEXITCODE -ne 0) { throw 'tinychannels media artifact verification failed' }
    cargo test --locked --release --manifest-path Cargo.toml --package tinychannels-module --test module_artifact compiled_artifact_authenticates_whatsapp_cloud_webhooks_and_runs_native_v2 -- --ignored
    if ($LASTEXITCODE -ne 0) { throw 'tinychannels WhatsApp artifact verification failed' }
    cargo test --locked --release --manifest-path Cargo.toml --package tinychannels-module --test routing_artifact --test runtime_artifact -- --ignored
    if ($LASTEXITCODE -ne 0) { throw 'tinychannels routing/runtime artifact verification failed' }
} finally {
    Remove-Item -LiteralPath $work -Recurse -Force
}
