param(
    [Parameter(Mandatory = $true)][string]$Target,
    [Parameter(Mandatory = $true)][string]$Version,
    [Parameter(Mandatory = $true)][string]$OutputDirectory
)

$ErrorActionPreference = "Stop"

if ($Target -notmatch '^[A-Za-z0-9_.-]+$' -or
    $Version -notmatch '^v?[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9.-]+)?$') {
    throw "target or version contains unsupported characters"
}

$packageName = "meshquill-$Version-$Target"
$binary = Join-Path (Join-Path (Join-Path "target" $Target) "dist") "meshquill.exe"

cargo build --locked --profile dist --target $Target --package meshquill
if ($LASTEXITCODE -ne 0) { throw "cargo build failed" }

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$resolvedOutputDirectory = (Resolve-Path $OutputDirectory).Path
$stageParent = Join-Path ([System.IO.Path]::GetTempPath()) ("meshquill-release-" + [guid]::NewGuid())
$stageRoot = Join-Path $stageParent $packageName

try {
    $binDirectory = Join-Path $stageRoot "bin"
    $manDirectory = Join-Path $stageRoot "share/man/man1"
    $completionDirectory = Join-Path $stageRoot "share/completions"
    New-Item -ItemType Directory -Force -Path $binDirectory, $manDirectory, $completionDirectory | Out-Null
    Copy-Item $binary (Join-Path $binDirectory "meshquill.exe")
    Copy-Item "README.md", "CHANGELOG.md", "CONTRIBUTING.md", "SECURITY.md", "STATUS.md", "PLAN.md", "LICENSE-APACHE", "LICENSE-MIT" $stageRoot
    Copy-Item "docs", "examples" $stageRoot -Recurse

    $completionFiles = @{
        "bash" = (Join-Path $completionDirectory "meshquill.bash")
        "zsh" = (Join-Path $completionDirectory "_meshquill")
        "fish" = (Join-Path $completionDirectory "meshquill.fish")
        "powershell" = (Join-Path $completionDirectory "_meshquill.ps1")
    }
    foreach ($shell in $completionFiles.Keys) {
        $completionPath = $completionFiles[$shell]
        & $binary completions $shell | Set-Content -Encoding utf8NoBOM $completionPath
        if ($LASTEXITCODE -ne 0 -or !(Test-Path -LiteralPath $completionPath) -or (Get-Item -LiteralPath $completionPath).Length -eq 0) {
            throw "completion generation failed for $shell"
        }
    }
    & $binary manpages $manDirectory
    if ($LASTEXITCODE -ne 0) { throw "reference asset generation failed" }
    $rootManpage = Join-Path $manDirectory "meshquill.1"
    if (!(Test-Path -LiteralPath $rootManpage) -or (Get-Item -LiteralPath $rootManpage).Length -eq 0) {
        throw "root manpage generation produced no content"
    }

    $archive = Join-Path $resolvedOutputDirectory "$packageName.zip"
    Compress-Archive -Path $stageRoot -DestinationPath $archive -CompressionLevel Optimal -Force
    $hash = (Get-FileHash -Algorithm SHA256 $archive).Hash.ToLowerInvariant()
    $checksum = "$hash  $([System.IO.Path]::GetFileName($archive))`n"
    [System.IO.File]::WriteAllText("$archive.sha256", $checksum, [System.Text.UTF8Encoding]::new($false))
}
finally {
    if (Test-Path -LiteralPath $stageParent) {
        Remove-Item -LiteralPath $stageParent -Recurse -Force
    }
}
