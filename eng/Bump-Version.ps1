<#
.SYNOPSIS
  Bumps the NuGet package version in Directory.Build.props.

.PARAMETER Part
  Which part to bump: Major, Minor, or Patch (default: Patch).

.PARAMETER Version
  Set an explicit version instead of bumping (e.g. "10.1.0").

.EXAMPLE
  .\eng\Bump-Version.ps1                  # 10.0.0 -> 10.0.1
  .\eng\Bump-Version.ps1 -Part Minor      # 10.0.0 -> 10.1.0
  .\eng\Bump-Version.ps1 -Version 11.0.0  # sets 11.0.0
#>
[CmdletBinding()]
param(
    [ValidateSet('Major', 'Minor', 'Patch')]
    [string]$Part = 'Patch',

    [string]$Version
)

$propsPath = Join-Path $PSScriptRoot '..\src\IntegratedS3\Directory.Build.props'
$propsPath = [System.IO.Path]::GetFullPath($propsPath)

if (-not (Test-Path $propsPath)) {
    Write-Error "Directory.Build.props not found at $propsPath"
    exit 1
}

$content = Get-Content $propsPath -Raw

if ($content -notmatch '<VersionPrefix>(\d+)\.(\d+)\.(\d+)</VersionPrefix>') {
    Write-Error 'Could not find <VersionPrefix>x.y.z</VersionPrefix> in Directory.Build.props'
    exit 1
}

$currentVersion = "$($Matches[1]).$($Matches[2]).$($Matches[3])"

if ($Version) {
    if ($Version -notmatch '^\d+\.\d+\.\d+$') {
        Write-Error "Invalid version format '$Version'. Expected Major.Minor.Patch (e.g. 10.1.0)."
        exit 1
    }
    $newVersion = $Version
}
else {
    [int]$major = $Matches[1]
    [int]$minor = $Matches[2]
    [int]$patch = $Matches[3]

    switch ($Part) {
        'Major' { $major++; $minor = 0; $patch = 0 }
        'Minor' { $minor++; $patch = 0 }
        'Patch' { $patch++ }
    }

    $newVersion = "$major.$minor.$patch"
}

$updated = $content -replace '<VersionPrefix>\d+\.\d+\.\d+</VersionPrefix>', "<VersionPrefix>$newVersion</VersionPrefix>"
Set-Content $propsPath $updated -NoNewline

Write-Host "Version bumped: $currentVersion -> $newVersion"
