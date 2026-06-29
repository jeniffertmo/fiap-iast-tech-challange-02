[CmdletBinding()]
param(
    [Alias('p')]
    [string]$Profile,

    [Parameter(Mandatory, Position = 0)]
    [string]$GlueScript
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

function Get-AwsFromProfile {
    param([string]$AwsProfile)
    $env:AWS_ACCESS_KEY_ID     = aws configure get aws_access_key_id     --profile $AwsProfile
    $env:AWS_SECRET_ACCESS_KEY = aws configure get aws_secret_access_key --profile $AwsProfile
    $env:AWS_DEFAULT_REGION    = aws configure get region                 --profile $AwsProfile
    $token = aws configure get aws_session_token --profile $AwsProfile 2>$null
    $env:AWS_SESSION_TOKEN = if ($token) { $token } else { $null }
}

function Assert-AwsEnvVars {
    foreach ($var in @('AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_DEFAULT_REGION')) {
        if (-not [Environment]::GetEnvironmentVariable($var)) {
            Write-Error "Error: $var is not set. Provide -Profile <profile> or set the credential env vars."
        }
    }
    if (-not $env:AWS_SESSION_TOKEN) { $env:AWS_SESSION_TOKEN = $null }
}

$GlueScriptPath = Join-Path $ScriptDir $GlueScript
if (-not (Test-Path $GlueScriptPath)) {
    Write-Error "Error: script '$GlueScriptPath' not found."
}

if ($Profile) {
    Get-AwsFromProfile -AwsProfile $Profile
} else {
    Assert-AwsEnvVars
}

$ScriptBasename   = Split-Path -Leaf $GlueScriptPath
$WorkspaceLocation = $ScriptDir

Write-Host "WORKSPACE_LOCATION : $WorkspaceLocation"
Write-Host "AWS_DEFAULT_REGION : $($env:AWS_DEFAULT_REGION)"
Write-Host ""

$dockerArgs = @(
    'run', '-it', '--rm',
    '-v', "${WorkspaceLocation}:/home/hadoop/workspace/",
    '-e', "AWS_ACCESS_KEY_ID=$($env:AWS_ACCESS_KEY_ID)",
    '-e', "AWS_SECRET_ACCESS_KEY=$($env:AWS_SECRET_ACCESS_KEY)",
    '-e', "AWS_DEFAULT_REGION=$($env:AWS_DEFAULT_REGION)"
)
if ($env:AWS_SESSION_TOKEN) {
    $dockerArgs += '-e', "AWS_SESSION_TOKEN=$($env:AWS_SESSION_TOKEN)"
}
$dockerArgs += @(
    '--name', 'glue5_spark_submit',
    'public.ecr.aws/glue/aws-glue-libs:5',
    'spark-submit', "/home/hadoop/workspace/$ScriptBasename"
)

& docker @dockerArgs
