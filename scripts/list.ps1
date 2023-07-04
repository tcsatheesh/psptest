[CmdletBinding()]
param(
    [Parameter()]
    [string]$run_state = "running",

    [Parameter()]
    [string]$workspace_name,

    [Parameter()]    
    [string]$spark_pool_name
)

$LIVY_ID=az synapse spark job list `
--workspace $workspace_name `
--spark-pool-name $spark_pool_name `
| jq -r `
".sessions[] | select(.state == `"$run_state`") | .id"

foreach ($id in $LIVY_ID) {
    Write-Host "Livy ID: $id"
}

