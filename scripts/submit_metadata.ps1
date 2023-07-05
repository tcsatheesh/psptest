[CmdletBinding()]
param(
    [Parameter()]
    [int]$max_events_per_trigger = 10,

    [Parameter()]
    [int]$processing_time_in_seconds = 10,

    [Parameter()]
    [string]$workspace_name,

    [Parameter()]    
    [string]$spark_pool_name,

    [Parameter()]
    [int]$number_of_executors = 2,

    [Parameter()]
    [ValidateSet("Small", "Medium", "Large")]
    [string]$executor_size = "Small",

    [Parameter()]
    [string]$storage_account_name,

    [Parameter()]
    [string]$container_name = "jobs",

    [Parameter()]
    [string]$blob_name = "metadata_loader.py",

    [Parameter()]
    [string]$keyvault_name,

    [Parameter()]
    [string]$partitionby = "loan_purpose",

    [Parameter()]
    [string]$output_path = "metadata",

    [Parameter()]
    [string]$checkpoint_path = "checkpoint/metadata",

    [Parameter()]
    [string]$consumer_group = "pyspark"
)



$arguments = "--max-events-per-trigger $max_events_per_trigger"
$arguments += " --processing-time-in-seconds $processing_time_in_seconds"
$arguments += " --keyvault-name $keyvault_name"
$arguments += " --keyvault-linked-service-name $keyvault_name"
$arguments += " --output-path $output_path"
$arguments += " --checkpoint-path $checkpoint_path"
$arguments += " --partitionby $partitionby"
$arguments += " --consumer-group $consumer_group"
$arguments += " --clear-checkpoint"


$name = "metadata_loader max_trigger:$max_per_trigger, processing_time:$processing_time_in_seconds seconds"

Write-Host "Arguments: $arguments"
Write-Host "Name of the job: $name"
Write-Host "Spark pool: $spark_pool_name and executor size: $executor_size"

$main_definition_file = "abfss://$container_name@$storage_account_name.dfs.core.windows.net/$blob_name"

Write-Host "Main definition file: $main_definition_file"

$BLOB_UPLOAD_STATUS=az storage blob upload `
    --account-name $storage_account_name `
    --container-name $container_name `
    --name $blob_name `
    --file $blob_name `
    --overwrite `
    --auth-mode login `
    | jq -r ".lastModified"

Write-Host "Blob last modified: $BLOB_UPLOAD_STATUS"

$LIVY_ID=az synapse spark job submit `
--name $name `
--workspace-name $workspace_name `
--spark-pool-name $spark_pool_name `
--main-definition-file $main_definition_file `
--arguments $arguments `
--executors $number_of_executors `
--executor-size $executor_size `
| jq -r ".id"


Write-Host "Livy ID: $LIVY_ID"
