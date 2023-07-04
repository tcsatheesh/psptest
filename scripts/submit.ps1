[CmdletBinding()]
param(
    [Parameter()]
    [int]$max_per_trigger = 10,

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
    [string]$blob_name = "data_loader.py",

    [Parameter()]
    [string]$keyvault_name,

    [Parameter()]
    [string]$input_paths = "input0",

    [Parameter()]
    [string]$first_timestamp_column_name = "processing_time",

    [Parameter()]
    [string]$partitionby = "loan_purpose",

    [Parameter()]
    [string]$output_path = "output",

    [Parameter()]
    [string]$checkpoint_path = "checkpoint"
)



$arguments = "--max-per-trigger $max_per_trigger"
$arguments += " --processing-time-in-seconds $processing_time_in_seconds"
$arguments += " --keyvault-name $keyvault_name"
$arguments += " --keyvault-linked-service-name $keyvault_name"
$arguments += " --clear-output"
$arguments += " --input-paths $input_paths"
$arguments += " --output-path $output_path"
$arguments += " --checkpoint-path $checkpoint_path"
$arguments += " --partitionby $partitionby"
$arguments += " --first-timestamp-column-name $first_timestamp_column_name"

$name = "data_loader max_trigger:$max_per_trigger, processing_time:$processing_time_in_seconds seconds"

Write-Host "Arguments: $arguments"
Write-Host "Name of the job: $name"

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
