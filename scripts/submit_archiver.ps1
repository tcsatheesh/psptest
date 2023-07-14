[CmdletBinding()]
param(
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
    [string]$blob_name = "archiver.py",

    [Parameter()]
    [string]$keyvault_name,

    [Parameter()]
    [string]$data_path = "output",

    [Parameter()]
    [string]$metadata_path = "metadata",

    [Parameter()]
    [string]$report_path = "report",

    [Parameter()]
    [string]$temp_path = "temp",

    [Parameter()]
    [string]$archive_date,

    [Parameter()]
    [int]$max_files_per_job = -1
)


$arguments = " --keyvault-name $keyvault_name"
$arguments += " --keyvault-linked-service-name $keyvault_name"
$arguments += " --data-path $data_path"
$arguments += " --metadata-path $metadata_path"
$arguments += " --report-path $report_path"
$arguments += " --temp-path $temp_path"
$arguments += " --archive-date $archive_date"
$arguments += " --max-files-per-job $max_files_per_job"


$name = "$blob_name"

Write-Host "Arguments: $arguments"
Write-Host "Name of the job: $name"
Write-Host "Spark pool: $spark_pool_name and executor size: $executor_size and number of executors: $number_of_executors"

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

# $LIVY_ID=az synapse spark job submit `
# --name $name `
# --workspace-name $workspace_name `
# --spark-pool-name $spark_pool_name `
# --main-definition-file $main_definition_file `
# --arguments $arguments `
# --executors $number_of_executors `
# --executor-size $executor_size `
# | jq -r ".id"


# Write-Host "Livy ID: $LIVY_ID"
