import json
import sys
import argparse
import logging
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient, ContainerClient

THREAD_POOL_ID_NAME = "threadPoolId"
INPUT_FILE_COLUMN_NAME = "source_file"
CURRENT_PROCESSING_TIME_COLUMN_NAME = "processing_time"

LOGGING_FORMAT = f"%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class CustomLogger:
    def __init__(
        self,
        logger_name,
        log_level,
    ):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(log_level)
        logging.basicConfig(
            format=LOGGING_FORMAT,
        )

    def get_logger(
        self,
    ):
        return self.logger


class Sparker:
    def __init__(
        self,
        app_name,
        logger,
        logger_extra,
        key_vault_name,
        key_vault_linked_service_name,
    ):
        self.logger = logger
        self.logger_extra = logger_extra
        self.logger.info(f"Spark appName is {app_name}", extra=self.logger_extra)
        self.spark = SparkSession.builder.appName(
            app_name,
        ).getOrCreate()
        self.key_vault_name = key_vault_name
        self.key_vault_linked_service_name = key_vault_linked_service_name
        self.token_library = (
            self.spark.sparkContext._jvm.com.microsoft.azure.synapse.tokenlibrary.TokenLibrary
        )

        self.logger.info(
            f"Spark version: {self.spark.version}",
            extra=self.logger_extra,
        )

        self.logger.info(
            "Spark initialized",
            extra=self.logger_extra,
        )

    def get_spark(self):
        return self.spark

    def get_secret(
        self,
        secret_name,
    ):
        return self.token_library.getSecret(
            self.key_vault_name,
            secret_name,
            self.key_vault_linked_service_name,
        )


class ClearFolders(Sparker):  # for debugging and performance testing only
    def __init__(
        self,
        args,
        logger,
        logger_extra,
        key_vault_name,
        key_vault_linked_service_name,
    ):
        super().__init__(
            app_name="ClearFolders",
            logger=logger,
            logger_extra=logger_extra,
            key_vault_name=key_vault_name,
            key_vault_linked_service_name=key_vault_linked_service_name,
        )
        self.args = args

    def clear_folder(
        self,
        folder_path,
    ):
        from notebookutils import mssparkutils

        for _file_info in mssparkutils.fs.ls(folder_path):
            mssparkutils.fs.rm(_file_info.path, recurse=True)

    def clear_folders(
        self,
    ):
        args = self.args
        logger = self.logger
        storage_account_name = self.get_secret("storage-account-name")
        container_name = self.get_secret("container-name")
        output_path = args.output_path
        checkpoint_path = args.checkpoint_path

        container_path = (
            f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
        )
        output_file_path = f"{container_path}/{output_path}"
        checkpoint_file_path = f"{container_path}/{checkpoint_path}"

        if self.args.clear_output:
            logger.info(
                f"Clearing output folder {output_file_path}", extra=self.logger_extra
            )
            self.clear_folder(output_file_path)
        if self.args.clear_checkpoint:
            logger.info(
                f"Clearing checkpoint folder {checkpoint_file_path}",
                extra=self.logger_extra,
            )
            self.clear_folder(checkpoint_file_path)


class StorageHandler(Sparker):
    def __init__(
        self,
        storage_account_name,
        container_name,
        tenant_id,
        client_id,
        client_secret,
    ):
        self.storage_account_name = storage_account_name
        self.container_name = container_name
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    def get_blob_client(
        self,
        file_name,
    ):
        storage_account_name = self.storage_account_name
        container_name = self.container_name
        credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        _file_url = file_name.replace(
            "abfss",
            "https",
        )
        _file_url = _file_url.replace(
            f"{container_name}@{storage_account_name}.dfs.core.windows.net",
            f"{storage_account_name}.blob.core.windows.net/{container_name}",
        )
        print(f"file_url for blob client: {_file_url}")
        blob_client = BlobClient.from_blob_url(
            _file_url,
            credential,
        )
        return blob_client

    def get_file_properties(
        self,
        file_name,
    ):
        _start_time = datetime.utcnow()
        try:
            blob_client = self.get_blob_client(file_name)
            blob_properties = blob_client.get_blob_properties()
            return {
                "size": f"{blob_properties.size}",
                "last_modified": f"{blob_properties.last_modified.strftime('%Y-%m-%dT%H:%M:%S.%f')}",
            }
        except:
            return {
                "size": "not_found",
                "last_modified": f"not_found",
            }
        finally:
            _end_time = datetime.utcnow()
            print(f"Time taken to get file properties: {_end_time - _start_time}")


class LoadMetaDataSet(Sparker):
    def __init__(
        self,
        logger,
        logger_extra,
        key_vault_name,
        key_vault_linked_service_name,
        args,
    ):
        super().__init__(
            app_name=logger_extra[THREAD_POOL_ID_NAME],
            logger=logger,
            logger_extra=logger_extra,
            key_vault_name=key_vault_name,
            key_vault_linked_service_name=key_vault_linked_service_name,
        )
        self.args = args
        logger_extra = logger_extra
        storage_account_name = self.get_secret("storage-account-name")
        container_name = self.get_secret("container-name")
        input_eventhub_connection_string = self.get_secret(
            "inputfile-eventhub-connection-string"
        )
        output_eventhub_connection_string = self.get_secret(
            "processed-eventhub-connection-string"
        )
        output_path = args.output_path
        checkpoint_path = args.checkpoint_path
        partitionby = args.partitionby

        self.partitionby = args.partitionby
        container_path = (
            f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
        )
        self.output_file_path = f"{container_path}/{output_path}"
        self.checkpoint_file_path = f"{container_path}/{checkpoint_path}"
        self.input_eh_conf = {
            "eventhubs.connectionString": self.spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
                input_eventhub_connection_string,
            )
        }
        self.output_eh_conf = {
            "eventhubs.connectionString": self.spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
                output_eventhub_connection_string,
            )
        }

        logger.info(
            f"Storage account name is {storage_account_name}",
            extra=logger_extra,
        )
        logger.info(
            f"Container name is {container_name}",
            extra=logger_extra,
        )
        logger.info(
            f"Output path is {self.output_file_path}",
            extra=logger_extra,
        )
        logger.info(
            f"Checkpoint path is {self.checkpoint_file_path}",
            extra=logger_extra,
        )
        logger.info(
            f"Partition by is {partitionby}",
            extra=logger_extra,
        )

        self.storage_account_name = self.get_secret("storage-account-name")
        self.container_name = self.get_secret("container-name")
        self.tenant_id = self.get_secret("tenant-id")
        self.client_id = self.get_secret("client-id")
        self.client_secret = self.get_secret("client-secret")

    def process_data_batch(
        self,
        bdf,
        batch_id,
    ):
        bdf.persist()
        _overall_start_time = datetime.utcnow()
        bdf.write.format("delta").mode("append").save(f"{self.output_file_path}")
        _overall_end_time = datetime.utcnow()
        _logger_msg = f"TotalTime: {_overall_end_time - _overall_start_time}"
        self.logger.info(_logger_msg)

    def get_input_data_schema(
        self,
    ):
        # input_data_schema = (
        #     StructType()
        #     .add("name", "string")
        #     .add("age", "integer")
        #     .add("first_time_buyer", "boolean")
        #     .add("ltv", "float")
        #     .add("loan_amount", "float")
        #     .add("loan_term", "integer")
        #     .add("interest_rate", "float")
        #     .add("loan_purpose", "string")
        #     .add("loan_type", "string")
        #     .add("loan_id", "integer")
        # )
        # input_data_schema = F.schema_of_csv(
        #     "test_0,45,True,70,153186,240,0.05,Not Available,Conventional,0"
        # )
        input_data_schema = "`name` STRING, `age` INT, `first_time_buyer` BOOLEAN, `ltv` INT, `loan_amount` INT, `loan_term` INT, `interest_rate` FLOAT, `loan_purpose` STRING, `loan_type` STRING, `loan_id` INT"
        return input_data_schema

    def process_events(
        self,
        max_events_per_trigger,
        processing_time_in_seconds,
    ):
        self.input_eh_conf["maxEventsPerTrigger"] = max_events_per_trigger
        self.input_eh_conf["eventhubs.consumerGroup"] = self.args.consumer_group
        self.input_eh_conf["eventhubs.prefetchCount"] = 500
        self.input_eh_conf["eventhubs.threadPoolSize"] = 64

        # Start from beginning of stream
        startOffset = "-1"

        # Create the positions
        startingEventPosition = {
            "offset": startOffset,
            "seqNo": -1,  # not in use
            "enqueuedTime": None,  # not in use
            "isInclusive": True,
        }
        self.input_eh_conf["eventhubs.startingPosition"] = json.dumps(
            startingEventPosition
        )

        df = (
            self.spark.readStream.format("eventhubs")
            .options(**self.input_eh_conf)
            .load()
        )

        transformed_df = df.withColumn("body", df["body"].cast("string"))

        input_schema = self.get_input_data_schema()
        properties_schema = (
            StructType()
            .add("source_file", StringType())
            .add("size", IntegerType())
            .add("created_datetime", TimestampType())
        )

        transformed_df = transformed_df.withColumn(
            "csv",
            F.from_csv(
                "body",
                input_schema,
                options= {
                    "sep": ",",
                    "ignoreLeadingWhiteSpace": "true",
                    "ignoreTrailingWhiteSpace": "true",
                    "header": "true",
                    "lineSep": "\n",
                }
            ),
        )
        transformed_df = transformed_df.withColumn(
            "properties",
            F.from_json(
                "properties",
                properties_schema,
            ),
        )
        _transformed_df = transformed_df.select("csv.*", "properties.*")

        # _storage_handler = StorageHandler(
        #     storage_account_name=self.storage_account_name,
        #     container_name=self.container_name,
        #     tenant_id=self.tenant_id,
        #     client_id=self.client_id,
        #     client_secret=self.client_secret,
        # )

        # filesize_udf = F.udf(
        #     lambda file_name: _storage_handler.get_file_properties(file_name),
        #     file_properties_schema,
        # )

        # _transformed_df = transformed_df.withColumn(
        #     "fileproperties", filesize_udf(F.col("source_file"))
        # )
        # _transformed_df = _transformed_df.select(
        #     F.col("source_file"), F.col("count"), F.col("fileproperties.*")
        # )

        _query = (
            _transformed_df.writeStream.option(
                "checkpointLocation", self.checkpoint_file_path
            )
            .trigger(processingTime=f"{processing_time_in_seconds} seconds")
            .foreachBatch(self.process_data_batch)
            .queryName("process_msgdata")
            .start()
        )
        return _query


class Main:
    def __init__(
        self,
    ):
        args = self.parse_arguments(sys.argv[1:])

        self.args = args
        self.logger_extra = {THREAD_POOL_ID_NAME: "Main"}
        custom_logger = CustomLogger(
            logger_name=args.logger_name,
            log_level=args.log_level,
        )
        self.logger = custom_logger.get_logger()

    def parse_arguments(
        self,
        args,
    ):
        parser = argparse.ArgumentParser(description="Process arguments.")
        parser.add_argument(
            "--max-events-per-trigger",
            type=int,
            dest="max_events_per_trigger",
            help="Max events per trigger",
            required=True,
        )
        parser.add_argument(
            "--processing-time-in-seconds",
            type=int,
            dest="processing_time_in_seconds",
            help="Processing time in seconds",
            required=True,
        )
        parser.add_argument(
            "--keyvault-name",
            type=str,
            dest="keyvault_name",
            help="Key Vault name",
            required=True,
        )
        parser.add_argument(
            "--keyvault-linked-service-name",
            type=str,
            dest="keyvault_linked_service_name",
            help="Key Vault linked service name",
            required=True,
        )
        parser.add_argument(
            "--clear-input",
            dest="clear_input",
            action="store_true",
            help="Clear input folder",
            required=False,
        )
        parser.add_argument(
            "--clear-output",
            dest="clear_output",
            action="store_true",
            help="Clear output folder",
            required=False,
        )
        parser.add_argument(
            "--clear-checkpoint",
            dest="clear_checkpoint",
            action="store_true",
            help="Clear checkpoint folder",
            required=False,
        )
        parser.add_argument(
            "--output-path",
            type=str,
            dest="output_path",
            help="Output path",
            required=True,
        )
        parser.add_argument(
            "--checkpoint-path",
            type=str,
            dest="checkpoint_path",
            help="Checkpoint path",
            required=True,
        )
        parser.add_argument(
            "--logger-name",
            type=str,
            dest="logger_name",
            default="com.contoso.DataIngestion",
            help="Logger name",
            required=False,
        )
        parser.add_argument(
            "--log-level",
            type=str,
            dest="log_level",
            default="INFO",
            help="Log level",
            required=False,
        )
        parser.add_argument(
            "--partitionby",
            type=str,
            dest="partitionby",
            help="Partition by this column",
            required=True,
        )
        parser.add_argument(
            "--consumer-group",
            type=str,
            dest="consumer_group",
            help="Consumer group for event hub",
            required=True,
        )

        return parser.parse_args(args)

    def process_data(
        self,
    ):
        logger = self.logger
        args = self.args

        metadata_loader = LoadMetaDataSet(
            logger=logger,
            logger_extra=self.logger_extra,
            key_vault_name=args.keyvault_name,
            key_vault_linked_service_name=args.keyvault_linked_service_name,
            args=args,
        )

        logger.info(
            "Starting message processing",
        )

        logger.info(
            f"Max events per trigger is {args.max_events_per_trigger}",
        )
        logger.info(
            f"Processing time is {args.processing_time_in_seconds} seconds",
        )

        process_data_query = metadata_loader.process_events(
            max_events_per_trigger=args.max_events_per_trigger,
            processing_time_in_seconds=args.processing_time_in_seconds,
        )

        process_data_query.awaitTermination()

    def _prepare_for_testing(
        self,
    ):
        args = self.args
        if args.clear_output or args.clear_checkpoint:
            _clear_folders = ClearFolders(
                logger=self.logger,
                logger_extra=self.logger_extra,
                args=args,
                key_vault_name=args.keyvault_name,
                key_vault_linked_service_name=args.keyvault_linked_service_name,
            )
            _clear_folders.clear_folders()

    def run(self):
        self.process_data()


if __name__ == "__main__":
    _main = Main()
    _main._prepare_for_testing()
    _main.run()
