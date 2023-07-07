import os
import sys
import argparse
import logging
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import concurrent.futures

THREAD_POOL_ID_NAME = "threadPoolId"
FIRST_TIMESTAMP_COLUMN_NAME = "firstTimeStamp"
INPUT_FILE_COLUMN_NAME = "source_file"
CURRENT_PROCESSING_TIME_COLUMN_NAME = "processing_time"

LOGGING_FORMAT = (
    f"%(asctime)s - %(name)s - %(levelname)s - %({THREAD_POOL_ID_NAME})s - %(message)s"
)


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
            f"Spark master: {self.spark.sparkContext.master}",
            extra=self.logger_extra,
        )
        self.logger.info(
            f"Spark executor memory: {self.spark.sparkContext._conf.get('spark.executor.memory')}",
            extra=self.logger_extra,
        )
        self.logger.info(
            f"Spark executor cores: {self.spark.sparkContext._conf.get('spark.executor.cores')}",
            extra=self.logger_extra,
        )
        self.logger.info(
            f"Spark executor instances: {self.spark.sparkContext._conf.get('spark.executor.instances')}",
            extra=self.logger_extra,
        )
        self.logger.info(
            f"Spark driver memory: {self.spark.sparkContext._conf.get('spark.driver.memory')}",
            extra=self.logger_extra,
        )
        self.logger.info(
            f"Spark driver cores: {self.spark.sparkContext._conf.get('spark.driver.cores')}",
            extra=self.logger_extra,
        )
        self.logger.info(
            f"Spark driver maxResultSize: {self.spark.sparkContext._conf.get('spark.driver.maxResultSize')}",
            extra=self.logger_extra,
        )
        self.logger.info(
            f"Spark shuffle partitions: {self.spark.sparkContext._conf.get('spark.sql.shuffle.partitions')}",
            extra=self.logger_extra,
        )
        self.logger.info(
            f"Spark defaultParallelism: {self.spark.sparkContext.defaultParallelism}",
            extra=self.logger_extra,
        )
        self.logger.info(
            f"Spark default parallelism: {self.spark.sparkContext._conf.get('spark.default.parallelism')}",
            extra=self.logger_extra,
        )
        self.logger.info(
            f"Spark default minPartitions: {self.spark.sparkContext.defaultMinPartitions}",
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

        try:
            for _file_info in mssparkutils.fs.ls(folder_path):
                mssparkutils.fs.rm(_file_info.path, recurse=True)
        except Exception as e:
            self.logger.warning(
                f"Error in deleting {folder_path}: {e}",
                extra=self.logger_extra,
            )
        finally:
            mssparkutils.fs.mkdirs(folder_path)

    def clear_folders(
        self,
        input_path,
    ):
        args = self.args
        logger = self.logger
        storage_account_name = self.get_secret("storage-account-name")
        container_name = self.get_secret("container-name")
        output_path = args.output_path
        input_path = input_path
        checkpoint_path = f"{args.checkpoint_path}/{input_path}"

        container_path = (
            f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
        )
        input_file_path = f"{container_path}/{input_path}"
        output_file_path = f"{container_path}/{output_path}"
        checkpoint_file_path = f"{container_path}/{checkpoint_path}"

        if self.args.clear_input:
            logger.info(
                f"Clearing input folder {input_file_path}", extra=self.logger_extra
            )
            self.clear_folder(input_file_path)
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


class LoadDataSet(Sparker):
    def __init__(
        self,
        logger,
        logger_extra,
        key_vault_name,
        key_vault_linked_service_name,
        args,
        input_path,
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
        eventhub_connection_string = self.get_secret("eventhub-connection-string")
        output_path = args.output_path
        archive_path = args.archive_path
        checkpoint_path = f"{args.checkpoint_path}/{input_path}"
        partitionby = args.partitionby
        first_timestamp_column_name = args.first_timestamp_column_name

        self.partitionby = args.partitionby
        container_path = (
            f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
        )
        self.input_file_path = f"{container_path}/{input_path}"
        self.output_file_path = f"{container_path}/{output_path}"
        self.archive_file_path = f"{container_path}/{archive_path}"
        self.checkpoint_file_path_data = f"{container_path}/{checkpoint_path}"
        self.first_timestamp_column_name = first_timestamp_column_name
        self.input_data_schema = self.get_input_data_schema()
        self.eh_conf = {
            "eventhubs.connectionString": self.spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
                eventhub_connection_string,
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
            f"Input path is {self.input_file_path}",
            extra=logger_extra,
        )
        logger.info(
            f"Output path is {self.output_file_path}",
            extra=logger_extra,
        )
        logger.info(
            f"Archive path is {self.archive_file_path}",
            extra=logger_extra,
        )
        logger.info(
            f"Checkpoint path is {self.checkpoint_file_path_data}",
            extra=logger_extra,
        )
        logger.info(
            f"Partition by is {partitionby}",
            extra=logger_extra,
        )
        logger.info(
            f"First timestamp column name is {first_timestamp_column_name}",
            extra=logger_extra,
        )
        logger.info(
            f"Clean up delay for cleanSource archive is {args.cleanup_delay}",
            extra=logger_extra,
        )
        logger.info(
            f"Number of threads for cleanSource archive is {args.num_threads_for_cleanup}",
            extra=logger_extra,
        )
        logger.info(
            f"Is CSV header present? {args.csv_header_is_present}",
            extra=logger_extra,
        )

    def get_input_data_schema(
        self,
    ):
        input_data_schema = (
            StructType()
            .add("name", "string")
            .add("age", "integer")
            .add("first_time_buyer", "boolean")
            .add("ltv", "float")
            .add("loan_amount", "float")
            .add("loan_term", "integer")
            .add("interest_rate", "float")
            .add("loan_purpose", "string")
            .add("loan_type", "string")
            .add("loan_id", "integer")
        )
        return input_data_schema

    def process_files(
        self,
        max_files_per_trigger,  # not used
        processing_time_in_seconds,
    ):
        options = {
            "header": self.args.csv_header_is_present,
        }
        if max_files_per_trigger > 0:
            options["maxFilesPerTrigger"] = max_files_per_trigger

        df = (
            self.spark.readStream.options(**options)
            .schema(self.input_data_schema)
            .csv(self.input_file_path)
        )

        # Get the input file name and the processing time
        transformed_df = df.select(
            "*",
            F.input_file_name().alias(INPUT_FILE_COLUMN_NAME),
            F.current_timestamp().alias(CURRENT_PROCESSING_TIME_COLUMN_NAME),
        )

        # Add the thread pool id (Optional: useful for debugging)
        transformed_df = transformed_df.withColumn(
            THREAD_POOL_ID_NAME,
            F.lit(self.logger_extra[THREAD_POOL_ID_NAME]),
        )

        query = (
            transformed_df.writeStream.option(
                "checkpointLocation", self.checkpoint_file_path_data
            )
            .option("cleanSource", "archive")
            .option("sourceArchiveDir", self.archive_file_path)
            .option(
                "spark.sql.streaming.fileSource.log.cleanupDelay",
                self.args.cleanup_delay,
            )
            .option(
                "spark.sql.streaming.fileSource.cleaner.numThreads",
                self.args.num_threads_for_cleanup,
            )
            .trigger(processingTime=f"{processing_time_in_seconds} seconds")
            .format("delta")
            .queryName(f"process_data_{self.logger_extra[THREAD_POOL_ID_NAME]}")
            .partitionBy(self.partitionby)
            .start(f"{self.output_file_path}")
        )
        return query


class Main:
    def __init__(
        self,
    ):
        args = self.parse_arguments(sys.argv[1:])

        self.args = args
        self.input_paths = args.input_paths.split(",")
        self.max_workers = len(self.input_paths)
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
            "--max-files-per-trigger",
            type=int,
            dest="max_files_per_trigger",
            help="Max files per trigger",
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
            "--input-paths",
            type=str,
            dest="input_paths",
            help="Comma separated input paths",
            required=True,
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
            "--first-timestamp-column-name",
            type=str,
            dest="first_timestamp_column_name",
            help="First timestamp column name",
            required=True,
        )
        parser.add_argument(
            "--archive-path",
            type=str,
            dest="archive_path",
            help="Archive path",
            required=True,
        )
        parser.add_argument(
            "--cleanup-delay",
            type=int,
            dest="cleanup_delay",
            help="Cleanup delay",
            default=3600,
            required=False,
        )
        parser.add_argument(
            "--num-threads-for-cleanup",
            type=int,
            dest="num_threads_for_cleanup",
            help="Number of threads for cleanup",
            default=5,
            required=False,
        )
        parser.add_argument(
            "--csv-header-is-present",
            dest="csv_header_is_present",
            action="store_true",
            help="Is csv header present",
            required=False,
        )

        return parser.parse_args(args)

    def process_input_path(
        self,
        input_path,
    ):
        logger = self.logger
        args = self.args
        # Note here we are using the index to create a unique thread pool id
        # This will help us identify which thread is processing which input path
        _logger_extra = {THREAD_POOL_ID_NAME: f"{input_path}"}

        logger.info(f"Start processing input path: {input_path}", extra=_logger_extra)

        data_loader = LoadDataSet(
            logger=logger,
            logger_extra=_logger_extra,
            key_vault_name=args.keyvault_name,
            key_vault_linked_service_name=args.keyvault_linked_service_name,
            args=args,
            input_path=input_path,
        )

        logger.info(
            "Starting Data Ingestion",
            extra=_logger_extra,
        )

        logger.info(
            f"Max files per trigger is {args.max_files_per_trigger}",
            extra=_logger_extra,
        )
        logger.info(
            f"Processing time is {args.processing_time_in_seconds} seconds",
            extra=_logger_extra,
        )

        process_data_query = data_loader.process_files(
            max_files_per_trigger=args.max_files_per_trigger,
            processing_time_in_seconds=args.processing_time_in_seconds,
        )

        process_data_query.awaitTermination()

    def _prepare_for_testing(
        self,
    ):
        args = self.args
        if args.clear_input or args.clear_output or args.clear_checkpoint:
            _clear_folders = ClearFolders(
                logger=self.logger,
                logger_extra=self.logger_extra,
                args=args,
                key_vault_name=args.keyvault_name,
                key_vault_linked_service_name=args.keyvault_linked_service_name,
            )
            for input_path in self.input_paths:
                _clear_folders.clear_folders(input_path)

    def run(self):
        _logger = self.logger
        _logger_extra = self.logger_extra
        _max_workers = self.max_workers
        _input_paths = self.input_paths

        _logger.info(
            f"Number of workers is {_max_workers}",
            extra=_logger_extra,
        )

        # with concurrent.futures.ThreadPoolExecutor(
        #     max_workers=_max_workers,
        # ) as executor:
        #     the_futures = [
        #         executor.submit(
        #             self.process_input_path,
        #             input_path,
        #         )
        #         for input_path in _input_paths
        #     ]
        #     concurrent.futures.wait(the_futures)
        self.process_input_path(_input_paths[0])


if __name__ == "__main__":
    _main = Main()
    _main._prepare_for_testing()
    _main.run()
