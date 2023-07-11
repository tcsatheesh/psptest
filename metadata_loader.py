import os
import sys
import argparse
import logging
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, TimestampType
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient

INPUT_FILE_COLUMN_NAME = "source_file"
CURRENT_PROCESSING_TIME_COLUMN_NAME = "processing_time"
PROCESSED_DATE_COLUMN_NAME = "processed_date"
FILE_PROPERTIES_SIZE_COLUMN_NAME = "size"
FILE_PROPERTIES_CREATION_TIME_COLUMN_NAME = "creation_time"
RECORD_COUNT_COLUMN_NAME = "record_count"
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
        key_vault_name,
        key_vault_linked_service_name,
    ):
        self.logger = logger
        self.logger.info(
            f"Spark appName is {app_name}",
        )
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
        )
        self.logger.info(
            f"Spark master: {self.spark.sparkContext.master}",
        )
        self.logger.info(
            f"Spark executor memory: {self.spark.sparkContext._conf.get('spark.executor.memory')}",
        )
        self.logger.info(
            f"Spark executor cores: {self.spark.sparkContext._conf.get('spark.executor.cores')}",
        )
        self.logger.info(
            f"Spark executor instances: {self.spark.sparkContext._conf.get('spark.executor.instances')}",
        )
        self.logger.info(
            f"Spark driver memory: {self.spark.sparkContext._conf.get('spark.driver.memory')}",
        )
        self.logger.info(
            f"Spark driver cores: {self.spark.sparkContext._conf.get('spark.driver.cores')}",
        )
        self.logger.info(
            f"Spark driver maxResultSize: {self.spark.sparkContext._conf.get('spark.driver.maxResultSize')}",
        )
        self.logger.info(
            f"Spark shuffle partitions: {self.spark.sparkContext._conf.get('spark.sql.shuffle.partitions')}",
        )
        self.logger.info(
            f"Spark defaultParallelism: {self.spark.sparkContext.defaultParallelism}",
        )
        self.logger.info(
            f"Spark default parallelism: {self.spark.sparkContext._conf.get('spark.default.parallelism')}",
        )
        self.logger.info(
            f"Spark default minPartitions: {self.spark.sparkContext.defaultMinPartitions}",
        )

        self.logger.info(
            "Spark initialized",
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
        file_path,
    ):
        try:
            blob_client = self.get_blob_client(file_path)
            blob_properties = blob_client.get_blob_properties()
            return {
                FILE_PROPERTIES_SIZE_COLUMN_NAME: blob_properties.size,
                FILE_PROPERTIES_CREATION_TIME_COLUMN_NAME: blob_properties.creation_time,
            }
        except:
            return {
                FILE_PROPERTIES_SIZE_COLUMN_NAME: None,
                FILE_PROPERTIES_CREATION_TIME_COLUMN_NAME: None,
            }


class LoadDataSet(Sparker):
    def __init__(
        self,
        logger,
        args,
    ):
        super().__init__(
            app_name="LoadMetaData",
            logger=logger,
            key_vault_name=args.keyvault_name,
            key_vault_linked_service_name=args.keyvault_linked_service_name,
        )
        self.args = args
        self.storage_account_name = self.get_secret("storage-account-name")
        self.container_name = self.get_secret("container-name")

        self.tenant_id = self.get_secret("tenant-id")
        self.client_id = self.get_secret("client-id")
        self.client_secret = self.get_secret("client-secret")

        output_path = args.output_path
        archive_path = args.archive_path
        input_path = args.input_path
        checkpoint_path = args.checkpoint_path
        partitionby = args.partitionby

        self.partitionby = args.partitionby
        container_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net"
        self.input_file_path = f"{container_path}/{input_path}"
        self.output_file_path = f"{container_path}/{output_path}"
        self.archive_file_path = f"{container_path}/{archive_path}"
        self.checkpoint_file_path_data = f"{container_path}/{checkpoint_path}"
        self.input_data_schema = self.get_input_data_schema()

        logger.info(
            f"Storage account name is {self.storage_account_name}",
        )
        logger.info(
            f"Container name is {self.container_name}",
        )
        logger.info(
            f"Input path is {self.input_file_path}",
        )
        logger.info(
            f"Output path is {self.output_file_path}",
        )
        logger.info(
            f"Archive path is {self.archive_file_path}",
        )
        logger.info(
            f"Checkpoint path is {self.checkpoint_file_path_data}",
        )
        logger.info(
            f"Partition by is {partitionby}",
        )
        logger.info(
            f"Clean up delay for cleanSource archive is {args.cleanup_delay}",
        )
        logger.info(
            f"Number of threads for cleanSource archive is {args.num_threads_for_cleanup}",
        )
        logger.info(
            f"Is CSV header present? {args.csv_header_is_present}",
        )
        logger.info(
            f"Max files per trigger is {args.max_files_per_trigger}",
        )
        logger.info(
            f"Processing time is {args.processing_time_in_seconds} seconds",
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

    def get_transformations(
        self,
        df,
    ):
        _df = df.withColumn(
            PROCESSED_DATE_COLUMN_NAME,
            F.date_format(
                F.col(CURRENT_PROCESSING_TIME_COLUMN_NAME),
                "yyyy-MM-dd",
            ),
        )
        return _df

    def process_metadata(
        self,
        bdf,
        epoch_id,
    ):
        bdf.persist()

        _SOURCE_FILE_NAME_ALIAS = "source_file_name"
        _window = Window.partitionBy(INPUT_FILE_COLUMN_NAME).orderBy(
            F.col(CURRENT_PROCESSING_TIME_COLUMN_NAME).desc()
        )
        _bdf = (
            bdf.select(
                INPUT_FILE_COLUMN_NAME,
                CURRENT_PROCESSING_TIME_COLUMN_NAME,
                self.args.first_column_name,
            )
            .withColumn("row", F.row_number().over(_window))
            .filter(F.col("row") == 1)
            .drop("row")
            .select(
                F.col(INPUT_FILE_COLUMN_NAME).alias(_SOURCE_FILE_NAME_ALIAS),
                self.args.first_column_name,
            )
        )

        _count_per_file = (
            bdf.groupBy(
                [
                    INPUT_FILE_COLUMN_NAME,
                    CURRENT_PROCESSING_TIME_COLUMN_NAME,
                    PROCESSED_DATE_COLUMN_NAME,
                ]
            )
            .count()
            .withColumnRenamed(
                "count",
                RECORD_COUNT_COLUMN_NAME,
            )
        )

        file_properties_schema = (
            StructType()
            .add(FILE_PROPERTIES_SIZE_COLUMN_NAME, IntegerType())
            .add(FILE_PROPERTIES_CREATION_TIME_COLUMN_NAME, TimestampType())
        )

        _storage_handler = StorageHandler(
            storage_account_name=self.storage_account_name,
            container_name=self.container_name,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        filesize_udf = F.udf(
            lambda file_path: _storage_handler.get_file_properties(
                file_path,
            ),
            file_properties_schema,
        )

        _transformed_df = _count_per_file.withColumn(
            "fileproperties", filesize_udf(F.col(INPUT_FILE_COLUMN_NAME))
        )
        _transformed_df = _transformed_df.select(
            F.col(INPUT_FILE_COLUMN_NAME),
            F.col(RECORD_COUNT_COLUMN_NAME),
            F.col("fileproperties.*"),
            F.col(CURRENT_PROCESSING_TIME_COLUMN_NAME),
            F.col(PROCESSED_DATE_COLUMN_NAME),
        )

        _joined_bdf = _transformed_df.join(
            _bdf,
            _bdf[_SOURCE_FILE_NAME_ALIAS] == _count_per_file[INPUT_FILE_COLUMN_NAME],
            "inner",
        ).drop(_SOURCE_FILE_NAME_ALIAS)

        _joined_bdf.write.format("delta").partitionBy(self.partitionby).mode(
            "append"
        ).save(f"{self.output_file_path}")
        bdf.unpersist()

    def process_files(
        self,
    ):
        options = {
            "header": self.args.csv_header_is_present,
        }
        if self.args.max_files_per_trigger > 0:
            options["maxFilesPerTrigger"] = self.args.max_files_per_trigger

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
        transformed_df = self.get_transformations(
            transformed_df,
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
            .trigger(processingTime=f"{self.args.processing_time_in_seconds} seconds")
            .queryName(f"process_metadata")
            .foreachBatch(self.process_metadata)
            .start()
        )
        return query


class Main:
    def __init__(
        self,
    ):
        args = self.parse_arguments(sys.argv[1:])

        self.args = args
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
            "--input-path",
            type=str,
            dest="input_path",
            help="Input path for the data",
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
        parser.add_argument(
            "--first-column-name",
            type=str,
            dest="first_column_name",
            help="First column name",
            required=True,
        )

        return parser.parse_args(args)

    def run(
        self,
    ):
        logger = self.logger
        args = self.args

        data_loader = LoadDataSet(
            logger=logger,
            args=args,
        )

        logger.info(
            "Starting Metadata Ingestion",
        )

        process_data_query = data_loader.process_files()

        process_data_query.awaitTermination()


if __name__ == "__main__":
    _main = Main()
    _main.run()
