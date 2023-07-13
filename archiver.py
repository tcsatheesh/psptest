import os
import sys
import argparse
import logging
import traceback
from datetime import datetime
from zipfile import ZipFile

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, DateType

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient

INPUT_FILE_COLUMN_NAME = "source_file"
PROCESSED_DATE_COLUMN_NAME = "processed_date"
DATETIME_ARCHIVED_COLUMN_NAME = "archived_on"
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


class Archiver(Sparker):
    def __init__(self, args, logger):
        super().__init__(
            app_name="Archiver",
            logger=logger,
            key_vault_name=args.keyvault_name,
            key_vault_linked_service_name=args.keyvault_linked_service_name,
        )
        self.args = args
        self.logger = logger

        self.client_id = self.get_secret(
            secret_name="client-id",
        )
        self.client_secret = self.get_secret(
            secret_name="client-secret",
        )
        self.tenant_id = self.get_secret(
            secret_name="tenant-id",
        )
        self.storage_account_name = self.get_secret(
            secret_name="storage-account-name",
        )
        self.container_name = self.get_secret(
            secret_name="container-name",
        )

        self.logger.info(
            f"storage_account_name: {self.storage_account_name}",
        )
        self.logger.info(
            f"container_name: {self.container_name}",
        )

        self.tenant_id = self.get_secret("tenant-id")
        self.client_id = self.get_secret("client-id")
        self.client_secret = self.get_secret("client-secret")

        data_path = args.data_path
        metadata_path = args.metadata_path
        archive_queue_path = args.archive_queue_path

        container_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net"
        self.data_file_path = f"{container_path}/{data_path}"
        self.metadata_file_path = f"{container_path}/{metadata_path}"
        self.archive_queue_path = f"{container_path}/{archive_queue_path}"

    def archive_files(
        self,
    ):
        _archive_date = self.args.archive_date

        self.logger.info(
            f"archive_date: {_archive_date}",
        )

        _data_df = self.spark.read.format("delta").load(
            self.data_file_path,
        )
        _data_df = _data_df.filter(
            _data_df[PROCESSED_DATE_COLUMN_NAME] == _archive_date,
        )
        _data_df = _data_df.select(INPUT_FILE_COLUMN_NAME).distinct()

        _metadata_df = self.spark.read.format("delta").load(
            self.metadata_file_path,
        )
        _metadata_df = _metadata_df.filter(
            _metadata_df[PROCESSED_DATE_COLUMN_NAME] == _archive_date,
        )
        _metadata_df = _metadata_df.select(INPUT_FILE_COLUMN_NAME).distinct()

        _INPUT_FILE_COLUMN_NAME_ALIAS = f"{INPUT_FILE_COLUMN_NAME}_alias"
        _metadata_df = _metadata_df.withColumnRenamed(
            INPUT_FILE_COLUMN_NAME,
            _INPUT_FILE_COLUMN_NAME_ALIAS,
        )

        _joined_df = _data_df.join(
            _metadata_df,
            _data_df[INPUT_FILE_COLUMN_NAME]
            == _metadata_df[_INPUT_FILE_COLUMN_NAME_ALIAS],
            "inner",
        ).drop(_INPUT_FILE_COLUMN_NAME_ALIAS)

        _joined_df = _joined_df.withColumn(
            DATETIME_ARCHIVED_COLUMN_NAME,
            F.lit(None).cast(TimestampType()),
        )
        _joined_df = _joined_df.withColumn(
            PROCESSED_DATE_COLUMN_NAME,
            F.lit(_archive_date).cast(DateType()),
        )

        _joined_df = _joined_df.select(
            INPUT_FILE_COLUMN_NAME,
            PROCESSED_DATE_COLUMN_NAME,
            DATETIME_ARCHIVED_COLUMN_NAME,
        )

        _joined_df.write.format("delta").mode("overwrite").save(
            self.archive_queue_path,
        )


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
            "--metadata-path",
            type=str,
            dest="metadata_path",
            help="Metadata path",
            required=True,
        )
        parser.add_argument(
            "--data-path",
            type=str,
            dest="data_path",
            help="Data path",
            required=True,
        )
        parser.add_argument(
            "--logger-name",
            type=str,
            dest="logger_name",
            default="com.contoso.ArchiveFiles",
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
            "--archive-date",
            type=str,
            dest="archive_date",
            help="Archive date",
            required=True,
        )
        parser.add_argument(
            "--archive-queue-path",
            type=str,
            dest="archive_queue_path",
            help="Archive queue path",
            required=True,
        )

        return parser.parse_args(args)

    def run(
        self,
    ):
        logger = self.logger
        args = self.args

        _archiver = Archiver(
            logger=logger,
            args=args,
        )

        logger.info(
            "Start queueing files for archiving",
        )

        _archiver.archive_files()

        logger.info(
            "End of queueing files for archiving",
        )


if __name__ == "__main__":
    _main = Main()
    _main.run()
