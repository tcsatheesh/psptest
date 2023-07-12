import os
import sys
import argparse
import logging
import traceback
from datetime import datetime
from zipfile import ZipFile

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StringType, FloatType

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient

INPUT_FILE_COLUMN_NAME = "source_file"
PROCESSED_DATE_COLUMN_NAME = "processed_date"
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

    def download_data_file(
        self,
        source_file_path,
    ):
        _source_file_path = source_file_path
        blob_client = self.get_blob_client(_source_file_path)
        _file_name = os.path.basename(_source_file_path)
        print(f"_file_name: {_file_name}")
        with open(file=_file_name, mode="wb") as input_blob:
            download_stream = blob_client.download_blob()
            input_blob.write(download_stream.readall())
        return _file_name

    def delete_data_file(
        self,
        source_file_path,
    ):
        blob_client = self.get_blob_client(source_file_path)
        # not deleting the source as we need it for further testing
        # blob_client.delete_blob()
        # instead we are calling the get_blob_properties to simulate an additional call to the storage account
        blob_properties = blob_client.get_blob_properties()

    def create_zip_file(
        self,
        file_name,
    ):
        _zip_file_name = f"{file_name}.zip"
        print(f"_zip_file_name: {_zip_file_name}")

        with ZipFile(_zip_file_name, "w") as zip:
            zip.write(file_name)
        return _zip_file_name

    def upload_zip_file(
        self,
        zip_file_name,
        remote_file_path,
    ):
        blob_client = self.get_blob_client(remote_file_path)
        with open(file=zip_file_name, mode="rb") as data:
            blob_client.upload_blob(data=data, overwrite=True)
        os.remove(zip_file_name)

    def archive_file(
        self,
        source_file_path,
        archive_file_path,
    ):
        _result = {
            "input_file_path": source_file_path,
            "archive_file_path": archive_file_path,
            "download": None,
            "zip": None,
            "upload": None,
            "delete": None,
            "error": None,
        }
        try:
            _overall_start_time = datetime.utcnow()
            _source_file = source_file_path

            _start_time = datetime.utcnow()
            _file_name = self.download_data_file(
                source_file_path=_source_file,
            )
            _end_time = datetime.utcnow()
            _result["download"] = (_end_time - _start_time).total_seconds()

            _start_time = datetime.utcnow()
            _zip_file_name = self.create_zip_file(
                file_name=_file_name,
            )
            _end_time = datetime.utcnow()
            _result["zip"] = (_end_time - _start_time).total_seconds()
            _start_time = datetime.utcnow()
            _archive_file_path = f"{archive_file_path}/{_zip_file_name}"

            _result["archive_file_path"] = _archive_file_path

            self.upload_zip_file(
                zip_file_name=_zip_file_name,
                remote_file_path=_archive_file_path,
            )
            _end_time = datetime.utcnow()
            _result["upload"] = (_end_time - _start_time).total_seconds()

            _start_time = datetime.utcnow()
            self.delete_data_file(_source_file)
            _end_time = datetime.utcnow()
            _result["delete"] = (_end_time - _start_time).total_seconds()

            _result["total"] = (_end_time - _overall_start_time).total_seconds()
        except:
            _result["error"] = traceback.format_exc()

        return _result


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
        archive_path = args.archive_path

        container_path = f"abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net"
        self.data_file_path = f"{container_path}/{data_path}"
        self.metadata_file_path = f"{container_path}/{metadata_path}"
        self.archive_file_path = f"{container_path}/{archive_path}"
        self.timings_file_path = f"{container_path}/{args.timings_path}"

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

        self.logger.info(
            f"Archiving {_joined_df.count()} files",
        )

        _storage_handler = StorageHandler(
            storage_account_name=self.storage_account_name,
            container_name=self.container_name,
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
        )

        _timings_schema = (
            StructType()
            .add("input_file_path", StringType())
            .add("archive_file_path", StringType())
            .add("download", FloatType())
            .add("zip", FloatType())
            .add("upload", FloatType())
            .add("delete", FloatType())
            .add("total", FloatType())
            .add("error", StringType())
        )

        _archive_udf = F.udf(
            _storage_handler.archive_file,
            _timings_schema,
        )

        _timings_df = _joined_df.withColumn(
            "timings",
            _archive_udf(
                F.col(INPUT_FILE_COLUMN_NAME),
                F.lit(self.archive_file_path),
            ),
        )

        _timings_df = _timings_df.select(
            F.col("timings.*"),
        ).orderBy(
            F.col("input_file_path"),
        )

        _timings_df.write.format("delta").mode("overwrite").save(
            self.timings_file_path,
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
            "--archive-path",
            type=str,
            dest="archive_path",
            help="Archive path",
            required=True,
        )
        parser.add_argument(
            "--archive-date",
            type=str,
            dest="archive_date",
            help="Archive date",
            required=True,
        )
        parser.add_argument(
            "--timings-path",
            type=str,
            dest="timings_path",
            help="Timings path",
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
            "Starting archival of files",
        )

        _archiver.archive_files()


if __name__ == "__main__":
    _main = Main()
    _main.run()
