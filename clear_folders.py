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


class ClearFolders(Sparker):  # for debugging and performance testing only
    def __init__(
        self,
        args,
        logger,
        key_vault_name,
        key_vault_linked_service_name,
    ):
        super().__init__(
            app_name="ClearFolders",
            logger=logger,
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
                f"Clearing input folder {input_file_path}",
            )
            self.clear_folder(input_file_path)
        if self.args.clear_output:
            logger.info(
                f"Clearing output folder {output_file_path}",
            )
            self.clear_folder(output_file_path)
        if self.args.clear_checkpoint:
            logger.info(
                f"Clearing checkpoint folder {checkpoint_file_path}",
            )
            self.clear_folder(checkpoint_file_path)

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
            "--input-path",
            type=str,
            dest="input_path",
            help="Input path",
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
            "--archive-path",
            type=str,
            dest="archive_path",
            help="Archive path",
            required=True,
        )

        return parser.parse_args(args)

    def _prepare_for_testing(
        self,
    ):
        args = self.args
        if args.clear_input or args.clear_output or args.clear_checkpoint:
            _clear_folders = ClearFolders(
                logger=self.logger,
                args=args,
                key_vault_name=args.keyvault_name,
                key_vault_linked_service_name=args.keyvault_linked_service_name,
            )            
            _clear_folders.clear_folders(args.input_path)


if __name__ == "__main__":
    _main = Main()
    _main._prepare_for_testing()    
