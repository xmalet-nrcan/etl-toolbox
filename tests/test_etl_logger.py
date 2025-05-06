# tests/test_etl_logger.py

import logging
import os
import pathlib
import shutil

from nrcan_etl_toolbox.etl_logging.etl_logger import CustomLogger


def test_default_logger_initialization():
    logger = CustomLogger(name="test_logger")
    assert logger.name == "test_logger"
    assert logger._logger_type == "default"
    assert isinstance(logger.formatter, logging.Formatter)


def test_logger_basic_initialization():
    logger = CustomLogger(name="basic_logger")
    assert logger.name == "basic_logger"
    assert logger._logger_type == "default"
    assert logger.level == logging.NOTSET
    assert logger.formatter._fmt == "%(asctime)s :: [%(name)-10s :: %(levelname)-10s]  %(module)-8s :: %(message)s"


def test_logger_with_custom_type():
    logger = CustomLogger(name="custom_logger", logger_type="simple")
    assert logger.name == "custom_logger"
    assert logger._logger_type == "simple"
    assert isinstance(logger.formatter, logging.Formatter)


def test_logger_file_path_creation():
    temp_dir = pathlib.Path("test_logs")
    logger = CustomLogger(name="file_logger", file_path=temp_dir, logger_file_name="test.log")
    assert logger._file_path == os.path.join(temp_dir, "file_logger_test.log")
    assert temp_dir.exists()
    del logger
    shutil.rmtree(temp_dir, ignore_errors=True)


