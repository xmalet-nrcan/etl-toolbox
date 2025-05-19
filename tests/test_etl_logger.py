# tests/test_etl_logger.py

import logging
import os
import pathlib
import shutil

import tempfile

from nrcan_etl_toolbox.etl_logging.etl_logger import CustomLogger

TMP_DIR = pathlib.Path(tempfile.TemporaryDirectory().name)

def test_default_logger_initialization():
    logger = CustomLogger(name="test_logger", file_path=TMP_DIR)
    assert logger.name == "test_logger"
    assert logger._logger_type == "default"
    assert isinstance(logger.formatter, logging.Formatter)


def test_logger_basic_initialization():
    logger = CustomLogger(name="basic_logger", file_path=TMP_DIR)
    assert logger.name == "basic_logger"
    assert logger._logger_type == "default"
    assert logger.level == logging.NOTSET
    assert logger.formatter._fmt == "%(asctime)s :: [%(name)-10s :: %(levelname)-10s]  %(module)-8s :: %(message)s"


def test_logger_with_custom_type():
    logger = CustomLogger(name="custom_logger", file_path=TMP_DIR, logger_type="simple")
    assert logger.name == "custom_logger"
    assert logger._logger_type == "simple"
    assert isinstance(logger.formatter, logging.Formatter)


def test_logger_file_path_creation():
    logger_file_name = 'test.log'
    logger_name = "file_logger"
    composed_logger_file_name = f"{logger_name}_{logger_file_name}"
    tmp_file_dir = pathlib.Path(TMP_DIR) / "test_logger_file_path_creation"


    logger = CustomLogger(name="file_logger",
                          file_path=tmp_file_dir,
                          logger_file_name=logger_file_name)
    assert logger._file_path == os.path.join(tmp_file_dir, composed_logger_file_name)
    assert tmp_file_dir.exists()
    assert  tmp_file_dir / composed_logger_file_name in tmp_file_dir.iterdir()
    del logger
    shutil.rmtree(tmp_file_dir, ignore_errors=True)
