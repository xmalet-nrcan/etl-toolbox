import datetime
import logging
import os
import pathlib
import tempfile
from typing import Union

import dotenv

dotenv.load_dotenv()

DEFAULT_FORMATTER = logging.Formatter(
    "%(asctime)s :: [%(name)-10s :: %(levelname)-10s]  %(module)-8s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

VERBOSE_FORMATER = logging.Formatter(
    "%(asctime)s :: [%(name)-10s :: %(levelname)-8s] %(module)-8s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

SIMPLE_FORMATTER = logging.Formatter("%(asctime)s :: %(levelname)-8s :: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

VERBOSE = bool(int(os.environ.get("VERBOSE_LOGGER", False)))

LOGGER_LEVEL = os.environ.get("LOGGER_LEVEL", logging.NOTSET)

_LOGGING_FORMATTERS = {"default": DEFAULT_FORMATTER, "verbose": VERBOSE_FORMATER, "simple": SIMPLE_FORMATTER}

_LOGGER_TYPE = {
    "default": {"is_verbose": False, "level": LOGGER_LEVEL, "formatter": _LOGGING_FORMATTERS["default"]},
    "verbose": {"is_verbose": True, "level": logging.DEBUG, "formatter": _LOGGING_FORMATTERS["verbose"]},
    "simple": {"is_verbose": False, "level": logging.INFO, "formatter": _LOGGING_FORMATTERS["simple"]},
    "custom": {
        "is_verbose": VERBOSE,
        "level": LOGGER_LEVEL,
        "formatter": _LOGGING_FORMATTERS["verbose"] if VERBOSE else _LOGGING_FORMATTERS["default"],
    },
}

PROGRESS_LEVELV_NUM = 60
logging.addLevelName(PROGRESS_LEVELV_NUM, "PROGRESS")


class CustomLogger(logging.Logger):
    """
    CustomLogger class extends the logging.Logger to provide enhanced logging functionalities.

    This class is designed to offer customizable logging capabilities, including support for
    different logger types, file-based logging, and verbosity control. It abstracts the setup
    of logging files, handlers, and formatting based on a predefined logger type configuration.
    The logger allows filtering of logs based on verbosity settings and provides additional
    features to log progress-specific messages.

    Logger types include 'default', 'verbose', 'simple', and 'custom' with configuration defined as :
    - 'default' : Default logger type with no verbosity control and logger level set to
    environ.get('LOGGER_LEVEL', logging.NOTSET) and default formatter.
    - 'verbose' : Logger type with verbose logging and logger level set to logging.DEBUG and verbose formatter.
    - 'simple' : Logger type with minimal logging and logger level set to logging.INFO and simple formatter.
    - 'custom' : Logger type with customizable verbosity and logger level.

    Logger setup can be configured using environment variables or by passing custom arguments.

    Available environment variables:
    - LOGGING_FILE_PATH : Specifies the directory path where logging files should be created.
    - VERBOSE_LOGGER: Specifies whether to enable verbose logging.
    - LOGGER_LEVEL: Specifies the logging level.
    - LOGGER_TYPE: Specifies the logger type.

    """

    def __init__(
        self,
        name: str,
        logger_type: str = "default",
        file_path: Union[str, pathlib.Path] = None,
        logger_file_name: str = "default.log",
        level=LOGGER_LEVEL,
    ):
        try:
            logging_level = _LOGGER_TYPE[logger_type]["level"]
        except KeyError:
            logging_level = level
        super().__init__(name, level=logging_level)
        self._logger_type = None
        self._verbose_logger_type: bool = None
        self.formatter: logging.Formatter = None
        self._file_path = None

        self._setup_logging_file_for_output(file_path, f"{name}_{logger_file_name}")
        self.set_logger_type(logger_type)

    def set_logger_type(self, logger_type: str):
        """
        Sets the type of logger for the current instance and configures the logger.

        This method allows the user to update the logger type according to the
        specified input and automatically configures the internal logger and its
        handlers.

        :param logger_type: Specifies the type of logger to set.
        :type logger_type: str
        """
        self._logger_type = logger_type
        self._set_logger_from_type()
        self._set_logger_handlers()

    def close(self):
        try:
            with open(self._file_path, "a") as f:
                f.write(f"{datetime.datetime.now()} :: ------------------ closing logger ------------------ \n")
        except FileNotFoundError:
            pass
        self.__del__()

    def __del__(self):
        for i in self.handlers:
            self.removeHandler(i)
        for i in self.filters:
            self.removeFilter(i)

    def _setup_logging_file_for_output(self, logging_file_path: str = None, file_name: str = None):
        """
        Sets up a logging file for output by preparing the necessary file path. It ensures
        the directory for the logging file exists or creates it if it does not. If no
        logging file path is provided, a default path is used based on an environment
        variable or the system's temporary directory.

        :param logging_file_path: The directory path where the logging file should be
            created. If not provided, it defaults to the value of the 'LOGGING_FILE_PATH'
            environment variable or the system's temporary directory.
        :type logging_file_path: str, optional
        :param file_name: The name of the logging file to be created.
        :type file_name: str, optional
        :return: None
        """

        if logging_file_path is None:
            output_path = os.getenv("LOGGING_FILE_PATH", tempfile.gettempdir())
        else:
            output_path = pathlib.Path(logging_file_path)
            output_path.mkdir(parents=True, exist_ok=True)
        self._file_path = os.path.join(output_path, file_name)

    def _set_logger_handlers(self):
        """Sets up the logger's handlers.'"""
        handler = logging.StreamHandler()
        handler.setFormatter(self.formatter)
        handler.addFilter(self._filter_logs)
        self.addHandler(handler)

        if self._file_path:
            self._add_file_handler(self._file_path)

    def _set_logger_from_type(self):
        """Sets the logger's level and formatter based on the logger type.'"""
        logger_params = _LOGGER_TYPE[self._logger_type]
        self._verbose_logger_type = bool(logger_params["is_verbose"])

        self.formatter = logger_params["formatter"]

    def _filter_logs(self, log: logging.LogRecord) -> bool:
        """if user sets verbose to false, only write logs that don't include 'PROGRESS'"""
        if log.msg is None:
            return True

        if not self._verbose_logger_type:
            return log.levelno != PROGRESS_LEVELV_NUM
        else:
            return True

    def _add_file_handler(self, file_path: str):
        """Adds a file handler to save logs to the specified file."""
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(self.formatter)
        self.addHandler(file_handler)
        file_handler.addFilter(self._filter_logs)

    def progress(self, msg: str, exc_info=None, stack_info=False, stacklevel=10, extra=None, *args, **kwargs):
        self.log(
            level=PROGRESS_LEVELV_NUM,
            msg=msg,
            exc_info=exc_info,
            stack_info=stack_info,
            stacklevel=stacklevel,
            extra=extra,
            *args,
            **kwargs,
        )

    def log(self, level, msg, exc_info=None, stack_info=False, stacklevel=10, extra=None, *args, **kwargs):
        super().log(level, msg, exc_info=exc_info, stack_info=stack_info, stacklevel=stacklevel, extra=extra, *args)
