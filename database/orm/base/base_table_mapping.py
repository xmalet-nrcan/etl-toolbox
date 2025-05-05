from __future__ import annotations

import os
import unicodedata

from firescan_modules.utils.config.module_1_config import OUTPATH_PATHLIB
from firescan_modules.utils.logger import CustomLogger as Logger
from shapely.geometry.base import BaseGeometry
from sqlalchemy import String, Text, func, or_
from sqlalchemy.orm import ColumnProperty, InstrumentedAttribute, Query
from sqlmodel import AutoString, SQLModel

FONCTION_FILTER = "funcs"
ORDER_BY = "order_by"
LIMIT = "limit"
OFFSET = "offset"

logger = Logger(
    "SQLModels",
    logger_level=os.environ.get("LOG_LEVEL", "DEBUG"),
    file_path=OUTPATH_PATHLIB,
    logger_file_name="SQLModels.log",
)


class Base(SQLModel):
    __abstract__ = True

    @classmethod
    def is_identity_column(cls, column_name: str) -> bool:
        """
        Checks if a specified column in an SQLAlchemy model is of type Identity.

        Parameters:
            column_name (str): The name of the column.

        Returns:
            bool: True if the column is an Identity column, False otherwise.
        """
        if hasattr(cls, column_name):
            column = getattr(cls, column_name)
            return column.property.columns[0].identity is not None
        return False

    @classmethod
    def query_all_rows(cls, session):
        return cls.query_object(session=session, condition="all")

    @classmethod
    def get_query_for_object(
        cls,
        session,
        condition="or",
        funcs_conditions="and",
        **filters,
    ) -> Query:
        query = session.query(cls)
        sub_filters = []

        # Collect conditions dynamically
        for attr, value in filters.items():
            if value is not None and hasattr(cls, attr):
                # Add equality and "LIKE" conditions
                column_attr = getattr(cls, attr)
                if isinstance(column_attr, InstrumentedAttribute) and isinstance(column_attr.property, ColumnProperty):
                    if isinstance(value, list):
                        for v in value:
                            cls.add_value_to_sub_query(column_attr, sub_filters, v)
                    if isinstance(value, BaseGeometry):
                        pass
                    else:
                        cls.add_value_to_sub_query(column_attr, sub_filters, value)

        if FONCTION_FILTER in filters:
            sub_filters.extend(filters[FONCTION_FILTER])

        # Apply OR logic if there are conditions
        if sub_filters:
            if condition == "or":
                query = query.filter(or_(*sub_filters))
            if condition == "and":
                query = query.filter(*sub_filters)

        if query.whereclause is None and condition != "all":
            return
        else:
            if ORDER_BY in filters:
                query = query.order_by(filters[ORDER_BY])
            if LIMIT in filters and isinstance(filters[LIMIT], int):
                query = query.limit(filters[LIMIT])
            if OFFSET in filters and isinstance(filters[OFFSET], int):
                query = query.offset(filters[OFFSET])

            compiled_query = query.statement.compile(
                dialect=session.bind.dialect, compile_kwargs={"literal_binds": True}
            )
            logger.debug(f"{compiled_query}")
            return query

    @classmethod
    def query_object(
        cls,
        session,
        condition="or",
        funcs_conditions="and",
        **filters,
    ):
        try:
            return cls.get_query_for_object(
                session=session, condition=condition, funcs_conditions=funcs_conditions, **filters
            ).all()
        except AttributeError:
            return None

    @classmethod
    def execute_query(cls, session, query: Query):
        return query.all()

    @classmethod
    def add_value_to_sub_query(cls, column_attr, sub_filters, value):
        sub_filters.append(column_attr == value)
        if isinstance(column_attr.property.columns[0].type, (String, Text, AutoString)):
            sub_filters.append(cls._is_like(column_attr, value))

    @staticmethod
    def remove_accents_characters_from_string(input_string: str) -> str:
        """
        Prepares a string for SQL LIKE queries by replacing accented characters with underscores (_).

        Parameters:
            input_string (str): The string to prepare.

        Returns:
        str: The prepared string with accented characters replaced by underscores.
        """
        normalized_string = unicodedata.normalize("NFKD", input_string)
        prepared_string = ""

        for char in normalized_string:
            if unicodedata.combining(char):  # If the character is a combining mark (e.g., accent)
                prepared_string = prepared_string[:-1] + "_"
            else:
                prepared_string += char

        return unicodedata.normalize("NFC", prepared_string)

    @classmethod
    def _formatted_parameter(cls, parameter: str) -> str:
        """Format parameter for SQL LIKE query."""
        parameter_norm = cls.remove_accents_characters_from_string(parameter)
        if parameter_norm == "%":
            return "%"
        return f"%{parameter_norm.lower()}%"

    @classmethod
    def _is_like(self, col: InstrumentedAttribute, parameter: str = None):
        if parameter == "%":
            return
        if parameter is not None:
            return func.lower(col).like(self._formatted_parameter(parameter))

    @classmethod
    def get_default_value_from_column(cls, column_name):
        """
        Extracts the default value (Python-side or server-side) for a specified column in an SQLAlchemy model.

        Args:
            orm_class: The SQLAlchemy ORM class.
            column_name: The name of the column.

        Returns:
            str: The default value or server default value, or None if not defined.
        """
        if hasattr(cls, column_name):
            column = getattr(cls, column_name)
            # Check for Python-side default (default=...)
            if column.default is not None:
                arg = column.default.arg
                if callable(arg):  # Handle callable defaults
                    try:
                        return arg()  # Try calling without argument
                    except TypeError:
                        return arg(None)  # In case of TypeError, try calling with None

                return arg  # For non-callable defaults

            # Check for server-side default (server_default=...)
            if column.server_default is not None:
                return str(column.server_default.arg)  # Return the SQL expression as a string

            return None
