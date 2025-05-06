from __future__ import annotations

import unicodedata
from collections.abc import Callable
from typing import Any

from geoalchemy2 import WKBElement
from shapely.geometry.base import BaseGeometry
from sqlalchemy import String, Text, func, or_, orm
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import ColumnProperty, InstrumentedAttribute, Query
from sqlmodel import AutoString, SQLModel

from nrcan_etl_toolbox.etl_logging import CustomLogger

FONCTION_FILTER = "funcs"
ORDER_BY = "order_by"
LIMIT = "limit"
OFFSET = "offset"

logger = CustomLogger("SQLModels")


class Base(SQLModel):
    __abstract__ = True

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return False
        else:
            self_gen_cols = self._get_columns_if_not_auto_gen()
            other_gen_cols = other._get_columns_if_not_auto_gen()
            self_dict = {k: v for k, v in self.__dict__.items() if k in self_gen_cols}
            other_dict = {k: v for k, v in other.__dict__.items() if k in other_gen_cols}

            return self_dict == other_dict

    def __hash__(self):
        class_cols = self._get_columns_if_not_auto_gen()
        vals = [getattr(self, i) for i in class_cols if not isinstance(getattr(self, i), dict)]
        return hash(tuple(vals))

    @property
    def columns(self):
        return self._get_columns()

    @property
    def get_identity_columns(self):
        return [i for i in self._get_columns() if self.is_identity_column(i)]

    @property
    def relations(self):
        return self._get_relations()

    @classmethod
    def _get_relations(cls):
        relations = []
        for attr_name, attr_value in vars(cls).items():
            if isinstance(attr_value, InstrumentedAttribute) and isinstance(
                attr_value.property, orm.RelationshipProperty
            ):
                relations.append(attr_name)
        return relations

    @classmethod
    def _get_columns(cls) -> list:
        return [i for i in cls.model_fields]

    def _get_columns_if_not_auto_gen(self):
        out_cols = []

        for cols in self._get_columns():
            if not self._is_value_null(cols) and not self.is_identity_column(cols):
                if self._is_default_callable(cols):
                    if not self._is_value_equal_default_gen_col(cols):
                        out_cols.append(cols)
                else:
                    out_cols.append(cols)

        return out_cols

    def _is_default_value_null(self, column_name: str) -> bool:
        if hasattr(self, column_name):
            col_attribute = getattr(type(self), column_name).property.columns[0]
            return col_attribute.default is None
        return False

    def _is_value_null(self, column_name: str) -> bool:
        if hasattr(self, column_name):
            return getattr(self, column_name) is None
        else:
            return False

    def _is_value_equal_default_gen_col(self, column_name: str) -> bool:
        if hasattr(self, column_name):
            col_attribute = getattr(type(self), column_name).property.columns[0]
            if not self._is_default_value_null(column_name):
                col_value = getattr(self, column_name)
                is_callable = self._is_default_callable(column_name)
                if not is_callable:
                    return col_value == col_attribute.default.arg
                if is_callable:
                    fct = col_attribute.default.arg
                    try:
                        return fct() == col_value
                    except TypeError:
                        return fct(None) == col_value
                else:
                    return False
        return False

    def _is_default_callable(self, column_name: str):
        if hasattr(self, column_name) and not self._is_default_value_null(column_name):
            col_attribute = getattr(type(self), column_name).property.columns[0]
            return isinstance(col_attribute.default.arg, Callable)

    @classmethod
    def is_identity_column(cls, column_name: str) -> bool:
        """
        Checks if a specified column in an SQLAlchemy model is of type Identity.

        Parameters:
            column_name (str): The name of the column.

        Returns:
            bool: True if the column is an Identity column, False otherwise.
        """
        if column_name in cls.model_fields:
            column = getattr(cls, column_name)
            try:
                return column.property.columns[0].identity is not None
            except AttributeError:
                return False
        return False

    @classmethod
    def query_all_rows(cls, session):
        return cls.query_object(session=session, condition="all")

    @classmethod
    def get_query_for_object(
        cls,
        session,
        condition="or",
        **filters,
    ) -> Query:
        query = session.query(cls)
        sub_filters = []

        # Collect conditions dynamically
        for attr, value in filters.items():
            if value is not None and hasattr(cls, attr):
                # Add equality and "LIKE" conditions
                column_attr = getattr(cls, attr)
                if isinstance(column_attr, (InstrumentedAttribute, ColumnProperty)):
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
            try:
                compiled_query = query.statement.compile(
                    dialect=postgresql.dialect(),
                    compile_kwargs={"literal_binds": True},
                )
            except Exception:
                compiled_query = query.statement.compile(dialect=postgresql.dialect())
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
        except AttributeError as e:
            logger.error(e)
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
        if isinstance(input_string, list):
            input_string = "".join(input_string)
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


@compiles(WKBElement, "postgresql")
def compile_wkb(element, compiler, **kw):
    # TODO : replace with logging debug
    # print("COMPILE_WKB")
    wkt = element.desc  # à vérifier selon votre version/usage
    srid = element.srid if hasattr(element, "srid") else 4326  # valeur par défaut si nécessaire
    return f"ST_GeomFromText('{wkt}', {srid})"
