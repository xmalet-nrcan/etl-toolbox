import re
import unicodedata
from collections import defaultdict
from typing import TypeVar

import sqlalchemy.engine
from dateutil import parser as date_parser
from psycopg2.errors import UniqueViolation
from sqlalchemy import BinaryExpression, create_engine, func
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.orm import InstrumentedAttribute, Session

from nrcan_etl_toolbox.database.orm import FONCTION_FILTER, LIMIT, ORDER_BY, Base
from nrcan_etl_toolbox.etl_logging import CustomLogger

T = TypeVar("T", bound="Base")


class AbstractDatabaseObjectsInterface:
    engine: sqlalchemy.engine.Engine = None
    session: sqlalchemy.orm.session.Session = None
    SessionLocal = None

    logger = CustomLogger("database_objects_handler", logger_type="default")

    @staticmethod
    def _is_date_valid(date_string: str) -> bool:
        """
        Validates if a date string is in a valid date format.

        Args:
            date_string: The string to validate

        Returns:
            bool: True if the date is valid, False otherwise
        """
        if not isinstance(date_string, str):
            return False
        try:
            # dateutil.parser can handle many date formats
            date_parser.parse(date_string)
            return True
        except (ValueError, TypeError):
            return False

    def __init__(self, database_url: str, db_objects_to_treat: list = None, logger_level="DEBUG"):
        if db_objects_to_treat is None:
            db_objects_to_treat = []
        self.logger.setLevel(logger_level)

        self._database_objects_to_treat = db_objects_to_treat
        # Ensure the database connection is initialized only once
        if AbstractDatabaseObjectsInterface.engine is None:  #
            self._connect_to_database(database_url)
        self.logger.debug(
            f"Connected to server {self.engine.url.host} "
            f"with database {self.engine.url.database} "
            f"(user : {self.engine.url.username})"
        )

        self._database_objects = defaultdict(list)
        for objects in self._database_objects_to_treat:
            self._database_objects[objects] = []

        self._max_number_of_retry = 10

    def clear_database_objects(self):
        for obj_type in self._database_objects_to_treat:
            self._database_objects[obj_type].clear()

    def _connect_to_database(self, database_url):
        """Connect to the database."""
        AbstractDatabaseObjectsInterface.engine = create_engine(database_url)
        AbstractDatabaseObjectsInterface.session = Session(
            bind=AbstractDatabaseObjectsInterface.engine, expire_on_commit=False
        )

        Base.metadata.create_all(AbstractDatabaseObjectsInterface.engine)
        self.logger.info("Connected to database")

    def _insert_object(self, db_object: Base) -> bool | None:
        with self.session as session:
            try:
                with session.begin(nested=True):
                    session.add(db_object)
                    session.commit()
                    return True
            except IntegrityError as e:
                session.rollback()
                if isinstance(e.orig, UniqueViolation):
                    constraint_match = re.search(r"unique « (.*?) »", str(e.args))
                    contraint_name = constraint_match.group(1) if constraint_match else None
                    if contraint_name and ("uni_" in contraint_name or "pk_" in contraint_name):
                        return True
                self.logger.error(f"IntegrityError - {db_object} \n{e}", stacklevel=3)
                return None
            except Exception as e:
                session.rollback()
                self.logger.error(f"Unexpected error - {db_object} \n{e}", stacklevel=3)
                raise

    def insert_object(self, db_object: Base):
        return self._insert_object(db_object)

    def insert_data(self):
        """Insert data into the database."""
        all_are_empty = all(len(self._database_objects[obj_type]) == 0 for obj_type in self._database_objects_to_treat)
        if all_are_empty:
            self.logger.debug("No data to insert")
            return
        for obj_type in self._database_objects_to_treat:
            objects = self._database_objects[obj_type].copy()
            for obj in objects:
                is_inserted = False
                try:
                    is_inserted = self._insert_object(obj)
                except UniqueViolation as v:
                    i = 0
                    self.logger.error(f"UniqueViolation - {obj} \n{v}")
                    self.logger.debug("Trying to re-insert object")
                    while i < self._max_number_of_retry:
                        self.logger.debug(f"Try nb : {i + 1}")
                        try:
                            is_inserted = self._insert_object(obj)
                        except UniqueViolation as v:
                            i = i + 1
                        else:
                            self.logger.debug(f"Successfully inserted {obj} after {i + 1} tries")
                            break
                except Exception as e:
                    self.logger.error(f"Unexpected error while inserting {obj_type}: {e}")
                    raise e
                else:
                    self.logger.debug(f"Successfully inserted {obj_type}")
                finally:
                    if is_inserted:
                        self._database_objects[obj_type].remove(obj)
                    else:
                        self.logger.error(f"Failed to insert {obj_type}")
                        raise Exception(f"Failed to insert {obj_type}")

                        # break #needed??

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
        prepared_string = "".join("_" if unicodedata.combining(char) else char for char in normalized_string)
        return prepared_string

    def _formatted_parameter(self, parameter: str) -> str:
        """Format parameter for SQL LIKE query."""
        parameter_norm = self.remove_accents_characters_from_string(parameter)

        return f"%{parameter_norm.lower()}%"

    def _is_like(self, col: InstrumentedAttribute, parameter: str = None) -> BinaryExpression | None:
        if parameter is not None:
            return func.lower(col).like(self._formatted_parameter(parameter))
        return None

    def _get_element_in_database(self, table_model: type[T], condition="or", **kwargs) -> list[T] | None:
        with self.session as session:
            try:
                session.begin(nested=True)
                data = table_model.query_object(session=session, condition=condition, **kwargs)
            except DataError:
                session.rollback()
                return None
            except Exception:
                session.rollback()
                return None
            finally:
                session.commit()
            return data

    def _get_or_create_element(
        self, dict_element: str, table_model: type[T], condition="and", **kwargs
    ) -> list[T] | None:
        try:
            self.session.begin(nested=True)
            data = self._get_element_in_database(table_model=table_model, condition=condition, **kwargs)
            if data is not None and len(data) == 0:
                data = self._get_element_to_be_inserted(dict_element=dict_element, table_model=table_model, **kwargs)
        except Exception as e:
            self.session.rollback()
            self.logger.warning(
                f"ON _get_or_create_element with \n{table_model}, {condition}, {kwargs} \nRAISED {e}", stacklevel=3
            )
        else:
            if data is not None:
                if len(data) == 0:
                    return [self._create_element(dict_element, table_model, **kwargs)]
                elif len(data) >= 1:
                    return data

                    # raise Exception(f"More than one {table_model.__name__} found with the same parameters")
            else:
                return data

    def _get_element_to_be_inserted(self, dict_element: str, table_model: type[T], **kwargs) -> list[T] | None:
        to_return = []
        in_dict_elements = {k: v for k, v in kwargs.items() if v is not None and not table_model.is_identity_column(k)}
        for elt in self._database_objects[dict_element]:
            is_to_return = True
            if isinstance(elt, table_model):
                for i in in_dict_elements:
                    if getattr(elt, i) != in_dict_elements[i]:
                        is_to_return = False
                        break
                if is_to_return:
                    to_return.append(elt)
        return to_return

    def _create_element(self, dict_element: str, table_model: type[T], **kwargs) -> T | None:
        t = table_model()
        for k, v in kwargs.items():
            if not t.is_identity_column(k):
                setattr(t, k, v)
        if t not in self._database_objects[dict_element]:
            self._database_objects[dict_element].append(t)
        return t

    def _associate_elements(self, elements: list[Base], associate_to: list[Base]):
        for elt in elements:
            if elt and elt not in associate_to:
                associate_to.append(elt)
                self.logger.debug(f"Associated {elt} to {associate_to}")

    def _get_similarity_func_and_order_by_for_column(
        self,
        column: str | list[str] | InstrumentedAttribute | list[InstrumentedAttribute],
        element: str,
        similarity_filter: float = 0.3,
        result_limit: int = 10,
    ) -> dict:
        """
        Generates a dictionary containing a similarity function, an order by
        clause, and a limit based on the given parameters to compute
        similarities for a specified column against a provided element.

        Args:
            column (str): The name of the column to compare against.
            element (str): The value to compute similarity for.
            similarity_filter (float, optional): Minimum similarity threshold.
                Defaults to 0.3.
            result_limit (int, optional): Maximum number of results to retrieve.
                Defaults to 10.

        Returns:
            dict:
                A dictionary containing three keys:
                  - `FONCTION_FILTER`: A list of boolean conditions applied on similarity metrics or an
                    empty dictionary if no valid similarity functions are created.
                  - `ORDER_BY`: The criterion for ordering results, such as descending order of the first
                    similarity function, or None if no similarity functions are available.
                  - `LIMIT`: The result limit constraint, by default set to result_limit.

        Raises:
            None
        """

        similarity_funcs = []
        if isinstance(column, (InstrumentedAttribute, str)):
            similarity_funcs.append(self._get_similarity_func(column, element))
        elif isinstance(column, list):
            for col in column:
                similarity_funcs.append(self._get_similarity_func(col, element))
        if len(similarity_funcs) == 0:
            return {FONCTION_FILTER: {}, ORDER_BY: None, LIMIT: result_limit}
        else:
            return {
                FONCTION_FILTER: [i > similarity_filter for i in similarity_funcs],
                ORDER_BY: sqlalchemy.desc(similarity_funcs[0]),
                LIMIT: result_limit,
            }

    @staticmethod
    def _get_similarity_func(
        in_col: str | InstrumentedAttribute,
        text_to_compare: str,
    ):
        return func.similarity(in_col, text_to_compare)

    @staticmethod
    def _get_bool_op_filter(in_col: InstrumentedAttribute, text_to_compare: str, operator: str):
        return in_col.bool_op(operator)(text_to_compare)

    def _get_similarity_bool_op(self, in_col: InstrumentedAttribute, text_to_compare: str):
        """
        Gets a boolean operation filter for similarity comparison of a given
        column value against a provided text.

        Parameters:
        in_col : InstrumentedAttribute
            The database column or ORM-mapped attribute to be compared.
        text_to_compare : str
            The string text to compare with the column value.
        """
        return self._get_bool_op_filter(in_col, text_to_compare, "%")

    def _get_word_similarity_bool_op(self, in_col: InstrumentedAttribute, text_to_compare: str):
        """
        Gets a boolean operation filter based on word similarity.

        This method constructs a filter for comparing the similarity of words
        between an input column and a text string. The comparison is performed
        using a pre-defined boolean operation that is specified for internal use.

        Args:
            in_col (InstrumentedAttribute): The database column containing the text
                data to compare against.
            text_to_compare (str): The string of text to compare for similarity
                with the content of the `in_col`.

        """
        return self._get_bool_op_filter(in_col, text_to_compare, "<%")
