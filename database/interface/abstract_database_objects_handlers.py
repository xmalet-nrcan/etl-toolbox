import unicodedata
from collections import defaultdict
from typing import Union

import sqlalchemy.engine
from firescan_modules.database_interface.orm import FONCTION_FILTER, LIMIT, ORDER_BY, Base
from firescan_modules.utils.config import logger as config_logger
from psycopg2.errors import UniqueViolation
from sqlalchemy import create_engine, func
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.orm import InstrumentedAttribute, sessionmaker


class AbstractDatabaseObjectsHandler:
    engine: sqlalchemy.engine.Engine = None
    session: sqlalchemy.orm.session.Session = None
    logger = config_logger

    def __init__(self, database_url: str, db_objects_to_treat: list = None, logger_level="DEBUG"):
        if db_objects_to_treat is None:
            db_objects_to_treat = []
        self.logger.setLevel(logger_level)

        self._database_objects_to_treat = db_objects_to_treat
        # Ensure the database connection is initialized only once
        if AbstractDatabaseObjectsHandler.engine is None or AbstractDatabaseObjectsHandler.session is None:
            self._connect_to_database(database_url)
            self.logger.debug(f"Instantiated new connection to server {self.engine.url.host} "
                              f"with database {self.engine.url.database} "
                              f"(user : {self.engine.url.username})")
        self.logger.debug(f"Connected to server {self.engine.url.host} "
                          f"with database {self.engine.url.database} "
                          f"(user : {self.engine.url.username})")

        self._database_objects = defaultdict(list)
        for objects in self._database_objects_to_treat:
            self._database_objects[objects] = []

        self._max_number_of_retry = 10

    def _connect_to_database(self, database_url):
        """Connect to the database."""
        AbstractDatabaseObjectsHandler.engine = create_engine(database_url)
        Session = sessionmaker(bind=AbstractDatabaseObjectsHandler.engine)
        AbstractDatabaseObjectsHandler.session = Session()
        Base.metadata.create_all(AbstractDatabaseObjectsHandler.engine)
        self.logger.info("Connected to database")

    def _insert_object(self, db_object: Base):
        try:
            # Ajout d'un seul objet à la session
            self.logger.debug(db_object)
            self.session.add(db_object)
            self.session.commit()
            return True
        except UniqueViolation as v:
            self.session.rollback()
            self.logger.error(f"UniqueViolation - {db_object} \n{v}", stacklevel=3)
            raise v

        except IntegrityError as e:
            self.session.rollback()
            if isinstance(e.orig, UniqueViolation):
                raise e.orig from e
            else:
                # Annule les modifications en cas d'erreur d'intégrité
                self.session.rollback()
                self.logger.error(f"IntegrityError - {db_object} \n{e}", stacklevel=3)


        except Exception as e:
            # Annule les modifications pour toute autre erreur
            self.session.rollback()
            raise e from e

    def insert_object(self, db_object: Base):
        return self._insert_object(db_object)

    def insert_data(self):
        """Insert data into the database."""
        all_are_empty = all(len(self._database_objects[obj_type]) == 0 for obj_type in self._database_objects_to_treat)
        if all_are_empty:
            self.logger.info("No data to insert")
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
                    self.logger.info("Trying to re-insert object")
                    while i < self._max_number_of_retry:
                        self.logger.info(f"Try nb : {i + 1}")
                        try:
                            is_inserted = self._insert_object(obj)
                        except UniqueViolation as v:
                            i = i + 1
                        else:
                            self.logger.info(f"Successfully inserted {obj} after {i + 1} tries")
                            break
                except Exception as e:
                    self.logger.error(f"Unexpected error while inserting {obj_type}: {e}")
                    raise e
                else:
                    self.logger.info(f"Successfully inserted {obj_type}")
                finally:
                    if is_inserted:
                        self._database_objects[obj_type].remove(obj)
                    else:
                        self.logger.error(f"Failed to insert {obj_type}")
                        raise Exception(f"Failed to insert {obj_type}")

                        #break #needed??


    @staticmethod
    def remove_accents_characters_from_string(input_string: str) -> str:
        """
        Prepares a string for SQL LIKE queries by replacing accented characters with underscores (_).

        Parameters:
            input_string (str): The string to prepare.

        Returns:
        str: The prepared string with accented characters replaced by underscores.
    """
        normalized_string = unicodedata.normalize('NFKD', input_string)
        prepared_string = ''.join('_' if unicodedata.combining(char) else char for char in normalized_string)
        return prepared_string

    def _formatted_parameter(self, parameter: str) -> str:
        """Format parameter for SQL LIKE query."""
        parameter_norm = self.remove_accents_characters_from_string(parameter)

        return f"%{parameter_norm.lower()}%"

    def _is_like(self, col: InstrumentedAttribute, parameter: str = None):
        if parameter is not None:
            return func.lower(col).like(self._formatted_parameter(parameter))

    def _get_element(self, table_model: type[Base], condition='or', **kwargs):
        try:
            data = table_model.query_object(session=self.session,
                                            condition=condition,
                                            **kwargs)
        except DataError:
            self.session.rollback()

            return None
        except Exception:
            self.session.rollback()
            return None

        return data

    def _get_or_create_element(self, dict_element: str, table_model: type[Base], condition='or', **kwargs):
        try:
            data = self._get_element(table_model=table_model, condition=condition,
                                     **kwargs)
        except Exception as e:
            self.session.rollback()
            self.logger.warning(f"ON _get_or_create_element with \n{table_model}, {condition}, {kwargs} \nRAISED {e}",
                                stacklevel=3)
        else:
            if len(data) == 0:
                return self._create_element(dict_element, table_model, **kwargs)
            elif len(data) == 1:
                return data[0]
            else:
                raise Exception(f"More than one {table_model.__name__} found with the same parameters")

    def _create_element(self, dict_element: str, table_model: type[Base], **kwargs):
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

    @staticmethod
    def _get_similarity_func_and_order_by_for_column(
            column: Union[str, list[str], InstrumentedAttribute, list[InstrumentedAttribute]],
            element: str
            , similarity_filter: float = 0.3,
            result_limit: int = 10):
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
                dict: A dictionary with the keys FONCTION_FILTER, ORDER_BY, and LIMIT.
                    The FONCTION_FILTER key maps to a list containing the similarity
                    threshold condition. The ORDER_BY key maps to a descending order
                    by similarity function. The LIMIT key maps to the maximum number
                    of results specified by result_limit.
        """

        def get_similarity_func_for_col(in_col: Union[str, InstrumentedAttribute],
                                        text_to_compare: str, ):
            similarity_func = func.similarity(in_col, text_to_compare)
            return similarity_func

        similarity_funcs = []
        if isinstance(column,  (str,InstrumentedAttribute)):
            similarity_funcs.append(get_similarity_func_for_col(column, element))
        elif isinstance(column, list):
            for col in column:
                similarity_funcs.append(get_similarity_func_for_col(col, element))
        if len(similarity_funcs) == 0:
            return {FONCTION_FILTER: {}, ORDER_BY: None, LIMIT: result_limit}
        else:
            return {FONCTION_FILTER: [i > similarity_filter for i in similarity_funcs],
                    ORDER_BY: sqlalchemy.desc(similarity_funcs[0]),
                    LIMIT: result_limit}
