from contextlib import contextmanager
from typing import Optional, TypeVar

import sqlalchemy
from sqlalchemy import create_engine, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import InstrumentedAttribute, Session
from sqlmodel import SQLModel

from nrcan_etl_toolbox.database.orm import FONCTION_FILTER, LIMIT, ORDER_BY
from nrcan_etl_toolbox.etl_logging import CustomLogger
from dateutil import parser as date_parser

T = TypeVar("T", bound="Base")  # noqa: F821


def db_safe(func):
    """Décorateur pour logger les erreurs DB sans interrompre sans log clair."""

    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            self.logger.error(f"[{func.__name__}] Unexpected error: {e}", exc_info=True)
            raise

    return wrapper


class AbstractDatabaseObjectsInterface:
    def __init__(self, database_url: str, logger_level="DEBUG"):
        """
        Interface de base pour gérer les objets de base de données.

        Args:
            database_url (str): URL de connexion à la base (SQLAlchemy style).
            logger_level (str): Niveau de logging (ex: "DEBUG", "INFO").
        """
        self.engine = create_engine(database_url, echo=False, future=True)
        self.logger = CustomLogger("database_objects_handler", logger_type="default")

        self.logger.setLevel(logger_level)

    def insert_data(self):
        pass

    def clear_database_objects(self):
        pass

    # --------------------
    # Session manager
    # --------------------
    @contextmanager
    def get_session(self) -> Session:
        session = Session(self.engine, expire_on_commit=False, autoflush=True)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _get_merged_kwargs(self, session, **kwargs) -> dict:
        merged_kwargs = {}
        for k, v in kwargs.items():
            if isinstance(v, SQLModel):  # si c’est un objet ORM
                merged_kwargs[k] = session.merge(v)  # rattacher à la session
            else:
                merged_kwargs[k] = v
        return merged_kwargs

    # --------------------
    # CREATE (insert immédiat)
    # --------------------
    @db_safe
    def _create_element(self, table_model: type[T], **kwargs) -> Optional[T]:
        """
        Crée et insère immédiatement un nouvel élément en base.
        Si un doublon existe déjà (contrainte unique), on retourne l'objet existant.
        """
        with self.get_session() as session:
            merged_kwargs = self._get_merged_kwargs(session, **kwargs)

            t = table_model(**{k: v for k, v in merged_kwargs.items() if not table_model.is_identity_column(k)})
            try:
                session.add(t)
                session.flush()  # force l'INSERT
                session.refresh(t)  # récupère les PK/valeurs générées
                session.expunge(t)  # détache l'objet de la session
                self.logger.debug(f"Inserted {type(t).__name__} with PK={t.__dict__.get('id')}")
                return t

            except IntegrityError as e:
                self.logger.error(f"IntegrityError: {e}")
                session.rollback()
                self.logger.debug(f"{type(t).__name__} already exists, fetching existing one.")
                # récupérer l’existant via les colonnes uniques si dispo
                filter_kwargs = {
                    k: v
                    for k, v in kwargs.items()
                    if hasattr(table_model, "unique_keys") and k in table_model.unique_keys()
                }
                if not filter_kwargs:
                    filter_kwargs = kwargs  # fallback : utiliser tous les kwargs

                existing = self._get_element_in_database(table_model, **filter_kwargs)
                return existing[0] if existing else None

    # --------------------
    # READ
    # --------------------
    @db_safe
    def _get_element_in_database(self, table_model: type[T], condition: str = "and", **kwargs) -> list[T]:
        """Récupère des éléments de la DB selon condition."""
        with self.get_session() as session:
            merged_kwargs = self._get_merged_kwargs(session, **kwargs)
            data = table_model.query_object(session=session, condition=condition, **merged_kwargs)
            if not data or len(data) == 0:
                return None

            for obj in data:
                session.expunge(obj)
            return data

    # --------------------
    # GET OR CREATE
    # --------------------
    def _get_or_create_element(self, table_model: type[T], condition: str = "and", **kwargs) -> list[Optional[T]]:
        """
        Cherche un élément en base, sinon le crée immédiatement.
        """
        data = self._get_element_in_database(table_model, condition=condition, **kwargs)
        if data:
            return data
        return [self._create_element(table_model, **kwargs)]

    @staticmethod
    def _get_bool_op_filter(in_col: InstrumentedAttribute, text_to_compare: str, operator: str):
        """Helper pour appliquer un opérateur booléen PostgreSQL."""
        return in_col.bool_op(operator)(text_to_compare)

    def _get_similarity_bool_op(self, in_col: InstrumentedAttribute, text_to_compare: str):
        """Utilise l’opérateur '%' pour la similarité."""
        return self._get_bool_op_filter(in_col, text_to_compare, "%")

    def _get_word_similarity_bool_op(self, in_col: InstrumentedAttribute, text_to_compare: str):
        """Utilise l’opérateur '<%' pour la similarité par mots."""
        return self._get_bool_op_filter(in_col, text_to_compare, "<%")

    @staticmethod
    def _get_similarity_func(in_col: str | InstrumentedAttribute, text_to_compare: str):
        """Return SQL similarity function for given column and text."""
        return func.similarity(in_col, text_to_compare)

    @staticmethod
    def _get_similarity_func_and_order_by_for_column(column, element, similarity_filter=0.3, result_limit=10) -> dict:
        similarity_funcs = []
        if isinstance(column, (InstrumentedAttribute, str)):
            similarity_funcs.append(AbstractDatabaseObjectsInterface._get_similarity_func(column, element))
        elif isinstance(column, list):
            for col in column:
                similarity_funcs.append(AbstractDatabaseObjectsInterface._get_similarity_func(col, element))

        if not similarity_funcs:
            return {FONCTION_FILTER: {}, ORDER_BY: None, LIMIT: result_limit}

        return {
            FONCTION_FILTER: [i > similarity_filter for i in similarity_funcs],
            ORDER_BY: sqlalchemy.desc(similarity_funcs[0]),
            LIMIT: result_limit,
        }

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