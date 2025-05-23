import datetime
from typing import Optional

import pytest
from sqlalchemy import Column, DateTime, Identity, Integer, String
from sqlalchemy.orm import Session
from sqlmodel import Field, SQLModel, create_engine
from sqlmodel import Session as SQLModelSession

from nrcan_etl_toolbox.database.orm.base.base_table_mapping import Base

DEFAULT_STR_VALUE = "teste as default value"


@pytest.fixture
def test_session():
    # Using SQLite in-memory database for testing
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with SQLModelSession(engine) as session:
        yield session


class TestSampleModel(Base, table=True):
    __tablename__ = "test_sample_model"
    id: int = Field(primary_key=True)
    name: str


class TestSampleModel2(Base, table=True):
    __tablename__ = "test_sample_model2"
    id: int = Field(sa_column=Column(Integer, Identity(start=1, cycle=True), primary_key=True, autoincrement=True))
    name: str = Field(sa_column=Column(String))

    name_as_default: str = Field(sa_column=Column(String, default=DEFAULT_STR_VALUE))
    created_at: Optional[datetime.datetime] = Column(DateTime, default=datetime.date.today)  # exécuté en Python


def test_base_equality_method(test_session: Session):
    obj1 = TestSampleModel(id=1, name="Test")
    obj2 = TestSampleModel(id=1, name="Test")
    assert obj1 == obj2


def test_base_hash_method(test_session: Session):
    obj = TestSampleModel(id=1, name="Test")
    assert isinstance(hash(obj), int)


def test_is_identity_column_method():
    assert not TestSampleModel.is_identity_column("name")
    assert not TestSampleModel.is_identity_column("id"), (
        "Column ID from TestSampleModel should not be an identity column"
    )

    assert TestSampleModel2.is_identity_column("id"), "Column ID from TestSampleModel2 should be an identity column"


def test_get_column_defaults_not_callable():
    assert TestSampleModel2.get_default_values_for_columns(["name_as_default"]) == {
        "name_as_default": DEFAULT_STR_VALUE
    }
    assert TestSampleModel2.get_default_value_from_column("name_as_default") == DEFAULT_STR_VALUE


def test_get_column_defaults_callable():
    assert TestSampleModel2.get_default_value_from_column("created_at") == datetime.date.today()


def test_get_column_defaults_callable_with_default_value():
    assert TestSampleModel2.get_default_values_for_columns(["name_as_default", "created_at"]) == {
        "name_as_default": DEFAULT_STR_VALUE,
        "created_at": datetime.date.today(),
    }


def test_get_column_defaults_with_default_value_is_none():
    assert TestSampleModel2.get_default_value_from_column("name") is None


def test_remove_accents_characters_from_string():
    s_0 = "café"
    result = Base.remove_accents_characters_from_string(s_0)
    assert result == "caf_", "Accents only should be removed"

    s_1 = "é1àaâ¸çiìiîeêe"
    result = Base.remove_accents_characters_from_string(s_1)
    assert len(result) == len(s_1)
    assert result == "_1_a___i_i_e_e", "Accents should be removed"

    s_2 = "Çéûñâïœøßàëÿîðğšžłđæåẞŧňĥĵ"
    result = Base.remove_accents_characters_from_string(s_2)
    assert len(result) == len(s_2)
    assert result == "_" * len(s_2), "Accents should be removed"

    s_3 = "1234567890"
    result = Base.remove_accents_characters_from_string(s_3)
    assert len(result) == len(s_3)
    assert result == s_3, "Numbers should not be removed"

    s_4 = """!"/$%_)(*?&%?*&"/$"""
    result = Base.remove_accents_characters_from_string(s_4)
    assert len(result) == len(s_4)
    assert result == s_4, "Special characters should not be removed"
