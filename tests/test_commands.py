import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from unittest.mock import patch
from src.models.analytics import AnalyticsBase, DimFilm, DimActor, DimCustomer, FactRental, FactPayment, SyncState
from src.config import get_mysql_engine

@pytest.fixture()
def test_engine(tmp_path):
    db_path = tmp_path / "test_analytics.db"
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    AnalyticsBase.metadata.create_all(engine)
    yield engine
    engine.dispose()

def make_patch(engine):
    return patch.multiple(
        "src.config",
        get_sqlite_engine=lambda: engine,
        get_sqlite_session=lambda: sessionmaker(bind=engine)()
    )

def test_init(test_engine):
    inspector = inspect(test_engine)
    tables = inspector.get_table_names()
    assert "dim_film" in tables
    assert "dim_actor" in tables
    assert "dim_customer" in tables
    assert "dim_store" in tables
    assert "dim_category" in tables
    assert "dim_date" in tables
    assert "fact_rental" in tables
    assert "fact_payment" in tables
    assert "sync_state" in tables
    print("Test 1 passed: Init creates all tables successfully")

def test_full_load(test_engine):
    from src.sync import full_load
    with patch.object(full_load, "get_sqlite_engine", lambda: test_engine):
        from src.sync.full_load import run_full_load
        run_full_load()
    session = sessionmaker(bind=test_engine)()
    assert session.query(DimFilm).count() == 1000
    assert session.query(DimActor).count() == 200
    assert session.query(DimCustomer).count() == 599
    assert session.query(FactRental).count() > 0
    assert session.query(FactPayment).count() > 0
    session.close()
    print("Test 2 passed: Full load loads all data successfully")

def test_incremental_new_data(test_engine):
    from src.sync import full_load, incremental
    with patch.object(full_load, "get_sqlite_engine", lambda: test_engine):
        full_load.run_full_load()
    session = sessionmaker(bind=test_engine)()
    for table in ['film', 'actor', 'customer', 'rental', 'payment']:
        state = session.query(SyncState).filter_by(table_name=table).first()
        if state:
            state.last_updated = datetime(2000, 1, 1)
    session.commit()
    session.close()
    with patch.object(incremental, "get_sqlite_engine", lambda: test_engine):
        incremental.run_incremental()
    session = sessionmaker(bind=test_engine)()
    assert session.query(DimFilm).count() == 1000
    session.close()
    print("Test 3 passed: Incremental handles new data correctly")

def test_incremental_updates(test_engine):
    from src.sync import full_load, incremental
    with patch.object(full_load, "get_sqlite_engine", lambda: test_engine):
        full_load.run_full_load()
    session = sessionmaker(bind=test_engine)()
    film = session.query(DimFilm).first()
    film_id = film.film_id
    original_title = film.title
    film.title = "UPDATED TITLE"
    state = session.query(SyncState).filter_by(table_name='film').first()
    if state:
        state.last_updated = datetime(2000, 1, 1)
    session.commit()
    session.close()
    with patch.object(incremental, "get_sqlite_engine", lambda: test_engine):
        incremental.run_incremental()
    session = sessionmaker(bind=test_engine)()
    updated = session.query(DimFilm).filter_by(film_id=film_id).first()
    assert updated.title == original_title
    session.close()
    print("Test 4 passed: Incremental updates existing records correctly")

def test_validate(test_engine):
    from src.sync import full_load
    with patch.object(full_load, "get_sqlite_engine", lambda: test_engine):
        full_load.run_full_load()
    mysql_engine = get_mysql_engine()
    mysql_session = sessionmaker(bind=mysql_engine)()
    from src.models.sakila import Film, Customer
    mysql_film_count = mysql_session.query(Film).count()
    mysql_customer_count = mysql_session.query(Customer).count()
    mysql_session.close()
    sqlite_session = sessionmaker(bind=test_engine)()
    assert mysql_film_count == sqlite_session.query(DimFilm).count()
    assert mysql_customer_count == sqlite_session.query(DimCustomer).count()
    sqlite_session.close()
    print("Test 5 passed: Validate confirms data consistency")