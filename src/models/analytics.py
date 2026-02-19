from sqlalchemy import Column, Integer, String, DECIMAL, DateTime, Date, Float, Index
from sqlalchemy.orm import declarative_base

AnalyticsBase = declarative_base()

class DimDate(AnalyticsBase):
    __tablename__ = 'dim_date'
    date_key = Column(Integer, primary_key=True)
    date = Column(Date)
    year = Column(Integer)
    quarter = Column(Integer)
    month = Column(Integer)
    day_of_month = Column(Integer)
    day_of_week = Column(Integer)
    is_weekend = Column(Integer)

class DimFilm(AnalyticsBase):
    __tablename__ = 'dim_film'
    film_key = Column(Integer, primary_key=True, autoincrement=True)
    film_id = Column(Integer, unique=True)
    title = Column(String)
    rating = Column(String)
    length = Column(Integer)
    language = Column(String)
    release_year = Column(String)
    last_update = Column(DateTime)

class DimActor(AnalyticsBase):
    __tablename__ = 'dim_actor'
    actor_key = Column(Integer, primary_key=True, autoincrement=True)
    actor_id = Column(Integer, unique=True)
    first_name = Column(String)
    last_name = Column(String)
    last_update = Column(DateTime)

class DimCategory(AnalyticsBase):
    __tablename__ = 'dim_category'
    category_key = Column(Integer, primary_key=True, autoincrement=True)
    category_id = Column(Integer, unique=True)
    name = Column(String)
    last_update = Column(DateTime)

class DimStore(AnalyticsBase):
    __tablename__ = 'dim_store'
    store_key = Column(Integer, primary_key=True, autoincrement=True)
    store_id = Column(Integer, unique=True)
    city = Column(String)
    country = Column(String)
    last_update = Column(DateTime)

class DimCustomer(AnalyticsBase):
    __tablename__ = 'dim_customer'
    customer_key = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, unique=True)
    first_name = Column(String)
    last_name = Column(String)
    active = Column(Integer)
    city = Column(String)
    country = Column(String)
    last_update = Column(DateTime)

class BridgeFilmActor(AnalyticsBase):
    __tablename__ = 'bridge_film_actor'
    film_key = Column(Integer, primary_key=True)
    actor_key = Column(Integer, primary_key=True)

class BridgeFilmCategory(AnalyticsBase):
    __tablename__ = 'bridge_film_category'
    film_key = Column(Integer, primary_key=True)
    category_key = Column(Integer, primary_key=True)

class FactRental(AnalyticsBase):
    __tablename__ = 'fact_rental'
    fact_rental_key = Column(Integer, primary_key=True, autoincrement=True)
    rental_id = Column(Integer, unique=True)
    date_key_rented = Column(Integer)
    date_key_returned = Column(Integer)
    film_key = Column(Integer)
    store_key = Column(Integer)
    customer_key = Column(Integer)
    staff_id = Column(Integer)
    rental_duration_days = Column(Integer)

class FactPayment(AnalyticsBase):
    __tablename__ = 'fact_payment'
    fact_payment_key = Column(Integer, primary_key=True, autoincrement=True)
    payment_id = Column(Integer, unique=True)
    date_key_paid = Column(Integer)
    customer_key = Column(Integer)
    store_key = Column(Integer)
    staff_id = Column(Integer)
    amount = Column(DECIMAL(5, 2))

class SyncState(AnalyticsBase):
    __tablename__ = 'sync_state'
    table_name = Column(String, primary_key=True)
    last_updated = Column(DateTime)

Index('idx_fact_rental_date', FactRental.date_key_rented)
Index('idx_fact_rental_customer', FactRental.customer_key)
Index('idx_fact_rental_film', FactRental.film_key)
Index('idx_fact_payment_date', FactPayment.date_key_paid)
Index('idx_fact_payment_customer', FactPayment.customer_key)
Index('idx_dim_film_id', DimFilm.film_id)
Index('idx_dim_customer_id', DimCustomer.customer_id)
Index('idx_dim_actor_id', DimActor.actor_id)