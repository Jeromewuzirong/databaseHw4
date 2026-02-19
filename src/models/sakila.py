from sqlalchemy import Column, Integer, String, Text, DECIMAL, DateTime, SmallInteger, ForeignKey, Enum, Date
from sqlalchemy.orm import declarative_base

SakilaBase = declarative_base()

class Language(SakilaBase):
    __tablename__ = 'language'
    language_id = Column(Integer, primary_key=True)
    name = Column(String(20))
    last_update = Column(DateTime)

class Film(SakilaBase):
    __tablename__ = 'film'
    film_id = Column(Integer, primary_key=True)
    title = Column(String(255))
    rating = Column(String(10))
    length = Column(SmallInteger)
    language_id = Column(Integer, ForeignKey('language.language_id'))
    release_year = Column(String(4))
    last_update = Column(DateTime)

class Actor(SakilaBase):
    __tablename__ = 'actor'
    actor_id = Column(Integer, primary_key=True)
    first_name = Column(String(45))
    last_name = Column(String(45))
    last_update = Column(DateTime)

class Category(SakilaBase):
    __tablename__ = 'category'
    category_id = Column(Integer, primary_key=True)
    name = Column(String(25))
    last_update = Column(DateTime)

class FilmActor(SakilaBase):
    __tablename__ = 'film_actor'
    actor_id = Column(Integer, ForeignKey('actor.actor_id'), primary_key=True)
    film_id = Column(Integer, ForeignKey('film.film_id'), primary_key=True)
    last_update = Column(DateTime)

class FilmCategory(SakilaBase):
    __tablename__ = 'film_category'
    film_id = Column(Integer, ForeignKey('film.film_id'), primary_key=True)
    category_id = Column(Integer, ForeignKey('category.category_id'), primary_key=True)
    last_update = Column(DateTime)

class Country(SakilaBase):
    __tablename__ = 'country'
    country_id = Column(Integer, primary_key=True)
    country = Column(String(50))
    last_update = Column(DateTime)

class City(SakilaBase):
    __tablename__ = 'city'
    city_id = Column(Integer, primary_key=True)
    city = Column(String(50))
    country_id = Column(Integer, ForeignKey('country.country_id'))
    last_update = Column(DateTime)

class Address(SakilaBase):
    __tablename__ = 'address'
    address_id = Column(Integer, primary_key=True)
    city_id = Column(Integer, ForeignKey('city.city_id'))
    last_update = Column(DateTime)

class Store(SakilaBase):
    __tablename__ = 'store'
    store_id = Column(Integer, primary_key=True)
    address_id = Column(Integer, ForeignKey('address.address_id'))
    last_update = Column(DateTime)

class Staff(SakilaBase):
    __tablename__ = 'staff'
    staff_id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('store.store_id'))
    last_update = Column(DateTime)

class Customer(SakilaBase):
    __tablename__ = 'customer'
    customer_id = Column(Integer, primary_key=True)
    first_name = Column(String(45))
    last_name = Column(String(45))
    active = Column(Integer)
    address_id = Column(Integer, ForeignKey('address.address_id'))
    last_update = Column(DateTime)

class Inventory(SakilaBase):
    __tablename__ = 'inventory'
    inventory_id = Column(Integer, primary_key=True)
    film_id = Column(Integer, ForeignKey('film.film_id'))
    store_id = Column(Integer, ForeignKey('store.store_id'))
    last_update = Column(DateTime)

class Rental(SakilaBase):
    __tablename__ = 'rental'
    rental_id = Column(Integer, primary_key=True)
    rental_date = Column(DateTime)
    inventory_id = Column(Integer, ForeignKey('inventory.inventory_id'))
    customer_id = Column(Integer, ForeignKey('customer.customer_id'))
    return_date = Column(DateTime)
    staff_id = Column(Integer, ForeignKey('staff.staff_id'))
    last_update = Column(DateTime)

class Payment(SakilaBase):
    __tablename__ = 'payment'
    payment_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('customer.customer_id'))
    staff_id = Column(Integer, ForeignKey('staff.staff_id'))
    rental_id = Column(Integer, ForeignKey('rental.rental_id'))
    amount = Column(DECIMAL(5, 2))
    payment_date = Column(DateTime)