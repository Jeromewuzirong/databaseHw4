from sqlalchemy.orm import sessionmaker
from src.config import get_mysql_engine, get_sqlite_engine
from src.models.sakila import Film, Actor, Category, FilmActor, FilmCategory, Language, Store, Customer, Inventory, Rental, Payment, Staff, Address, City, Country
from src.models.analytics import DimFilm, DimActor, DimCategory, DimStore, DimCustomer, BridgeFilmActor, BridgeFilmCategory, FactRental, FactPayment, SyncState
from datetime import datetime

def get_date_key(dt):
    if dt is None:
        return None
    return int(dt.strftime('%Y%m%d'))

def run_full_load():
    print("Starting full load...")

    mysql_session = sessionmaker(bind=get_mysql_engine())()
    sqlite_session = sessionmaker(bind=get_sqlite_engine())()

    try:
        print("Loading dim_film...")
        languages = {l.language_id: l.name for l in mysql_session.query(Language).all()}
        for film in mysql_session.query(Film).all():
            existing = sqlite_session.query(DimFilm).filter_by(film_id=film.film_id).first()
            if not existing:
                sqlite_session.add(DimFilm(
                    film_id=film.film_id,
                    title=film.title,
                    rating=film.rating,
                    length=film.length,
                    language=languages.get(film.language_id, 'Unknown'),
                    release_year=str(film.release_year),
                    last_update=film.last_update
                ))
        sqlite_session.commit()
        print("dim_film done")

        print("Loading dim_actor...")
        for actor in mysql_session.query(Actor).all():
            existing = sqlite_session.query(DimActor).filter_by(actor_id=actor.actor_id).first()
            if not existing:
                sqlite_session.add(DimActor(
                    actor_id=actor.actor_id,
                    first_name=actor.first_name,
                    last_name=actor.last_name,
                    last_update=actor.last_update
                ))
        sqlite_session.commit()
        print("dim_actor done")

        print("Loading dim_category...")
        for cat in mysql_session.query(Category).all():
            existing = sqlite_session.query(DimCategory).filter_by(category_id=cat.category_id).first()
            if not existing:
                sqlite_session.add(DimCategory(
                    category_id=cat.category_id,
                    name=cat.name,
                    last_update=cat.last_update
                ))
        sqlite_session.commit()
        print("dim_category done")

        print("Loading dim_store...")
        addresses = {a.address_id: a for a in mysql_session.query(Address).all()}
        cities = {c.city_id: c for c in mysql_session.query(City).all()}
        countries = {c.country_id: c for c in mysql_session.query(Country).all()}
        for store in mysql_session.query(Store).all():
            address = addresses.get(store.address_id)
            city = cities.get(address.city_id) if address else None
            country = countries.get(city.country_id) if city else None
            existing = sqlite_session.query(DimStore).filter_by(store_id=store.store_id).first()
            if not existing:
                sqlite_session.add(DimStore(
                    store_id=store.store_id,
                    city=city.city if city else None,
                    country=country.country if country else None,
                    last_update=store.last_update
                ))
        sqlite_session.commit()
        print("dim_store done")

        print("Loading dim_customer...")
        customers = mysql_session.query(Customer).all()
        for customer in customers:
            address = addresses.get(customer.address_id)
            city = cities.get(address.city_id) if address else None
            country = countries.get(city.country_id) if city else None
            existing = sqlite_session.query(DimCustomer).filter_by(customer_id=customer.customer_id).first()
            if not existing:
                sqlite_session.add(DimCustomer(
                    customer_id=customer.customer_id,
                    first_name=customer.first_name,
                    last_name=customer.last_name,
                    active=customer.active,
                    city=city.city if city else None,
                    country=country.country if country else None,
                    last_update=customer.last_update
                ))
        sqlite_session.commit()
        print("dim_customer done")

        print("Loading bridge_film_actor...")
        film_map = {f.film_id: f.film_key for f in sqlite_session.query(DimFilm).all()}
        actor_map = {a.actor_id: a.actor_key for a in sqlite_session.query(DimActor).all()}
        for fa in mysql_session.query(FilmActor).all():
            fk = film_map.get(fa.film_id)
            ak = actor_map.get(fa.actor_id)
            if fk and ak:
                existing = sqlite_session.query(BridgeFilmActor).filter_by(film_key=fk, actor_key=ak).first()
                if not existing:
                    sqlite_session.add(BridgeFilmActor(film_key=fk, actor_key=ak))
        sqlite_session.commit()
        print("bridge_film_actor done")

        print("Loading bridge_film_category...")
        category_map = {c.category_id: c.category_key for c in sqlite_session.query(DimCategory).all()}
        for fc in mysql_session.query(FilmCategory).all():
            fk = film_map.get(fc.film_id)
            ck = category_map.get(fc.category_id)
            if fk and ck:
                existing = sqlite_session.query(BridgeFilmCategory).filter_by(film_key=fk, category_key=ck).first()
                if not existing:
                    sqlite_session.add(BridgeFilmCategory(film_key=fk, category_key=ck))
        sqlite_session.commit()
        print("bridge_film_category done")

        print("Loading fact_rental...")
        inventories = {i.inventory_id: i for i in mysql_session.query(Inventory).all()}
        staffs = {s.staff_id: s for s in mysql_session.query(Staff).all()}
        customer_map = {c.customer_id: c.customer_key for c in sqlite_session.query(DimCustomer).all()}
        store_map = {s.store_id: s.store_key for s in sqlite_session.query(DimStore).all()}
        for rental in mysql_session.query(Rental).all():
            inventory = inventories.get(rental.inventory_id)
            fk = film_map.get(inventory.film_id) if inventory else None
            sk = store_map.get(inventory.store_id) if inventory else None
            ck = customer_map.get(rental.customer_id)
            duration = None
            if rental.return_date and rental.rental_date:
                duration = (rental.return_date - rental.rental_date).days
            existing = sqlite_session.query(FactRental).filter_by(rental_id=rental.rental_id).first()
            if not existing:
                sqlite_session.add(FactRental(
                    rental_id=rental.rental_id,
                    date_key_rented=get_date_key(rental.rental_date),
                    date_key_returned=get_date_key(rental.return_date),
                    film_key=fk,
                    store_key=sk,
                    customer_key=ck,
                    staff_id=rental.staff_id,
                    rental_duration_days=duration
                ))
        sqlite_session.commit()
        print("fact_rental done")

        print("Loading fact_payment...")
        rental_store_map = {r.rental_id: inventories.get(r.inventory_id) for r in mysql_session.query(Rental).all()}
        for payment in mysql_session.query(Payment).all():
            ck = customer_map.get(payment.customer_id)
            inventory = rental_store_map.get(payment.rental_id)
            sk = store_map.get(inventory.store_id) if inventory else None
            existing = sqlite_session.query(FactPayment).filter_by(payment_id=payment.payment_id).first()
            if not existing:
                sqlite_session.add(FactPayment(
                    payment_id=payment.payment_id,
                    date_key_paid=get_date_key(payment.payment_date),
                    customer_key=ck,
                    store_key=sk,
                    staff_id=payment.staff_id,
                    amount=payment.amount
                ))
        sqlite_session.commit()
        print("fact_payment done")

        now = datetime.now()
        for table in ['film', 'actor', 'category', 'store', 'customer', 'rental', 'payment']:
            state = sqlite_session.query(SyncState).filter_by(table_name=table).first()
            if state:
                state.last_updated = now
            else:
                sqlite_session.add(SyncState(table_name=table, last_updated=now))
        sqlite_session.commit()

        print("Full load complete!")

    except Exception as e:
        sqlite_session.rollback()
        print(f"Full load failed: {e}")
    finally:
        mysql_session.close()
        sqlite_session.close()