from sqlalchemy.orm import sessionmaker
from src.config import get_mysql_engine, get_sqlite_engine
from src.models.sakila import Film, Actor, Category, Language, Store, Customer, Inventory, Rental, Payment, Address, City, Country
from src.models.analytics import DimFilm, DimActor, DimCategory, DimStore, DimCustomer, BridgeFilmActor, BridgeFilmCategory, FactRental, FactPayment, SyncState
from datetime import datetime

def get_date_key(dt):
    if dt is None:
        return None
    return int(dt.strftime('%Y%m%d'))

def get_last_updated(sqlite_session, table_name):
    state = sqlite_session.query(SyncState).filter_by(table_name=table_name).first()
    return state.last_updated if state else datetime(2000, 1, 1)

def update_sync_state(sqlite_session, table_name, dt):
    state = sqlite_session.query(SyncState).filter_by(table_name=table_name).first()
    if state:
        state.last_updated = dt
    else:
        sqlite_session.add(SyncState(table_name=table_name, last_updated=dt))
    sqlite_session.commit()

def run_incremental():
    print("Starting incremental update...")

    mysql_session = sessionmaker(bind=get_mysql_engine())()
    sqlite_session = sessionmaker(bind=get_sqlite_engine())()
    now = datetime.now()

    try:
        languages = {l.language_id: l.name for l in mysql_session.query(Language).all()}
        addresses = {a.address_id: a for a in mysql_session.query(Address).all()}
        cities = {c.city_id: c for c in mysql_session.query(City).all()}
        countries = {c.country_id: c for c in mysql_session.query(Country).all()}
        inventories = {i.inventory_id: i for i in mysql_session.query(Inventory).all()}

        print("Updating dim_film...")
        last = get_last_updated(sqlite_session, 'film')
        for film in mysql_session.query(Film).filter(Film.last_update > last).all():
            existing = sqlite_session.query(DimFilm).filter_by(film_id=film.film_id).first()
            if existing:
                existing.title = film.title
                existing.rating = film.rating
                existing.length = film.length
                existing.language = languages.get(film.language_id, 'Unknown')
                existing.release_year = str(film.release_year)
                existing.last_update = film.last_update
            else:
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
        update_sync_state(sqlite_session, 'film', now)
        print("dim_film done")

        print("Updating dim_actor...")
        last = get_last_updated(sqlite_session, 'actor')
        for actor in mysql_session.query(Actor).filter(Actor.last_update > last).all():
            existing = sqlite_session.query(DimActor).filter_by(actor_id=actor.actor_id).first()
            if existing:
                existing.first_name = actor.first_name
                existing.last_name = actor.last_name
                existing.last_update = actor.last_update
            else:
                sqlite_session.add(DimActor(
                    actor_id=actor.actor_id,
                    first_name=actor.first_name,
                    last_name=actor.last_name,
                    last_update=actor.last_update
                ))
        sqlite_session.commit()
        update_sync_state(sqlite_session, 'actor', now)
        print("dim_actor done")

        print("Updating dim_customer...")
        last = get_last_updated(sqlite_session, 'customer')
        for customer in mysql_session.query(Customer).filter(Customer.last_update > last).all():
            address = addresses.get(customer.address_id)
            city = cities.get(address.city_id) if address else None
            country = countries.get(city.country_id) if city else None
            existing = sqlite_session.query(DimCustomer).filter_by(customer_id=customer.customer_id).first()
            if existing:
                existing.first_name = customer.first_name
                existing.last_name = customer.last_name
                existing.active = customer.active
                existing.city = city.city if city else None
                existing.country = country.country if country else None
                existing.last_update = customer.last_update
            else:
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
        update_sync_state(sqlite_session, 'customer', now)
        print("dim_customer done")

        print("Updating fact_rental...")
        last = get_last_updated(sqlite_session, 'rental')
        film_map = {f.film_id: f.film_key for f in sqlite_session.query(DimFilm).all()}
        store_map = {s.store_id: s.store_key for s in sqlite_session.query(DimStore).all()}
        customer_map = {c.customer_id: c.customer_key for c in sqlite_session.query(DimCustomer).all()}
        for rental in mysql_session.query(Rental).filter(Rental.last_update > last).all():
            inventory = inventories.get(rental.inventory_id)
            fk = film_map.get(inventory.film_id) if inventory else None
            sk = store_map.get(inventory.store_id) if inventory else None
            ck = customer_map.get(rental.customer_id)
            duration = None
            if rental.return_date and rental.rental_date:
                duration = (rental.return_date - rental.rental_date).days
            existing = sqlite_session.query(FactRental).filter_by(rental_id=rental.rental_id).first()
            if existing:
                existing.date_key_rented = get_date_key(rental.rental_date)
                existing.date_key_returned = get_date_key(rental.return_date)
                existing.film_key = fk
                existing.store_key = sk
                existing.customer_key = ck
                existing.staff_id = rental.staff_id
                existing.rental_duration_days = duration
            else:
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
        update_sync_state(sqlite_session, 'rental', now)
        print("fact_rental done")

        print("Updating fact_payment...")
        last = get_last_updated(sqlite_session, 'payment')
        rental_store_map = {r.rental_id: inventories.get(r.inventory_id) for r in mysql_session.query(Rental).all()}
        for payment in mysql_session.query(Payment).filter(Payment.payment_date > last).all():
            ck = customer_map.get(payment.customer_id)
            inventory = rental_store_map.get(payment.rental_id)
            sk = store_map.get(inventory.store_id) if inventory else None
            existing = sqlite_session.query(FactPayment).filter_by(payment_id=payment.payment_id).first()
            if existing:
                existing.date_key_paid = get_date_key(payment.payment_date)
                existing.customer_key = ck
                existing.store_key = sk
                existing.staff_id = payment.staff_id
                existing.amount = payment.amount
            else:
                sqlite_session.add(FactPayment(
                    payment_id=payment.payment_id,
                    date_key_paid=get_date_key(payment.payment_date),
                    customer_key=ck,
                    store_key=sk,
                    staff_id=payment.staff_id,
                    amount=payment.amount
                ))
        sqlite_session.commit()
        update_sync_state(sqlite_session, 'payment', now)
        print("fact_payment done")

        print("Incremental update complete!")

    except Exception as e:
        sqlite_session.rollback()
        print(f"Incremental update failed: {e}")
    finally:
        mysql_session.close()
        sqlite_session.close()