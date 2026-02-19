from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from src.config import get_mysql_engine, get_sqlite_engine
from src.models.sakila import Rental, Payment, Customer, Film, Staff, Inventory
from src.models.analytics import FactRental, FactPayment, DimCustomer, DimFilm, DimStore

def run_validate(days=30):
    print(f"Running validation (days parameter: {days})...")

    mysql_session = sessionmaker(bind=get_mysql_engine())()
    sqlite_session = sessionmaker(bind=get_sqlite_engine())()
    errors = 0

    try:
        mysql_rental_count = mysql_session.query(Rental).count()
        sqlite_rental_count = sqlite_session.query(FactRental).count()
        print(f"Rental count - MySQL: {mysql_rental_count}, SQLite: {sqlite_rental_count}")
        if mysql_rental_count != sqlite_rental_count:
            print("WARNING: Rental count mismatch!")
            errors += 1

        mysql_payment_total = float(mysql_session.query(func.sum(Payment.amount)).scalar() or 0)
        sqlite_payment_total = float(sqlite_session.query(func.sum(FactPayment.amount)).scalar() or 0)
        print(f"Payment total - MySQL: {mysql_payment_total:.2f}, SQLite: {sqlite_payment_total:.2f}")
        if abs(mysql_payment_total - sqlite_payment_total) > 1:
            print("WARNING: Payment total mismatch!")
            errors += 1

        mysql_customer_count = mysql_session.query(Customer).count()
        sqlite_customer_count = sqlite_session.query(DimCustomer).count()
        print(f"Customer count - MySQL: {mysql_customer_count}, SQLite: {sqlite_customer_count}")
        if mysql_customer_count != sqlite_customer_count:
            print("WARNING: Customer count mismatch!")
            errors += 1

        mysql_film_count = mysql_session.query(Film).count()
        sqlite_film_count = sqlite_session.query(DimFilm).count()
        print(f"Film count - MySQL: {mysql_film_count}, SQLite: {sqlite_film_count}")
        if mysql_film_count != sqlite_film_count:
            print("WARNING: Film count mismatch!")
            errors += 1

        if errors == 0:
            print("Validation passed! All checks OK.")
        else:
            print(f"Validation finished with {errors} warning(s).")

        print("Checking payment total by store...")
        store1_key = sqlite_session.query(DimStore.store_key).filter_by(store_id=1).scalar()
        store2_key = sqlite_session.query(DimStore.store_key).filter_by(store_id=2).scalar()

        mysql_store1 = float(mysql_session.query(func.sum(Payment.amount))
            .join(Rental, Payment.rental_id == Rental.rental_id)
            .join(Inventory, Rental.inventory_id == Inventory.inventory_id)
            .filter(Inventory.store_id == 1).scalar() or 0)
        mysql_store2 = float(mysql_session.query(func.sum(Payment.amount))
            .join(Rental, Payment.rental_id == Rental.rental_id)
            .join(Inventory, Rental.inventory_id == Inventory.inventory_id)
            .filter(Inventory.store_id == 2).scalar() or 0)

        sqlite_store1 = float(sqlite_session.query(func.sum(FactPayment.amount))
            .filter(FactPayment.store_key == store1_key).scalar() or 0)
        sqlite_store2 = float(sqlite_session.query(func.sum(FactPayment.amount))
            .filter(FactPayment.store_key == store2_key).scalar() or 0)

        print(f"Store 1 payment total - MySQL: {mysql_store1:.2f}, SQLite: {sqlite_store1:.2f}")
        print(f"Store 2 payment total - MySQL: {mysql_store2:.2f}, SQLite: {sqlite_store2:.2f}")

        if abs(mysql_store1 - sqlite_store1) > 1 or abs(mysql_store2 - sqlite_store2) > 1:
            print("WARNING: Payment total by store mismatch!")
        else:
            print("Store payment totals match!")

    except Exception as e:
        print(f"Validation failed: {e}")
    finally:
        mysql_session.close()
        sqlite_session.close()