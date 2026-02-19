from datetime import date, timedelta
from sqlalchemy import inspect
from src.config import get_sqlite_engine, get_mysql_engine
from src.models.analytics import AnalyticsBase, DimDate
from sqlalchemy.orm import sessionmaker

def run_init():
    print("Initializing database...")

    try:
        mysql_engine = get_mysql_engine()
        with mysql_engine.connect() as conn:
            print("MySQL connection successful")
    except Exception as e:
        print(f"MySQL connection failed: {e}")
        return

    try:
        sqlite_engine = get_sqlite_engine()
        AnalyticsBase.metadata.create_all(sqlite_engine)
        print("SQLite tables created successfully")
    except Exception as e:
        print(f"SQLite table creation failed: {e}")
        return

    try:
        Session = sessionmaker(bind=sqlite_engine)
        session = Session()

        existing = session.query(DimDate).first()
        if existing:
            print("dim_date already exists, skipping")
        else:
            print("Generating dim_date...")
            start = date(2000, 1, 1)
            end = date(2030, 12, 31)
            current = start
            batch = []
            while current <= end:
                date_key = int(current.strftime('%Y%m%d'))
                batch.append(DimDate(
                    date_key=date_key,
                    date=current,
                    year=current.year,
                    quarter=(current.month - 1) // 3 + 1,
                    month=current.month,
                    day_of_month=current.day,
                    day_of_week=current.weekday(),
                    is_weekend=1 if current.weekday() >= 5 else 0
                ))
                current += timedelta(days=1)
            session.bulk_save_objects(batch)
            session.commit()
            print(f"dim_date generated successfully, {len(batch)} records")
        session.close()
    except Exception as e:
        print(f"dim_date generation failed: {e}")
        return

    print("Initialization complete!")