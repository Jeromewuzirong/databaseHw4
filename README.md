# ORM Data Sync: MySQL (Sakila) → SQLite
A data sync pipeline that replicates Sakila MySQL data into a SQLite analytics database using SQLAlchemy ORM.

## Prerequirements

- Python 3.8+
- MySQL with Sakila database installed

## Installation

1. Install dependencies:
```
pip install -r requirements.txt
```

2. Create a `.env` file in the project root:
```
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DB=sakila
SQLITE_PATH=analytics.db
```

## CLI Commands

**Init**
```
python cli.py init
```

**Full Load**
```
python cli.py full-load
```

**Incremental**
```
python cli.py incremental
```

**Validate**
```
python cli.py validate
```

## Running Tests
```
python -m pytest tests/test_commands.py -v
```

Citation: (https://claude.ai/share/13a6331b-1095-4cca-9093-b1912eb69604)