import click
from src.sync.init_db import run_init
from src.sync.full_load import run_full_load
from src.sync.incremental import run_incremental
from src.sync.validate import run_validate

@click.group()
def cli():
    pass

@cli.command()
def init():
    run_init()

@cli.command()
def full_load():
    run_full_load()

@cli.command()
def incremental():
    run_incremental()

@cli.command()
@click.option('--days', default=30, help='Number of days to validate')
def validate(days):
    run_validate(days)

if __name__ == '__main__':
    cli()