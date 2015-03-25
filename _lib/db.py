import sys
import time
import re
import os
from contextlib import contextmanager

import logbook

import click

from .bootstrapping import requires_env

_SQLITE_RE = re.compile(r"^sqlite:///(.*)$")


@click.group()
def db():
    pass


def _create_sqlite():
    from flask_app.models import db
    from flask_app.app import app
    uri = app.config['SQLALCHEMY_DATABASE_URI']
    path = _SQLITE_RE.search(uri).group(1)

    if os.path.exists(path):
        logbook.info("{} exists. Not doing anything", path)

    db.create_all()
    logbook.info("successfully created the tables")


def _create_postgres():
    import sqlalchemy
    from flask_app.app import app
    from flask_app.models import db

    uri = app.config['SQLALCHEMY_DATABASE_URI']
    try:
        sqlalchemy.create_engine(uri).connect()
    except sqlalchemy.exc.OperationalError:
        uri, db_name = uri.rsplit('/', 1)
        engine = sqlalchemy.create_engine(uri + '/postgres')
        conn = engine.connect()
        conn.execute("commit")
        conn.execute("create database {} with encoding = 'UTF8'".format(db_name))
        conn.close()
        logbook.info("Database {} successfully created on {}.", db_name, uri)
        db.create_all()
        logbook.info("successfully created the tables")
    else:
        logbook.info("Database exists. Not doing anything.")


@db.command()
@requires_env("app")
def ensure():
    from flask_app.app import app

    uri = app.config['SQLALCHEMY_DATABASE_URI']
    if 'postgres' in uri or 'psycopg2' in uri:
        return _create_postgres()
    elif 'sqlite' in uri:
        return _create_sqlite()
    else:
        logbook.error("Don't know how to create a database of type {}", url)
        sys.exit(-1)


@db.command()
def wait(num_retries=60, retry_sleep_seconds=1):
    import sqlalchemy

    from flask_app.app import app

    uri = app.config['SQLALCHEMY_DATABASE_URI']
    for retry in xrange(num_retries):
        logbook.info("Testing database connection... (retry {0}/{1})", retry+1, num_retries)
        if retry > 0:
            time.sleep(retry_sleep_seconds)
        try:
            sqlalchemy.create_engine(uri).connect()
        except sqlalchemy.exc.OperationalError as e:
            if 'does not exist' in str(e):
                break
            logbook.error("Ignoring OperationError {0} (db still not availalbe?)", e)
        except Exception as e:
            logbook.error("Could not connect to database ({0.__class__}: {0}. Going to retry...", e, exc_info=True)
        else:
            break
    else:
        raise RuntimeError("Could not connect to database")
    logbook.info("Database connection successful")

@db.command()
@requires_env("app")
def drop():
    from flask_app.app import app
    from flask_app.models import db
    db.drop_all()
    db.engine.execute('DROP TABLE IF EXISTS alembic_version')


@db.command()
@requires_env("app")
def revision():
    with _migrate_context() as migrate:
        migrate.upgrade()
        migrate.revision(autogenerate=True)


@db.command()
@requires_env("app")
def upgrade():
    with _migrate_context() as migrate:
        migrate.upgrade()


@contextmanager
def _migrate_context():
    from flask_app.app import app
    from flask_app.models import db
    from flask.ext import migrate

    migrate.Migrate(app, db)

    with app.app_context():
        yield migrate
