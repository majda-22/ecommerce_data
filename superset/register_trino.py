import os

from superset import db
from superset.app import create_app


DATABASE_NAME = "ShopFlow Iceberg"
SQLALCHEMY_URI = os.getenv(
    "SUPERSET_TRINO_URI",
    "trino://admin@trino:8080/iceberg",
)


app = create_app()

with app.app_context():
    from superset.models.core import Database

    database = (
        db.session.query(Database)
        .filter(Database.database_name == DATABASE_NAME)
        .one_or_none()
    )

    if database is None:
        database = Database(database_name=DATABASE_NAME)
        db.session.add(database)

    database.sqlalchemy_uri = SQLALCHEMY_URI
    database.expose_in_sqllab = True
    database.allow_run_async = False
    database.allow_dml = False
    database.allow_file_upload = False
    database.allow_ctas = False
    database.allow_cvas = False
    database.allow_multi_schema_metadata_fetch = True

    db.session.commit()
    print(f"Registered Superset database: {DATABASE_NAME} -> {SQLALCHEMY_URI}")
