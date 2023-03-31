from time import time

from psycopg2 import sql
from sqlalchemy import create_engine, Table, Column, MetaData, inspect, TEXT
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql.ddl import CreateSchema

sql.Identifier('column_name')
MATHESAR_PREFIX = "mathesar_"
TEMP_TABLE = f"{MATHESAR_PREFIX}temp_table_%s"
INFERENCE_SCHEMA = f"{MATHESAR_PREFIX}inference_schema"
TEMP_SCHEMA = INFERENCE_SCHEMA


def get_all_schemas(engine):
    inspector = inspect(engine)
    # We don't need to exclude system schemas (i.e., starting with "pg_")
    # since Inspector.get_schema_names already excludes them.  Thus, this
    # function actually gets all non-pg-reserved schemas.
    return inspector.get_schema_names()


def create_schema(schema, engine, comment=None):
    """
    This method creates a Postgres schema.
    """
    if schema not in get_all_schemas(engine):
        with engine.begin() as connection:
            connection.execute(CreateSchema(schema))


def create_table_from_json(engine, datafile, metadata=None):
    metadata = metadata if metadata else MetaData()
    temp_name = TEMP_TABLE % (int(time()))
    create_schema(TEMP_SCHEMA, engine)
    with engine.begin() as conn:
        while engine.dialect.has_table(conn, temp_name, schema=TEMP_SCHEMA):
            temp_name = TEMP_TABLE.format(int(time()))

    full_temp_name = f"{temp_name}"
    file_path = "./nb_json.json"

    with open(file_path, "r") as nbjson_file:
        with engine.begin() as conn:
            cursor = conn.connection.cursor()

            # Step 1
            temp_table = Table(
                full_temp_name, metadata,
                Column('data', JSONB),
                schema=TEMP_SCHEMA
            )
            metadata.create_all(engine)

            full_temp_name = sql.SQL(f"{TEMP_SCHEMA}.{temp_name}")

            # Step 2
            copy_sql = sql.SQL("COPY {full_temp_name} (data) FROM STDIN").format(
                full_temp_name=full_temp_name,
                file_path=file_path,
            )
            cursor.copy_expert(copy_sql, nbjson_file)

            # Step 3
            select_distinct_column_names_sql = sql.SQL(
                "SELECT DISTINCT jsonb_object_keys(data) as key from {full_temp_name} key").format(
                full_temp_name=full_temp_name)
            cursor.execute(select_distinct_column_names_sql)
            distinct_column_names_result = cursor.fetchall()
            distinct_column_names = []
            for column_name in distinct_column_names_result:
                distinct_column_names.append(Column(name=column_name[0], type_=TEXT))

            # Step 4
            table = Table(
                f"test{int(time())}",
                metadata,
                *distinct_column_names,
                schema=TEMP_SCHEMA
            )
            metadata.create_all(engine)

            # Step 5
            values_stmt = [f"data->>'{value[0]}'" for value in distinct_column_names_result]
            json_keys = sql.SQL(",".join(values_stmt))
            insert_stmt = sql.SQL("INSERT INTO {table_name}"
                                  " SELECT {json_keys} FROM {json_table}"
                                  ).format(
                table_name=sql.SQL(f"{TEMP_SCHEMA}.{table.name}"),
                json_keys=json_keys,
                json_table=full_temp_name
            )
            cursor.execute(insert_stmt)

            # Step 6
            drop_stmt = sql.SQL("DROP TABLE {temp_table}").format(
                temp_table=full_temp_name
            )
            cursor.execute(drop_stmt)


engine = create_engine("postgresql+psycopg2://poc:poc@localhost:5433/poc_db")
create_table_from_json(engine, None)
