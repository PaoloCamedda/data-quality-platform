import io
import re
import pandas as pd
from pyspark.sql import DataFrame
from src.config import logger


def sanitize_identifier(name: str) -> str:
    """Rende sicuro il nome di una tabella o colonna SQL."""
    clean = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
    if not clean or clean[0].isdigit():
        clean = f"tabella_{clean}"
    return clean.lower()


def export_to_csv(df: DataFrame) -> str:
    return df.toPandas().to_csv(index=False)


def export_to_json(df: DataFrame) -> str:
    return df.toPandas().to_json(orient="records", indent=2, date_format="iso")


def export_to_parquet_bytes(df: DataFrame) -> bytes:
    pdf = df.toPandas()
    buffer = io.BytesIO()
    pdf.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    return buffer.getvalue()


def generate_sql_dump(df: DataFrame, raw_table_name: str) -> str:
    """Genera uno script SQL completo di CREATE TABLE con tipi inferiti e INSERT INTO."""
    table_name = sanitize_identifier(raw_table_name)
    logger.info(f"Generazione SQL Dump per la tabella '{table_name}'...")

    sql_type_map = {
        "int": "INT",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "tinyint": "TINYINT",
        "double": "DOUBLE PRECISION",
        "float": "FLOAT",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "string": "VARCHAR(255)",
    }

    col_definitions = []
    col_types = {}
    for field in df.schema.fields:
        col_name = sanitize_identifier(field.name)
        spark_dtype = field.dataType.simpleString()
        sql_dtype = sql_type_map.get(spark_dtype, "VARCHAR(255)")
        col_types[field.name] = spark_dtype
        col_definitions.append(f"    {col_name} {sql_dtype}")

    ddl = f"-- SQL Dump generato da CSV Data Quality Platform\n"
    ddl += f"CREATE TABLE {table_name} (\n" + ",\n".join(col_definitions) + "\n);\n\n"

    pdf = df.toPandas()
    insert_statements = []

    for _, row in pdf.iterrows():
        formatted_values = []
        for col in df.columns:
            val = row[col]
            dtype = col_types[col]
            if pd.isna(val):
                formatted_values.append("NULL")
            elif dtype in ("int", "bigint", "smallint", "tinyint", "double", "float"):
                formatted_values.append(str(val))
            elif dtype == "boolean":
                formatted_values.append("TRUE" if val else "FALSE")
            else:
                escaped_str = str(val).replace("'", "''")
                formatted_values.append(f"'{escaped_str}'")

        insert_statements.append(
            f"INSERT INTO {table_name} VALUES ({', '.join(formatted_values)});"
        )

    return ddl + "\n".join(insert_statements) + "\n"