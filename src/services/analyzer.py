import re
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from src.config import logger


def _infer_semantic_type(values: list[str]) -> tuple[str, bool]:
    """
    Analizza un campione di valori non nulli di una colonna per determinare:
    - il tipo dominante ('int', 'float', 'date', 'string')
    - se la colonna presenta tipi misti/incoerenti (True/False)
    """
    if not values:
        return "string", False

    int_pattern = re.compile(r"^[+-]?\d+$")
    float_pattern = re.compile(r"^[+-]?(\d+[.,]\d+|\d+[.,]|[.,]\d+)$")
    date_pattern = re.compile(
        r"^(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{2,4})([ T]\d{2}:\d{2}(:\d{2})?)?$"
    )

    counts = {"int": 0, "float": 0, "date": 0, "string": 0}
    for v in values:
        s = str(v).strip()
        if int_pattern.match(s):
            counts["int"] += 1
        elif float_pattern.match(s):
            counts["float"] += 1
        elif date_pattern.match(s):
            counts["date"] += 1
        else:
            counts["string"] += 1

    dominant_type = max(counts, key=counts.get)
    # Se un colonne numerica o data contiene anche stringhe (o viceversa in modo significativo), è incoerente
    non_zero_types = [k for k, count in counts.items() if count > 0]
    is_inconsistent = len(non_zero_types) > 1 and not (
        set(non_zero_types) == {"int", "float"}
    )
    return dominant_type, is_inconsistent


def analyze_dataset(df: DataFrame) -> dict:
    """Esegue un audit completo di Data Quality sul DataFrame PySpark."""
    logger.info("Avvio analisi Data Quality...")
    total_rows = df.count()
    cols = df.columns
    total_cols = len(cols)

    if total_rows == 0 or total_cols == 0:
        return {
            "total_rows": 0,
            "total_cols": total_cols,
            "duplicate_rows": 0,
            "duplicate_content_cols": [],
            "fully_empty_rows": 0,
            "total_nulls": 0,
            "total_empty_strings": 0,
            "valid_pct": 0.0,
            "missing_pct": 0.0,
            "column_types": {},
            "type_distribution": {"int": 0, "float": 0, "string": 0, "date": 0},
            "nulls_per_column": {},
            "inconsistent_columns": [],
        }

    # 1. Righe duplicate
    distinct_rows = df.distinct().count()
    duplicate_rows = total_rows - distinct_rows

    # 2. Valori NULL e stringhe vuote ("") per colonna
    agg_exprs = []
    for c in cols:
        col_str = F.col(f"`{c}`").cast("string")
        agg_exprs.append(F.sum(F.when(F.col(f"`{c}`").isNull(), 1).otherwise(0)).alias(f"null__{c}"))
        agg_exprs.append(
            F.sum(
                F.when(F.col(f"`{c}`").isNotNull() & (F.trim(col_str) == ""), 1).otherwise(0)
            ).alias(f"empty__{c}")
        )

    agg_result = df.agg(*agg_exprs).collect()[0].asDict()

    nulls_per_column = {}
    empty_per_column = {}
    for c in cols:
        n_count = agg_result[f"null__{c}"] or 0
        e_count = agg_result[f"empty__{c}"] or 0
        nulls_per_column[c] = n_count + e_count
        empty_per_column[c] = e_count

    total_nulls = sum(nulls_per_column.values())
    total_empty_strings = sum(empty_per_column.values())

    # 3. Righe completamente vuote (tutte le celle null o vuote)
    non_empty_conditions = [
        (F.col(f"`{c}`").isNotNull() & (F.trim(F.col(f"`{c}`").cast("string")) != ""))
        for c in cols
    ]
    any_valid_cell = non_empty_conditions[0]
    for cond in non_empty_conditions[1:]:
        any_valid_cell = any_valid_cell | cond

    non_empty_rows_count = df.filter(any_valid_cell).count()
    fully_empty_rows = total_rows - non_empty_rows_count

    # 4. Inferenza tipi e rilevamento colonne incoerenti / colonne duplicate nel contenuto
    sample_pdf = df.limit(500).toPandas()
    column_types = {}
    inconsistent_columns = []
    type_distribution = {"int": 0, "float": 0, "string": 0, "date": 0}

    for c in cols:
        spark_type = df.schema[c].dataType.simpleString()
        if spark_type in ("int", "bigint", "smallint", "tinyint"):
            inferred, inconsistent = "int", False
        elif spark_type in ("double", "float", "decimal"):
            inferred, inconsistent = "float", False
        elif spark_type in ("date", "timestamp"):
            inferred, inconsistent = "date", False
        else:
            valid_vals = [
                v for v in sample_pdf[c].dropna().tolist() if str(v).strip() != ""
            ]
            inferred, inconsistent = _infer_semantic_type(valid_vals)

        column_types[c] = inferred
        type_distribution[inferred] = type_distribution.get(inferred, 0) + 1
        if inconsistent:
            inconsistent_columns.append(c)

    # Colonne duplicate (stesso identico contenuto su tutto il campione)
    duplicate_content_cols = []
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            c1, c2 = cols[i], cols[j]
            if sample_pdf[c1].fillna("__NA__").equals(sample_pdf[c2].fillna("__NA__")):
                duplicate_content_cols.append(f"{c1} == {c2}")

    # 5. Percentuali di validità
    total_cells = total_rows * total_cols
    missing_pct = (total_nulls / total_cells * 100.0) if total_cells > 0 else 0.0
    valid_pct = max(0.0, 100.0 - missing_pct)

    return {
        "total_rows": total_rows,
        "total_cols": total_cols,
        "duplicate_rows": duplicate_rows,
        "duplicate_content_cols": duplicate_content_cols,
        "fully_empty_rows": fully_empty_rows,
        "total_nulls": total_nulls,
        "total_empty_strings": total_empty_strings,
        "valid_pct": round(valid_pct, 2),
        "missing_pct": round(missing_pct, 2),
        "column_types": column_types,
        "type_distribution": type_distribution,
        "nulls_per_column": nulls_per_column,
        "inconsistent_columns": inconsistent_columns,
    }