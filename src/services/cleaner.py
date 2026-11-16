from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from src.config import logger


def clean_dataset(
    df: DataFrame,
    inferred_types: dict[str, str],
    remove_duplicates: bool = False,
    remove_empty_rows: bool = False,
    trim_strings: bool = False,
    auto_cast_types: bool = False,
    fill_null_value: str | None = None,
    drop_empty_cols: bool = False,
    normalize_text_mode: str = "Nessuna",
) -> DataFrame:
    """Applica in sequenza le regole di pulizia selezionate dall'utente."""
    logger.info("Avvio pipeline di pulizia dati...")
    cols = df.columns

    # 1. Converte eventuali stringhe vuote "" in NULL standard per trattarle uniformemente
    for c in cols:
        if df.schema[c].dataType.simpleString() == "string":
            df = df.withColumn(
                c,
                F.when(F.trim(F.col(f"`{c}`")) == "", None).otherwise(F.col(f"`{c}`")),
            )

    # 2. Trim degli spazi bianchi superflui
    if trim_strings:
        for c in df.columns:
            if df.schema[c].dataType.simpleString() == "string":
                df = df.withColumn(c, F.trim(F.col(f"`{c}`")))

    # 3. Normalizzazione testo (Minuscolo, Maiuscolo, Title Case)
    if normalize_text_mode != "Nessuna":
        for c in df.columns:
            if df.schema[c].dataType.simpleString() == "string":
                if normalize_text_mode == "Minuscolo (lowercase)":
                    df = df.withColumn(c, F.lower(F.col(f"`{c}`")))
                elif normalize_text_mode == "Maiuscolo (UPPERCASE)":
                    df = df.withColumn(c, F.upper(F.col(f"`{c}`")))
                elif normalize_text_mode == "Iniziale Maiuscola (Title Case)":
                    df = df.withColumn(c, F.initcap(F.col(f"`{c}`")))

    # 4. Eliminazione colonne completamente vuote
    if drop_empty_cols:
        total_rows = df.count()
        if total_rows > 0:
            null_counts = df.agg(
                *[F.sum(F.col(f"`{c}`").isNull().cast("int")).alias(c) for c in df.columns]
            ).collect()[0].asDict()
            valid_cols = [c for c in df.columns if (null_counts[c] or 0) < total_rows]
            if valid_cols:
                df = df.select([f"`{c}`" for c in valid_cols])

    # 5. Eliminazione righe completamente vuote
    if remove_empty_rows and df.columns:
        conditions = [F.col(f"`{c}`").isNotNull() for c in df.columns]
        any_not_null = conditions[0]
        for cond in conditions[1:]:
            any_not_null = any_not_null | cond
        df = df.filter(any_not_null)

    # 6. Conversione automatica dei tipi tollerrante agli errori (ANSI-safe)
    if auto_cast_types:
        for c in df.columns:
            target = inferred_types.get(c, "string")
            col_expr = F.trim(F.col(f"`{c}`").cast("string"))

            if target == "int":
                # Rimuove eventuali .0 finali (es. "42.0" -> "42") e usa try_cast
                cleaned_int = F.regexp_replace(col_expr, r"[.,]0+$", "")
                df = df.withColumn(c, cleaned_int.try_cast("bigint"))

            elif target == "float":
                # Normalizza la virgola decimale europea e usa try_cast
                cleaned_num = F.regexp_replace(col_expr, ",", ".")
                df = df.withColumn(c, cleaned_num.try_cast("double"))

            elif target == "date":
                # Prova in sequenza i formati data più comuni senza sollevare eccezioni
                parsed_date = F.coalesce(
                    F.try_to_date(col_expr, "yyyy-MM-dd"),
                    F.try_to_date(col_expr, "yyyy/MM/dd"),
                    F.try_to_date(col_expr, "dd/MM/yyyy"),
                    F.try_to_date(col_expr, "dd-MM-yyyy"),
                    F.try_to_date(col_expr, "yyyy-MM-dd HH:mm:ss"),
                    F.try_to_date(col_expr, "yyyy/MM/dd HH:mm:ss"),
                    F.try_to_date(col_expr, "dd/MM/yyyy HH:mm:ss"),
                )
                df = df.withColumn(c, parsed_date)

    # 7. Sostituzione dei valori NULL con valore scelto dall'utente
    if fill_null_value is not None and fill_null_value != "":
        for c in df.columns:
            dtype = df.schema[c].dataType.simpleString()
            if dtype == "string":
                df = df.na.fill({c: fill_null_value})
            elif dtype in ("int", "bigint"):
                try:
                    df = df.na.fill({c: int(fill_null_value)})
                except ValueError:
                    pass
            elif dtype in ("double", "float"):
                try:
                    df = df.na.fill({c: float(fill_null_value.replace(",", "."))})
                except ValueError:
                    pass

    # 8. Rimozione righe duplicate
    if remove_duplicates:
        df = df.dropDuplicates()

    logger.info("Pulizia dati completata.")
    return df