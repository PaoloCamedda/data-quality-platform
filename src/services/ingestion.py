import csv
import io
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from src.config import logger


def detect_separator(file_bytes: bytes) -> str:
    """Rileva automaticamente il separatore CSV analizzando i primi 4KB."""
    sample = file_bytes[:4096].decode("utf-8", errors="ignore")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        return dialect.delimiter
    except Exception:
        counts = {sep: sample.count(sep) for sep in [",", ";", "\t", "|"]}
        best_sep = max(counts, key=counts.get)
        return best_sep if counts[best_sep] > 0 else ","


def format_file_size(num_bytes: int) -> str:
    """Converte la dimensione in byte in un formato leggibile (KB, MB)."""
    for unit in ["B", "KB", "MB", "GB"]:
        if num_bytes < 1024.0:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} TB"


def load_dataset_to_spark(
    spark: SparkSession, file_bytes: bytes, filename: str
) -> tuple[DataFrame, dict]:
    """
    Carica i byte di un file (CSV, TXT, JSON, Parquet) in un DataFrame PySpark.
    Restituisce il DataFrame e i metadati di ingestion.
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "csv"
    size_str = format_file_size(len(file_bytes))
    duplicate_headers = []
    separator_info = "N/A (Formato strutturato)"

    logger.info(f"Ingestion '{filename}' ({ext.upper()}) | Dimensione: {size_str}")

    if ext == "parquet":
        pdf = pd.read_parquet(io.BytesIO(file_bytes), engine="pyarrow")
        # Convertiamo temporaneamente a stringa per rilevare anomalie e uniformare la pipeline
        pdf = pdf.astype(str).replace({"None": None, "nan": None, "NaT": None, "<NA>": None})

    elif ext == "json":
        text_stream = io.BytesIO(file_bytes)
        try:
            # Prova prima JSON standard (array di oggetti o dizionario)
            pdf = pd.read_json(text_stream)
        except ValueError:
            # Fallback per JSON Lines (NDJSON, una riga per record JSON)
            text_stream.seek(0)
            pdf = pd.read_json(text_stream, lines=True)

        # Se ci sono campi annidati (dict/list), li serializza in stringa per evitare errori di schema Spark
        for col in pdf.columns:
            pdf[col] = pdf[col].apply(
                lambda x: str(x) if isinstance(x, (dict, list)) else x
            )
        pdf = pdf.astype(str).replace({"None": None, "nan": None, "NaT": None, "<NA>": None})

    else:
        # CSV / TXT
        separator = detect_separator(file_bytes)
        separator_info = "\\t (TAB)" if separator == "\t" else separator

        text_io = io.TextIOWrapper(io.BytesIO(file_bytes), encoding="utf-8", errors="replace")
        pdf = pd.read_csv(text_io, sep=separator, dtype=str, keep_default_na=True)

        raw_text_first_line = file_bytes.splitlines()[0].decode("utf-8", errors="replace") if file_bytes else ""
        raw_headers = [h.strip() for h in raw_text_first_line.split(separator)]
        seen = set()
        for h in raw_headers:
            if h in seen and h not in duplicate_headers:
                duplicate_headers.append(h)
            seen.add(h)

    # Pulisce i nomi delle colonne per compatibilità con Spark e SQL
    clean_cols = []
    used_names = {}
    for idx, col in enumerate(pdf.columns):
        base_name = str(col).strip().replace(" ", "_")
        if not base_name or base_name.startswith("Unnamed:"):
            base_name = f"colonna_{idx + 1}"
        if base_name in used_names:
            used_names[base_name] += 1
            base_name = f"{base_name}_{used_names[base_name]}"
        else:
            used_names[base_name] = 0
        clean_cols.append(base_name)

    pdf.columns = clean_cols
    pdf = pdf.where(pd.notnull(pdf), None)

    df_spark = spark.createDataFrame(pdf)

    metadata = {
        "filename": filename,
        "format": ext.upper(),
        "separator": separator_info,
        "file_size": size_str,
        "duplicate_headers": duplicate_headers,
    }
    return df_spark, metadata