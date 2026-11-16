import os
import sys

# Aggiunge la root del progetto al PYTHONPATH per risolvere gli import "src.*"
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import streamlit as st
from src.components.dashboard import render_charts, render_kpi_cards
from src.components.ui_helpers import render_file_metadata, render_quality_audit_details
from src.config import logger
from src.services.analyzer import analyze_dataset
from src.services.cleaner import clean_dataset
from src.services.exporter import (
    export_to_csv,
    export_to_json,
    export_to_parquet_bytes,
    generate_sql_dump,
)
from src.services.ingestion import load_dataset_to_spark
from src.services.spark_session import get_spark_session

st.set_page_config(
    page_title="CSV Data Quality Platform",
    page_icon="📊",
    layout="wide",
)


@st.cache_resource
def init_spark():
    return get_spark_session()


@st.cache_data(show_spinner="Caricamento e analisi iniziale del dataset...")
def load_and_profile_original(file_bytes: bytes, filename: str):
    """Esegue ingestion e audit iniziale una sola volta per file caricato."""
    _spark = init_spark()
    df_raw, metadata = load_dataset_to_spark(_spark, file_bytes, filename)
    metrics_orig = analyze_dataset(df_raw)
    # Salviamo anche l'anteprima in Pandas per renderla istantanea
    preview_orig_pdf = df_raw.limit(15).toPandas()
    return metadata, metrics_orig, preview_orig_pdf


spark = init_spark()

st.title("📊 CSV Data Quality Platform")
st.caption("Motore di profiling, pulizia dati e generazione export multi-formato basato su PySpark.")

# --- SIDEBAR: UPLOAD ---
st.sidebar.header("1. Upload Dataset")
uploaded_file = st.sidebar.file_uploader("Carica un dataset (CSV, JSON, Parquet)", type=["csv", "txt", "json", "parquet"])
if uploaded_file is not None:
    file_bytes = uploaded_file.getvalue()

    try:
        metadata, metrics_orig, preview_orig_pdf = load_and_profile_original(
            file_bytes, uploaded_file.name
        )
    except Exception as e:
        logger.error(f"Errore ingestion: {e}")
        st.error(f"Impossibile leggere il file CSV: {e}")
        st.stop()

    # --- SIDEBAR: FORM CONFIGURAZIONE PULIZIA (ESEGUE SOLO AL CLICK DEL PULSANTE) ---
    st.sidebar.markdown("---")
    st.sidebar.header("2. Configura Pipeline")

    with st.sidebar.form(key="cleaning_pipeline_form"):
        remove_dup = st.checkbox("Rimozione duplicati", value=False)
        remove_empty_r = st.checkbox("Eliminazione righe vuote", value=False)
        drop_empty_c = st.checkbox("Eliminazione colonne completamente vuote", value=False)
        trim_str = st.checkbox("Trim degli spazi", value=True)
        auto_cast = st.checkbox("Conversione automatica dei tipi", value=False)

        normalize_mode = st.selectbox(
            "Normalizzazione testo",
            [
                "Nessuna",
                "Minuscolo (lowercase)",
                "Maiuscolo (UPPERCASE)",
                "Iniziale Maiuscola (Title Case)",
            ],
        )

        fill_nulls_val = st.text_input(
            "Sostituzione NULL con valore scelto (vuoto = mantieni NULL):",
            value="",
        )

        st.markdown("---")
        st.markdown("**3. Nome Output**")
        default_name = uploaded_file.name.rsplit(".", 1)[0] + "_clean"
        output_name = st.text_input("Nome file / tabella SQL finale", value=default_name)

        submitted = st.form_submit_button(
            label="Applica Modifiche",
            type="primary",
            use_container_width=True,
        )

    # Salva i parametri applicati nello state così restano attivi anche cambiando tab
    state_key = f"applied_params_{uploaded_file.name}_{len(file_bytes)}"
    if submitted or state_key not in st.session_state:
        st.session_state[state_key] = {
            "remove_dup": remove_dup if submitted else False,
            "remove_empty_r": remove_empty_r if submitted else False,
            "drop_empty_c": drop_empty_c if submitted else False,
            "trim_str": trim_str if submitted else False,
            "auto_cast": auto_cast if submitted else False,
            "normalize_mode": normalize_mode if submitted else "Nessuna",
            "fill_nulls_val": fill_nulls_val if submitted else "",
            "output_name": output_name.strip() if (submitted and output_name.strip()) else default_name,
        }

    params = st.session_state[state_key]

    # Esegue Spark solo con i parametri confermati
    try:
        with st.spinner("Esecuzione trasformazioni PySpark in corso..."):
            df_raw, _ = load_dataset_to_spark(spark, file_bytes, uploaded_file.name)
            df_clean = clean_dataset(
                df=df_raw,
                inferred_types=metrics_orig["column_types"],
                remove_duplicates=params["remove_dup"],
                remove_empty_rows=params["remove_empty_r"],
                trim_strings=params["trim_str"],
                auto_cast_types=params["auto_cast"],
                fill_null_value=params["fill_nulls_val"] if params["fill_nulls_val"] != "" else None,
                drop_empty_cols=params["drop_empty_c"],
                normalize_text_mode=params["normalize_mode"],
            )
            metrics_final = analyze_dataset(df_clean)
    except Exception as e:
        logger.error(f"Errore durante la pulizia: {e}")
        st.error(f"Errore nella pipeline di pulizia: {e}")
        st.stop()

    # --- NAVIGAZIONE PRINCIPALE ---
    tab_upload, tab_analysis, tab_dashboard, tab_download = st.tabs(
        [
            "📂 1. Upload & Anteprima",
            "🔍 2. Analisi Data Quality",
            "📈 3. Dashboard & KPI",
            "📥 4. Download & SQL Dump",
        ]
    )

    with tab_upload:
        st.subheader("Dettagli File Caricato")
        render_file_metadata(metadata, metrics_orig["total_rows"], metrics_orig["total_cols"])
        st.markdown("---")
        col_pre, col_post = st.columns(2)
        with col_pre:
            st.markdown("#### Anteprima Originale (Prime 15 righe)")
            st.dataframe(preview_orig_pdf, use_container_width=True)
        with col_post:
            st.markdown("#### Anteprima Post-Pulizia (Prime 15 righe)")
            st.dataframe(df_clean.limit(15).toPandas(), use_container_width=True)

    with tab_analysis:
        st.subheader("Audit Completo sulla Qualità dei Dati (Dataset Originale)")
        render_quality_audit_details(metrics_orig)

    with tab_dashboard:
        st.subheader("KPI di Trasformazione")
        render_kpi_cards(metrics_orig, metrics_final)
        st.markdown("---")
        st.subheader("Analisi Visiva (Dataset Risultante)")
        render_charts(metrics_final)

    with tab_download:
        final_out_name = params["output_name"]
        st.subheader(f"Esportazione Risultati: `{final_out_name}`")
        st.write(
            f"Record pronti per l'esportazione: **{metrics_final['total_rows']}** | "
            f"Colonne: **{metrics_final['total_cols']}**"
        )

        c1, c2, c3, c4 = st.columns(4)

        with c1:
            csv_data = export_to_csv(df_clean)
            st.download_button(
                label="📄 Scarica CSV",
                data=csv_data,
                file_name=f"{final_out_name}.csv",
                mime="text/csv",
                use_container_width=True,
            )

        with c2:
            json_data = export_to_json(df_clean)
            st.download_button(
                label="{} Scarica JSON",
                data=json_data,
                file_name=f"{final_out_name}.json",
                mime="application/json",
                use_container_width=True,
            )

        with c3:
            parquet_data = export_to_parquet_bytes(df_clean)
            st.download_button(
                label="📦 Scarica Parquet",
                data=parquet_data,
                file_name=f"{final_out_name}.parquet",
                mime="application/octet-stream",
                use_container_width=True,
            )

        with c4:
            sql_data = generate_sql_dump(df_clean, final_out_name)
            st.download_button(
                label="🗄️ Scarica SQL Dump",
                data=sql_data,
                file_name=f"{final_out_name}.sql",
                mime="text/plain",
                use_container_width=True,
            )

        st.markdown("---")
        st.markdown("#### Anteprima Script SQL Generato (Prime 25 righe)")
        sql_preview = "\n".join(sql_data.splitlines()[:25])
        st.code(sql_preview, language="sql")

else:
    st.info("👈 Carica un file CSV dalla barra laterale a sinistra per avviare l'analisi.")