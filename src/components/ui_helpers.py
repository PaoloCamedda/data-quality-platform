import pandas as pd
import streamlit as st


def render_file_metadata(metadata: dict, total_rows: int, total_cols: int):
    """Mostra i dettagli tecnici del CSV caricato."""
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Nome File", metadata["filename"])
    c2.metric("Dimensione", metadata["file_size"])
    c3.metric("Separatore Rilevato", f"`{metadata['separator']}`")
    c4.metric("Struttura", f"{total_rows} righe × {total_cols} col")

    if metadata.get("duplicate_headers"):
        st.warning(
            f"Intestazioni duplicate rilevate nel file originale (rinominate automaticamente): "
            f"**{', '.join(metadata['duplicate_headers'])}**"
        )


def render_quality_audit_details(metrics: dict):
    """Mostra il riepilogo dettagliato delle anomalie e dei tipi delle colonne."""
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Dati Validi (%)", f"{metrics['valid_pct']}%")
    c2.metric("Dati Mancanti (%)", f"{metrics['missing_pct']}%")
    c3.metric("Righe Duplicati", metrics["duplicate_rows"])
    c4.metric("Righe Completamente Vuote", metrics["fully_empty_rows"])

    st.markdown("#### Diagnostica Schema & Anomalie")
    col_left, col_right = st.columns([1.4, 1])

    with col_left:
        schema_rows = []
        for col_name, col_type in metrics["column_types"].items():
            null_cnt = metrics["nulls_per_column"].get(col_name, 0)
            is_inconsistent = "Sì (Tipi misti)" if col_name in metrics["inconsistent_columns"] else "No"
            schema_rows.append({
                "Colonna": col_name,
                "Tipo Inferito": col_type,
                "Valori Null / Vuoti": null_cnt,
                "Dati Incoerenti": is_inconsistent,
            })
        st.dataframe(pd.DataFrame(schema_rows), use_container_width=True, hide_index=True)

    with col_right:
        if metrics["inconsistent_columns"]:
            st.error(
                f"**Colonne con dati incoerenti:** {', '.join(metrics['inconsistent_columns'])}"
            )
        else:
            st.success("Nessuna colonna con tipi di dato incoerenti rilevata.")

        if metrics["duplicate_content_cols"]:
            st.warning(
                f"**Colonne con contenuto identico (duplicate):** {', '.join(metrics['duplicate_content_cols'])}"
            )
        else:
            st.info("Nessuna colonna duplicata nel contenuto.")