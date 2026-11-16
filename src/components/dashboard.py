import plotly.express as px
import streamlit as st


def render_kpi_cards(metrics_orig: dict, metrics_final: dict):
    """Mostra le KPI cards con confronto pre/post pulizia."""
    orig_rows = metrics_orig["total_rows"]
    final_rows = metrics_final["total_rows"]
    duplicates_removed = max(0, metrics_orig["duplicate_rows"] - metrics_final["duplicate_rows"])

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Record Originali", orig_rows)
    col2.metric(
        "Record Finali",
        final_rows,
        delta=final_rows - orig_rows if final_rows != orig_rows else None,
    )
    col3.metric("Duplicati Rimossi", duplicates_removed)
    col4.metric("Null Trovati (Originali)", metrics_orig["total_nulls"])


def render_charts(metrics: dict):
    """Genera i 3 grafici interattivi Plotly: Torta integrità, Barre null, Distribuzione tipi."""
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Integrità Dati")
        total_cells = metrics["total_rows"] * metrics["total_cols"]
        null_cells = metrics["total_nulls"]
        dup_cells = metrics["duplicate_rows"] * metrics["total_cols"]
        valid_cells = max(0, total_cells - null_cells - dup_cells)

        fig_pie = px.pie(
            names=["Dati Validi", "Valori Null/Vuoti", "Celle in Righe Duplicate"],
            values=[valid_cells, null_cells, dup_cells],
            hole=0.45,
            color_discrete_sequence=["#2ECC71", "#E74C3C", "#F39C12"],
        )
        fig_pie.update_layout(margin=dict(t=20, b=20, l=10, r=10), legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        st.subheader("Null per Colonna")
        nulls = metrics["nulls_per_column"]
        fig_bar = px.bar(
            x=list(nulls.keys()),
            y=list(nulls.values()),
            labels={"x": "Colonna", "y": "Conteggio Null"},
            color_discrete_sequence=["#E74C3C"],
        )
        fig_bar.update_layout(margin=dict(t=20, b=20, l=10, r=10))
        st.plotly_chart(fig_bar, use_container_width=True)

    with col3:
        st.subheader("Distribuzione Tipi Colonne")
        type_dist = metrics["type_distribution"]
        ordered_types = ["int", "float", "string", "date"]
        counts = [type_dist.get(t, 0) for t in ordered_types]

        fig_types = px.bar(
            x=ordered_types,
            y=counts,
            labels={"x": "Tipo Dato", "y": "Numero Colonne"},
            color=ordered_types,
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_types.update_layout(showlegend=False, margin=dict(t=20, b=20, l=10, r=10))
        st.plotly_chart(fig_types, use_container_width=True)