"""
bench_viz.py — Point d'entrée Streamlit

Définit la navigation et masque bench_viz de la sidebar.
Lancement : streamlit run bench_viz.py
"""
import streamlit as st

pg = st.navigation([
    st.Page("pages/0_Synthèse.py",   title="Synthèse",   icon="📊"),
    st.Page("pages/1_Écriture.py",   title="Écriture",   icon="✍️"),
    st.Page("pages/2_Lecture.py",    title="Lecture",    icon="📖"),
    st.Page("pages/3_Vectoriel.py",  title="Vectoriel",  icon="🔢"),
])
pg.run()