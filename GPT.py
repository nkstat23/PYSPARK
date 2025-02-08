import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

# Générer une base de données fictive
def generate_fake_data(n=5000):
    np.random.seed(42)
    categories = ["Mode", "Mobile", "Électronique", "Électroménager", "Beauté", "Sport"]
    modes_paiement = ["Carte de crédit", "Easy-Paisa", "Jazz-Cash", "Paiement à la livraison"]
    statuts = ["Terminée", "Annulée", "Remboursée"]
    
    data = {
        "Date_Commande": [datetime(2016, 1, 1) + timedelta(days=np.random.randint(0, 973)) for _ in range(n)],
        "Categorie": np.random.choice(categories, n),
        "Mode_Paiement": np.random.choice(modes_paiement, n),
        "Statut": np.random.choice(statuts, n, p=[0.8, 0.15, 0.05]),
        "Quantité": np.random.randint(1, 10, n),
        "Total": np.random.randint(500, 50000, n)
    }
    return pd.DataFrame(data)

@st.cache_data
def load_data():
    return generate_fake_data()

data = load_data()

data["Année"] = pd.to_datetime(data["Date_Commande"]).dt.year

# Sidebar pour les filtres
st.sidebar.header("Filtres")
date_range = st.sidebar.slider("Sélectionner la période", 2016, 2018, (2016, 2018))
categorie = st.sidebar.multiselect("Sélectionner une catégorie", data["Categorie"].unique())
paiement = st.sidebar.multiselect("Mode de paiement", data["Mode_Paiement"].unique())

data_filtered = data[(data["Année"] >= date_range[0]) & (data["Année"] <= date_range[1])]
if categorie:
    data_filtered = data_filtered[data_filtered["Categorie"].isin(categorie)]
if paiement:
    data_filtered = data_filtered[data_filtered["Mode_Paiement"].isin(paiement)]

# KPIs principaux
st.title("📊 Tableau de bord E-Commerce - Pakistan")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Nombre total de commandes", data_filtered.shape[0])
with col2:
    st.metric("Chiffre d’affaires total (PKR)", f"{data_filtered['Total'].sum():,.0f}")
with col3:
    taux_annulation = data_filtered[data_filtered["Statut"] == "Annulée"].shape[0] / data_filtered.shape[0] * 100
    st.metric("Taux d’annulation", f"{taux_annulation:.2f}%")

# Visualisation des ventes par mois
st.subheader("📈 Évolution des ventes")
data_filtered["Mois"] = pd.to_datetime(data_filtered["Date_Commande"]).dt.to_period("M")
ventes_mensuelles = data_filtered.groupby("Mois")["Total"].sum()
fig, ax = plt.subplots()
ventes_mensuelles.plot(kind='line', ax=ax)
ax.set_xlabel("Mois")
ax.set_ylabel("Chiffre d’affaires (PKR)")
st.pyplot(fig)

# Répartition des commandes par catégorie
st.subheader("🎯 Répartition des commandes par catégorie")
categorie_count = data_filtered["Categorie"].value_counts()
st.bar_chart(categorie_count)

# Meilleurs produits
st.subheader("🏆 Top 10 Produits les plus vendus")
top_produits = data_filtered.groupby("Categorie")["Quantité"].sum().nlargest(10)
st.bar_chart(top_produits)

st.write("📌 D’autres visualisations et analyses peuvent être ajoutées !")