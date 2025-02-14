import streamlit as st
import numpy as np
import pandas as pd
import sys
import itertools
#from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
#from pyspark.ml import Pipeline
#from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
#from pyspark.ml.evaluation import MulticlassClassificationEvaluator
#from sklearn.metrics import classification_report, confusion_matrix
from pyspark.sql.functions import to_date, col, sum as spark_sum, countDistinct, count
# Import Sparksession
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.express as px

import warnings
warnings.filterwarnings('ignore')

#_spark=SparkSession.builder.appName("Data_Wrangling").getOrCreate()

# --- Initialisation de la session Spark et chargement des données ---

@st.cache_resource(show_spinner=False)
def get_spark_session():
    """
    Initialise et renvoie la session Spark.
    """
    _spark = SparkSession.builder.appName("BIGDATA").getOrCreate()
    return _spark

#@st.cache_data(show_spinner=False)
def load_data(_spark):
    """
    Charge le jeu de données CSV en utilisant PySpark.
    """
    # Remplacez le chemin par le chemin réel vers votre fichier CSV
    df = _spark.read.csv("Pakistan Largest Ecommerce Dataset.csv", header=True, inferSchema=True)
    return df

# --- Pages du tableau de bord ---

def Preprocessing(df):
    col1, col2 = st.columns(2)
    # Suppression des colonnes nulles
    drop_columns_list=["_c21","_c22","_c23","_c24","_c25"]
    df1 = df.drop(*drop_columns_list)
    # Suppression des lignes vides
    df1 = df1.na.drop(how = "all")
    # Remplissage des valeurs nulles
    mode_status = df1.groupby("status").count().orderBy("count", ascending=False).first()[0]
    df1 = df1.fillna(mode_status, subset=['status'])
    mode_category_name_1 = df1.groupby("category_name_1").count().orderBy("count", ascending=False).first()[0]
    df1 = df1.fillna(mode_category_name_1, subset=['category_name_1'])
    # Suppression des lignes nulles
    df1 = df1.na.drop(subset=['Working Date', 'sku', 'Customer ID'])
    #fixer les types de colonnes
    df1 = df1.withColumn('created_at', F.to_date(F.unix_timestamp('created_at', 'M/d/y').cast('timestamp')))
    df1 = df1.withColumn('Working Date', F.to_date(F.unix_timestamp('Working Date', 'M/d/y').cast('timestamp')))
    #nDefinir les type de colonnes
    df1 = df1.withColumn("qty_ordered", df1["qty_ordered"].cast(IntegerType()))
    df1 = df1.withColumn("price", df1["price"].cast(IntegerType()))
    df1 = df1.withColumn("grand_total", df1["grand_total"].cast(IntegerType()))
    df1 = df1.withColumn("discount_amount", df1["discount_amount"].cast(IntegerType()))
    df1 = df1.withColumn("Year", df1["Year"].cast(StringType()))
    df1 = df1.withColumn("Customer ID", df1["Customer ID"].cast(StringType()))
    df1 = df1.withColumn("Month", df1["Month"].cast(StringType()))
    # Supprimer les espaces supplementaires
    df1 = df1.withColumnRenamed(" MV ", "MV")
    df1 = df1.withColumnRenamed("created_at", "order_date")
    #Supprimer les lignes dupliquees
    df1 = df1.drop_duplicates()
    return df1


# Page d'accueil
def page_overview(df1):
    st.title("Vue d'ensemble / KPI Globaux")
    st.subheader("📈 Performande globale de la Plateform E-commerce")
    # Sidebar pour les filtres
    st.sidebar.header("Filtres")
    date_range = st.sidebar.slider("Sélectionner la période", 2016, 2018, (2016, 2018))
    categorie = st.sidebar.multiselect("Sélectionner une catégorie", df1.select("category_name_1").distinct())
    paiement = st.sidebar.multiselect("Mode de paiement", df1.select("payment_method").distinct())

    data_filtered = df1[(df1["Year"] >= date_range[0]) & (df1["Year"] <= date_range[1])]
    if categorie:
        data_filtered = data_filtered[data_filtered["category_name_1"].isin(categorie)]
    if paiement:
        data_filtered = data_filtered[data_filtered["payment_method"].isin(paiement)]

    # Calcul du chiffre d'affaires total (grand_total)
    total_revenue = data_filtered.agg(spark_sum("grand_total").alias("total_revenue")).collect()[0]["total_revenue"]
    #total_rev = data_filtered.agg(spark_sum("grand_total").alias("total_rev")).collect()[0][total_rev]
    # Nombre total de produits commandés (on suppose que 'increment_id' est l'identifiant de commande)
    total_orders = data_filtered.select("item_id").distinct().count()

    # Panier moyen
    average_order_value = total_revenue / total_orders if total_orders != 0 else 0

    # Taux de commandes réussies : ici nous considérons que le statut "completed" indique une commande réussie
    successful_orders = data_filtered.filter(col("status") == "complete").select("item_id").distinct().count()
    success_rate = (successful_orders / total_orders) * 100 if total_orders != 0 else 0
    
    # Affichage des KPIs
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Chiffre d'affaires total (2016-2017-2018)", f"{total_revenue:.2f}")
    #col1.metric("Chiffre d'affaire annuel", f"{total_rev:.2f}")
    col2.metric("Nombre total de commandes (2016-2017-2018)", total_orders)
    col3.metric("Panier moyen", f"{average_order_value:.2f}")
    col4.metric("Taux de succès", f"{success_rate:.2f}%")

    # Visualisation des categories
    st.subheader("📈Les Categories des produits vendus")
    best_category = data_filtered.groupby("category_name_1").count().sort(col("count").desc()).toPandas()
    best_category = data_filtered.groupby("category_name_1").count().sort(F.col("count").desc()).toPandas()
    # Mettre la colonne 'category_name_1' comme index puis la réinitialiser pour qu'elle devienne une colonne
    best_category.set_index('category_name_1', inplace=True)
    best_category = best_category.reset_index()
    # Création du diagramme circulaire avec Plotly Express
    fig = px.pie(
        best_category,
        names='category_name_1',   # Les catégories seront utilisées comme labels
        values='count',            # Les valeurs associées à chaque catégorie
        title="Meuilleurs Categories des produits vendus", 
        hole=0  # Optionnel : pour un pie chart classique (mettre une valeur > 0 pour un donut chart)
    )
    fig.update_traces(textinfo='percent+label')
    # Positionner la légende (similaire à bbox_to_anchor dans matplotlib)
    fig.update_layout(legend=dict(x=1.1, y=0.5))
    # Afficher le graphique dans le tableau de bord Streamlit
    st.plotly_chart(fig)

    # Mode de paiements

    st.subheader("📈 Situation des commandes au cours de la periode")
    # Affichage par annees
    # Récupérer la liste des années distinctes triées
    years = [row["Year"] for row in data_filtered.select("Year").distinct().sort("Year").collect()]

    # Créer un graphique avec autant de colonnes que d'années
    fig = make_subplots(
        rows=1,
        cols=len(years),
        subplot_titles=[f"État des commandes en {year}" for year in years]
    )
    # Pour chaque année, extraire les données et ajouter un histogramme
    for idx, year in enumerate(years, start=1):
        # Filtrer les données pour l'année courante
        status_rows = data_filtered.filter(F.col("Year") == year).select("status").collect()
        # Extraire les valeurs de la colonne 'status' et exclure les éventuelles valeurs None
        status_values = [row['status'] for row in status_rows if row['status'] is not None]
        
        # Créer une trace histogramme pour l'année courante
        trace = go.Histogram(x=status_values, name=str(year), showlegend=False)
        fig.add_trace(trace, row=1, col=idx)
        
        # Configurer les axes de chaque sous-graphique
        fig.update_xaxes(title_text="État des commandes par an", tickangle=90, row=1, col=idx)
        fig.update_yaxes(title_text="Nombre total de commandes", row=1, col=idx)

    # Mettre à jour la mise en page du graphique
    fig.update_layout(
        title_text="État des commandes par an",
        height=500,
        width=2200,
        bargap=0.2
    )
    # Afficher le graphique dans le tableau de bord Streamlit
    st.plotly_chart(fig)
    from pyspark.sql.functions import count, when
    # Total des commandes
    total_orders = data_filtered.count()
    # Commandes annulées
    canceled_orders = data_filtered.filter(col("status") == "canceled").count()
    # Taux d'annulation
    cancellation_rate = (canceled_orders / total_orders) * 100
    # Données pour le graphique
    data = {
        "Status": ["Annulées", "Non annulées"],
        "Count": [canceled_orders, total_orders - canceled_orders]
    }
    st.subheader("📈 Etats des commandes")
    fig = go.Figure(data=[go.Pie(labels=data["Status"], values=data["Count"], hole=0.5)])
    fig.update_layout(title="Répartition des commandes annulées")
    st.plotly_chart(fig)


def Periode(df1):
    categorie = st.sidebar.multiselect("Sélectionner une catégorie", df1.select("category_name_1").distinct())
    paiement = st.sidebar.multiselect("Mode de paiement", df1.select("payment_method").distinct())
    data_filtered = df1
    if categorie:
        data_filtered = df1[df1["category_name_1"].isin(categorie)]
    if paiement:
        data_filtered = df1[df1["payment_method"].isin(paiement)]
    st.subheader("📈 Meilleurs produit par an")
    df_best_year = data_filtered.filter(df1.status == "complete") \
            .groupBy("Year", "sku") \
            .agg(F.sum("qty_ordered").alias("total_sold")) \
            .orderBy(F.desc("total_sold")) \
            .limit(10) \
            .toPandas()
    fig = px.bar(df_best_year, x="Year", y="total_sold", color="sku",title="Top 10 Best-sellers par an", labels={"total_sold": "Quantité vendue", "Year": "Année"},barmode="group")
    st.plotly_chart(fig)
    st.subheader("📈 Evolution du chiffre d'affaires")
    ca_per_day = data_filtered.groupBy("order_date").agg(
    F.sum("grand_total").alias("total_revenue")
    ).orderBy("order_date")
    ca_per_day_pd = ca_per_day.toPandas()

    # Création du graphique
    fig = px.line(ca_per_day_pd, x="order_date", y="total_revenue",
                title="Répartition du chiffre d'affaires par jour",
                labels={"order_date": "Date", "total_revenue": "Chiffre d'Affaires"})
    st.plotly_chart(fig)

    ca_per_month = data_filtered.groupBy("Year", "Month").agg(
        F.sum("grand_total").alias("total_revenue")
    ).orderBy("Year", "Month")

    ca_per_month_pd = ca_per_month.toPandas()
    fig = px.bar(ca_per_month_pd, x=["Month"], y="total_revenue",
                title="Chiffre d'affaire par mois",
                labels={"total_revenue": "CA"})
    st.plotly_chart(fig)

    orders_per_day = data_filtered.groupBy("order_date").agg(
    F.count("increment_id").alias("total_orders")
    ).orderBy("order_date")
    orders_per_day_pd = orders_per_day.toPandas()
    fig = px.bar(orders_per_day_pd, x="order_date", y="total_orders",
                title="Nombre de commandes par jour",
                labels={"order_date": "Date", "total_orders": "Nombre de Commandes"})
    st.plotly_chart(fig)

    orders_per_month = data_filtered.groupBy("Year", "Month").agg(
    F.count("increment_id").alias("total_orders")
        ).orderBy("Year", "Month")
    orders_per_month_pd = orders_per_month.toPandas()
    fig = px.bar(orders_per_month_pd, x=["Month"], y="total_orders",
                title="Nombre de commandes par mois",
                labels={"total_orders": "Nombre de Commandes"})
    st.plotly_chart(fig)

   
def Paiement(df1):
    st.sidebar.header("Filtres")
    date_range = st.sidebar.slider("Sélectionner la période", 2016, 2018, (2016, 2018))
    data_filtered = df1
    fy = st.sidebar.multiselect("Année budgétaire", df1.select("FY").distinct())
    if categorie:
        data_filtered = data_filtered[data_filtered["FY"].isin(fy)]

    from pyspark.sql.window import Window
    st.subheader("📈 Les modes de paiement les plus utilisés")
    payment_mode_count = data_filtered.groupBy("payment_method") \
    .count() \
    .orderBy(col("count").desc()).limit(7)
    data_payment = payment_mode_count.toPandas()

    fig = px.pie(data_payment, names="payment_method", values="count",
                title="Top 7 des modes de paiement les plus utilisés")
    st.plotly_chart(fig)

    st.subheader("📈 Frequence des modes de paiements suivant les années")
    payment_by_category = df1.groupBy("category_name_1", "payment_method", "Year", "Month") \
    .count() \
    .withColumn("rank", row_number().over(Window.partitionBy("category_name_1", "Year", "Month").orderBy(col("count").desc()))) \
    .filter(col("rank") == 1).select("category_name_1", "payment_method", "Year", "Month", "count")
    data_payment_category = payment_by_category.toPandas()

    fig = px.bar(data_payment_category, x="category_name_1", y="count", color="payment_method",
                facet_col="Year", title="Mode de paiement récurrent par catégorie", labels={"count": "Nombre de commandes"})
    st.plotly_chart(fig)
    




def client(df1):
    date_range = st.sidebar.slider("Sélectionner la période", 2016, 2018, (2016, 2018))
    categorie = st.sidebar.multiselect("Sélectionner une catégorie", df1.select("category_name_1").distinct())
    paiement = st.sidebar.multiselect("Mode de paiement", df1.select("payment_method").distinct())
    data_filtered = df1[(df1["Year"] >= date_range[0]) & (df1["Year"] <= date_range[1])]
    if categorie:
        data_filtered = data_filtered[data_filtered["category_name_1"].isin(categorie)]
    if paiement:
        data_filtered = data_filtered[data_filtered["payment_method"].isin(paiement)]

    st.subheader("📈 Profil des clients")
    top_10_customers = data_filtered.groupBy("Customer ID").agg(
    F.sum("grand_total").alias("total_revenue")
    ).orderBy(F.desc("total_revenue")).limit(10)
    top_10_customers_pd = top_10_customers.toPandas()
    fig = px.bar(top_10_customers_pd, x="Customer ID", y="total_revenue",
                title="Top 10 des clients les plus contributifs",
                labels={"Customer ID": "ID Client", "total_revenue": "Chiffre d'Affaires"})
    st.plotly_chart(fig)

    st.subheader("📈 Les clients fidèles : ceux qui ont commandé au moins une fois durant les trois années")
    loyal_customers = data_filtered.filter(F.year("order_date").isin([2016, 2017, 2018])) \
    .groupBy("Customer ID") \
    .agg(F.countDistinct(F.year("order_date")).alias("years_active")) \
    .filter(F.col("years_active") == 3)
    loyal_customers.toPandas()
    st.table(loyal_customers)

def categorie(df1):
     st.sidebar.header("Filtres")
     date_range = st.sidebar.slider("Sélectionner la période", 2016, 2018, (2016, 2018))
     fy = st.sidebar.multiselect("Sélectionner une année budgétaire", df1.select("FY").distinct())
     status = st.sidebar.multiselect("Statut des commandes", df1.select("status").distinct())
     data_filtered = df1[(df1["Year"] >= date_range[0]) & (df1["Year"] <= date_range[1])]
     if fy:
        data_filtered = data_filtered[data_filtered["FY"].isin(fy)]
     if status:
         data_filtered = data_filtered[data_filtered["status"].isin(status)]

     st.subheader("📈 Nombre de commandes")
     import plotly.express as px
     orders_per_day = data_filtered.groupBy("order_date").agg(
    F.count("increment_id").alias("total_orders")
    ).orderBy("order_date")
     orders_per_day_pd = orders_per_day.toPandas()
     fig = px.bar(orders_per_day_pd, x="order_date", y="total_orders",
                title="Nombre de commandes par jour",
                labels={"order_date": "Date", "total_orders": "Nombre de Commandes"})
     st.plotly_chart(fig)

    # Agrégation par catégorie et produit
     df_best_category = data_filtered.groupBy("category_name_1", "sku") \
        .agg(sum("qty_ordered").alias("total_qty"))
    # Obtenir le best-seller par catégorie en utilisant un window
     from pyspark.sql.window import Window
     from pyspark.sql.functions import row_number
    # Fenêtre partitionnée par catégorie, ordonnée par quantité décroissante
     windowSpec = Window.partitionBy("category_name_1").orderBy(desc("total_qty"))
     st.subheader("📈 Best Seller par categorie")
     df_best_category = df_best_category.withColumn("rank", F.rank().over(windowSpec))
     best_seller_category = df_best_category.filter(col("rank") == 1)
     best_seller_category_pd = best_seller_category.toPandas()
     import plotly.express as px
     fig = px.bar(best_seller_category_pd, 
                x="category_name_1", y="total_qty", 
                color="sku", 
                title="Best Seller par catégorie")
     st.plotly_chart(fig)

     from pyspark.sql.functions import mean
    # Moyenne des commandes par catégorie
     avg_order_value_category = data_filtered.groupBy("category_name_1") \
        .agg(mean("grand_total").alias("avg_order_value")) \
        .orderBy(col("avg_order_value").desc())
     st.subheader("📈 Nombre moyen des commande par categories")
     data_category = avg_order_value_category.toPandas()
     fig = px.bar(data_category, x="category_name_1", y="avg_order_value",
                title="Commande moyenne par catégorie",
                labels={"avg_order_value": "Commande moyenne", "category_name_1": "Catégorie"})
     st.plotly_chart(fig)

    
     top_discounted_products = data_filtered.select("sku", "discount_amount") \
        .orderBy(col("discount_amount").desc()) \
        .limit(10)
     data_discount = top_discounted_products.toPandas()
     st.subheader("📈 Produits avec les remises maximales")
     fig = px.bar(data_discount, x="sku", y="discount_amount",
                title="Produits avec les plus grosses remises",
                labels={"sku": "Produit", "discount_amount": "Montant de la remise"})
     st.plotly_chart(fig)


    
     orders_by_month_category = data_filtered.groupBy("Month", "category_name_1") \
        .agg(count("increment_id").alias("order_count")) \
        .orderBy("Month", "order_count", ascending=False)
     data_month_category = orders_by_month_category.toPandas()
     st.subheader("📈 Répartition des commandes par catégorie et par mois")
     fig = px.density_heatmap(data_month_category, x="Month", y="category_name_1", z="order_count",
                            title="Répartition des commandes par mois et catégorie",
                            labels={"Month": "Mois", "category_name_1": "Catégorie", "order_count": "Nombre de commandes"},
                            color_continuous_scale="Viridis")
     st.plotly_chart(fig)

    
     avg_discount_by_category = data_filtered.groupBy("category_name_1") \
        .agg(mean("discount_amount").alias("avg_discount")) \
        .orderBy("avg_discount", ascending=False)

     data_avg_discount = avg_discount_by_category.toPandas()
     st.subheader("📈 Taux de remise moyen par catégorie")
     fig = px.bar(data_avg_discount, x="category_name_1", y="avg_discount",
                title="Taux de remise moyen par catégorie",
                labels={"category_name_1": "Catégorie", "avg_discount": "Remise moyenne"})
     st.plotly_chart(fig)

    
     avg_basket_by_category = data_filtered.groupBy("category_name_1") \
        .agg(mean("grand_total").alias("avg_basket")) \
        .orderBy("avg_basket", ascending=False)

     data_avg_basket = avg_basket_by_category.toPandas()
     st.subheader("📈 CA moyen par catégorie")
     fig = px.bar(data_avg_basket, x="avg_basket", y="category_name_1", orientation="h",
                title="CA moyen par catégorie",
                labels={"category_name_1": "Catégorie", "avg_basket": "Panier moyen"})
     st.plotly_chart(fig)

    # Mode de paiement par catégorie
     payment_by_category = data_filtered.groupBy("category_name_1", "payment_method") \
        .count() \
        .orderBy("category_name_1", col("count").desc())
     data_payment_category = payment_by_category.toPandas()
     st.subheader("📈 Mode de paiement par catégorie")
     fig = px.bar(data_payment_category, x="category_name_1", y="count", color="payment_method",
                title="Répartition des modes de paiement par catégorie",
                labels={"category_name_1": "Catégorie", "count": "Nombre de commandes", "payment_method": "Mode de paiement"})
     st.plotly_chart(fig)
     st.subheader("📈 Best seller par an")
     df_best_year = data_filtered.groupBy("Year", "sku") \
    .agg(sum("qty_ordered").alias("total_qty"))

     windowSpec_year = Window.partitionBy("Year").orderBy(desc("total_qty"))
     df_best_year = df_best_year.withColumn("rank", F.rank().over(windowSpec_year))
     best_seller_year = df_best_year.filter(col("rank") == 1)
     best_seller_year_pd = best_seller_year.toPandas()
     fig = px.bar(best_seller_year_pd, 
             x="Year", y="total_qty", 
             color="sku", 
             title="Best Seller par année")
     st.plotly_chart(fig)


     most_expensive_product = data_filtered.groupBy("category_name_1").agg(
    F.max("price").alias("max_price"),
    F.first(F.struct("sku", "price"), ignorenulls=True).alias("product_details")
     ).select(
    "category_name_1",
    "product_details.sku",
    "product_details.price"
    )
     most_expensive_product_pd = most_expensive_product.toPandas()
     fig = px.bar(most_expensive_product_pd, x="category_name_1", y="price", color="sku",
                title="Produit le plus cher par catégorie", labels={"price": "Prix", "category_name_1": "Catégorie"})
     st.plotly_chart(fig)


# --- Fonction principale ---
def main():
    st.sidebar.title("Menue de l'application")
    menu = st.sidebar.radio(
        "Choisir une page",
        ("Accueil", "Analyse des ventes par periode", "Analyse par des produits par categories", "Analyse des paiements et des commissions", "Profil Client")
    )

    # Chargement de la session Spark et du jeu de données
    _spark = get_spark_session()
    df = load_data(_spark)
    df1 = Preprocessing(df)
    # Affichage de la page sélectionnée
    if menu == "Accueil":
        page_overview(df1)
    if menu == "Analyse des ventes par periode" :
        Periode(df1)
    if menu == "Analyse des paiements et des commissions" :
        Paiement(df1)
    if menu == "Profil Client" :
        client(df1)
    if menu == "Analyse par des produits par categories":
        categorie(df1)
if __name__ == "__main__":
    main()
