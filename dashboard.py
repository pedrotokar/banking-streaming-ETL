# dashboard.py
import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
from geopy.distance import geodesic
import pydeck as pdk

# --- Load Data from SQLite and CSVs ---
@st.cache_data
def load_data():
    transactions = pd.read_csv("data/transactions/transacoes_100k.csv", parse_dates=["data_horario"])
    users = pd.read_csv("data/informacoes_cadastro_100k.csv")
    regions = pd.read_csv("data/regioes_estados_brasil.csv")
    return transactions, users, regions

transactions, users, regions = load_data()


# --- Merge Data ---
users = users.rename(columns={"id_regiao": "id_regiao_u"})
regions_r = regions.rename(columns={"id_regiao": "id_regiao_r"})

transactions = transactions.rename(columns={"id_regiao": "id_regiao_t"})

# Join transactions with user data
merged_df = transactions.merge(users, left_on="id_usuario_pagador", right_on="id_usuario", how="left")
# Join with region data (transaction region)
merged_df = merged_df.merge(regions.rename(columns={
    "id_regiao": "id_regiao_t",
    "latitude": "latitude_t",
    "longitude": "longitude_t"
}), on="id_regiao_t", how="left")
# Join with region data (user region)
merged_df = merged_df.merge(regions.rename(columns={
    "id_regiao": "id_regiao_u",
    "latitude": "latitude_u",
    "longitude": "longitude_u"
})[["id_regiao_u", "latitude_u", "longitude_u"]], on="id_regiao_u", how="left")

# --- Add Additional Columns ---
merged_df["hora"] = merged_df["data_horario"].dt.hour
merged_df["t7_score"] = (merged_df["hora"] - 12) / 12

# Fake approval flag for testing
np.random.seed(42)
merged_df["aprovada"] = np.random.choice([True, False], size=len(merged_df), p=[0.8, 0.2])

# Compute distances
merged_df["distancia_km"] = merged_df.apply(lambda row: geodesic(
    (row["latitude_u"], row["longitude_u"]),
    (row["latitude_t"], row["longitude_t"])
).km if pd.notnull(row["latitude_u"]) and pd.notnull(row["latitude_t"]) else np.nan, axis=1)

# Distance category
bins = [0, 50, 300, 1000, np.inf]
labels = ["<50km", "50-300km", "300-1000km", ">1000km"]
merged_df["faixa_distancia"] = pd.cut(merged_df["distancia_km"], bins=bins, labels=labels)

# --- Sidebar Filters ---
st.sidebar.title("Filters")
st.sidebar.markdown("Filter by transaction type or hour")
trans_types = st.sidebar.multiselect("Transaction Type", merged_df["modalidade_pagamento"].unique(), default=merged_df["modalidade_pagamento"].unique())
hour_range = st.sidebar.slider("Hour of Day", 0, 23, (0, 23))

filtered_df = merged_df[
    (merged_df["modalidade_pagamento"].isin(trans_types)) &
    (merged_df["hora"] >= hour_range[0]) & (merged_df["hora"] <= hour_range[1])
]

# --- Main ---
st.title("Banking Transaction Dashboard")
st.markdown("Visual insights into approvals, risk scores, and user behavior")

# 1. Approval Overview
st.subheader("1. Transaction Approvals")
st.bar_chart(filtered_df["aprovada"].value_counts())

# 2. Hourly Frequency
st.subheader("2. Transactions by Hour")
hour_freq = merged_df.groupby("hora").size()
st.line_chart(hour_freq)

# 3. Time Risk vs Approval
st.subheader("3. Hourly Risk vs Approval")
st.scatter_chart(merged_df[["t7_score", "aprovada"]])

# 4. Geographic Distance Analysis
st.subheader("4. Geographic Distance vs Approval")
dist_group = merged_df.groupby(["faixa_distancia", "aprovada"], observed=False).size().unstack(fill_value=0)
st.bar_chart(dist_group)

# 5. Denials by Transfer Type
st.subheader("5. Denied Transactions by Type")
denied_by_type = merged_df[merged_df["aprovada"] == False].groupby("modalidade_pagamento").size()
st.bar_chart(denied_by_type)

# 6. Frequency Score per User
st.subheader("6. User Frequency Score")
merged_df["rounded_hour"] = merged_df["data_horario"].dt.floor("h")
freqs = merged_df.groupby(["id_usuario_pagador", "rounded_hour"], observed=False).size().reset_index(name="frequency")
conditions = [
    freqs["frequency"] <= 3,
    freqs["frequency"].between(4, 10),
    freqs["frequency"] > 10
]
scores = [0, 0.5, 1]
freqs["frequency_score"] = np.select(conditions, scores)
st.dataframe(freqs.sort_values("frequency", ascending=False).head(10))

# 7. Region Map
st.subheader("7. Regional Transaction Map")
region_display = regions.copy()
region_display["lat"] = region_display["latitude"]
region_display["lon"] = region_display["longitude"]
st.pydeck_chart(pdk.Deck(
    map_style='mapbox://styles/mapbox/light-v9',
    initial_view_state=pdk.ViewState(
        latitude=region_display["lat"].mean(),
        longitude=region_display["lon"].mean(),
        zoom=3,
        pitch=0,
    ),
    layers=[
        pdk.Layer(
            "ScatterplotLayer",
            data=region_display,
            get_position='[lon, lat]',
            get_color='[200, 30, 0, 160]',
            get_radius=20000,
        ),
    ],
))
