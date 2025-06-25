# dashboard.py
import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, URL
from datetime import datetime
from geopy.distance import geodesic
import pydeck as pdk
import os
import redis

st.set_page_config(
    page_title="Banking Transaction Dashboard (Live PostgreSQL Data)",
    initial_sidebar_state="expanded",
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

POSTGRES_DB       = os.getenv("POSTGRES_DB", "bank")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "bank_etl")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ihateavroformat123")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")

def get_redis_connection():
    """Estabelece conexão com o Redis."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        r.ping()
        st.toast("Conexão com Redis bem-sucedida!")
        return r
    except redis.exceptions.ConnectionError as e:
        st.error(f"Não foi possível conectar ao Redis: {e}")
        return None

def get_postgres_connection():
    """Estabelece conexão com o PostgreSQL."""
    try:
        url_obj = URL.create(
            "postgresql+psycopg2",
            username=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            port=POSTGRES_PORT,
        )
        engine = create_engine(url_obj)
        st.toast("Conexão com PostgreSQL bem-sucedida!")
        return engine
    except Exception as e:
        st.error(f"Não foi possível conectar ao PostgreSQL: {e}")
        return None

# --- Load Data ---
ENGINE = get_postgres_connection()

@st.cache_data
def load_data():

    transactions = pd.read_sql("SELECT * FROM transacoes;", ENGINE, parse_dates=["data_horario"])
    users = pd.read_sql("SELECT * FROM usuarios;", ENGINE)
    regions = pd.read_csv("data/regioes_estados_brasil.csv")

    return transactions, users, regions


# --- Preprocess Data ---
def preprocess_data(transactions, users, regions):
    users = users.rename(columns={"id_regiao": "region_id_u"})
    transactions = transactions.rename(columns={"id_regiao": "region_id_t"})

    df = transactions.merge(users, left_on="id_usuario_pagador", right_on="id_usuario", how="left")

    df = df.merge(
        regions.rename(columns={"id_regiao": "region_id_t", "latitude": "lat_t", "longitude": "lon_t"}),
        on="region_id_t", how="left"
    ).merge(
        regions.rename(columns={"id_regiao": "region_id_u", "latitude": "lat_u", "longitude": "lon_u"}),
        on="region_id_u", how="left"
    )

    df["hour"] = df["data_horario"].dt.hour
    df["rounded_hour"] = df["data_horario"].dt.floor("h")

    # Distance calculation
    bins = [0, 50, 300, 1000, np.inf]
    labels = ["<50km", "50-300km", "300-1000km", ">1000km"]
    df["distance_km"] = df.apply(
        lambda row: geodesic((row["lat_u"], row["lon_u"]), (row["lat_t"], row["lon_t"])).km
        if pd.notnull(row["lat_u"]) and pd.notnull(row["lat_t"]) else np.nan,
        axis=1
    )
    df["distance_bucket"] = pd.cut(df["distance_km"], bins=bins, labels=labels)

    # Frequency score
    freq = df.groupby(["id_usuario_pagador", "rounded_hour"]).size().reset_index(name="frequency")
    df = df.merge(freq, on=["id_usuario_pagador", "rounded_hour"], how="left")
    df["frequency_score"] = np.select(
        [df["frequency"] <= 3, df["frequency"].between(4, 10), df["frequency"] > 10],
        [0, 0.5, 1]
    )

    # Z-score transaction value
    stats = df.groupby("id_usuario_pagador")["valor_transacao"].agg(['mean', 'std']).reset_index()
    df = df.merge(stats, on="id_usuario_pagador", how="left")
    df["z_score"] = (df["valor_transacao"] - df["mean"]) / df["std"]

    # Time score
    df["time_score"] = abs(df["hour"] - 12) / 12

    # Denial reasons
    for m in ["PIX", "TED", "DOC", "Boleto"]:
        df[f"exceed_limit_{m.lower()}"] = (df["modalidade_pagamento"] == m) & (df["valor_transacao"] > df[f"limite_{m.lower()}"])
    df["denied_by_limit"] = df[[f"exceed_limit_{m.lower()}" for m in ["PIX", "TED", "DOC", "Boleto"]]].any(axis=1)
    df["denied_by_balance"] = df["valor_transacao"] > df["saldo"]

    return df




get_redis_connection()

# --- Load and Prepare ---
transactions, users, regions = load_data()
df = preprocess_data(transactions, users, regions)

# --- Streamlit UI ---
st.sidebar.header("Filters")
selected_types = st.sidebar.multiselect("Payment Types", df["modalidade_pagamento"].unique(), default=list(df["modalidade_pagamento"].unique()))
hour_range = st.sidebar.slider("Hour Range", 0, 23, (0, 23))

filtered_df = df[
    (df["modalidade_pagamento"].isin(selected_types)) &
    (df["hour"].between(hour_range[0], hour_range[1]))
]

# --- Analyses ---



# 1
st.subheader("1. Transaction Approval Overview")
st.bar_chart(filtered_df["transacao_aprovada"].value_counts())

# 2
st.subheader("2. Risk Score (Value) vs Approval")
st.scatter_chart(filtered_df[["valor_transacao", "transacao_aprovada"]])

# 3
st.subheader("3. Risk Score (Time) vs Approval")
st.scatter_chart(filtered_df[["time_score", "transacao_aprovada"]])

# 4
st.subheader("4. Region vs Approval Rate")
st.bar_chart(filtered_df.groupby("region_id_t")["transacao_aprovada"].mean())

# 5
st.subheader("5. Denials by Balance and Limit")
denial_counts = pd.DataFrame({
    "Limit": filtered_df["denied_by_limit"].sum(),
    "Balance": filtered_df["denied_by_balance"].sum()
}, index=["Count"])
st.bar_chart(denial_counts.T)

# 6
st.subheader("6. Denied Transactions by Payment Type")
denied_type_counts = filtered_df[filtered_df["transacao_aprovada"] == False].groupby("modalidade_pagamento").size()
st.bar_chart(denied_type_counts)

# 7
st.subheader("7. Hourly Transaction Frequency")
st.line_chart(df.groupby("hour").size())

# 8
st.subheader("8. Frequency Score vs Approval")
st.line_chart(df.groupby("frequency_score")["transacao_aprovada"].mean())

# 9
st.subheader("9. Transaction Value Z-score Outliers")
outliers = filtered_df[filtered_df["z_score"].abs() > 3]
st.dataframe(outliers[["id_usuario_pagador", "valor_transacao", "z_score"]].head(10))

# 10
st.subheader("10. Geographic Distance vs Approval")
distance_vs_approval = filtered_df.groupby(["distance_bucket", "transacao_aprovada"], observed=False).size().unstack(fill_value=0)
st.bar_chart(distance_vs_approval)
