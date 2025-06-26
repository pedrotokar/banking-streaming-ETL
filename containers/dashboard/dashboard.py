# dashboard.py
import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, URL
from datetime import datetime, date, timezone, timedelta
from geopy.distance import geodesic
import plotly.graph_objects as go
import plotly.express as px
import os
import redis
import altair as alt
import json

st.set_page_config(
    page_title="Banking Transaction Dashboard (Live PostgreSQL Data)",
    initial_sidebar_state="expanded",
)

hide_streamlit_style = """
    <style>
        div[data-testid="stToolbar"] {
            visibility: hidden;
            height: 0%;
            position: fixed;
        }
        div[data-testid="stDecoration"] {
            visibility: hidden;
            height: 0%;
            position: fixed;
        }
        div[data-testid="stStatusWidget"] {
            visibility: hidden;
            height: 0%;
            position: fixed;
        }
        #MainMenu {
            visibility: hidden;
            height: 0%;
        }
        header {
            visibility: hidden;
            height: 0%;
        }
        footer {
            visibility: hidden;
            height: 0%;
        }
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

POSTGRES_DB       = os.getenv("POSTGRES_DB", "bank")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "bank_etl")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASS", "ihateavroformat123")
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
REDIS_CLIENT = get_redis_connection()

@st.cache_data(ttl=1)
def df_from_redis(_redis_client, count):
    """Carrega as transações mais recentes do Redis."""
    if not _redis_client:
        return pd.DataFrame()
    try:
        recent_ids = _redis_client.zrevrange("recent_transactions", 0, count -1)

        if not recent_ids:
            return pd.DataFrame()

        pipeline = _redis_client.pipeline()
        for transaction_id in recent_ids:
            pipeline.hgetall(f"transacoes:{transaction_id}")
        
        transaction_details = pipeline.execute()

        df = pd.DataFrame(transaction_details)
        df = df.reindex(sorted(df.columns), axis=1)
        
        if not df.empty:
            numeric_cols = ["valor_transacao", "latencia_total_ms", "tempo_processamento_ms"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            datetime_cols = ["data_horario", "tempo_saida_resultado", "tempo_entrada_kafka", "tempo_inicio_processamento"]
            for col in datetime_cols:
                 if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

            if "transacao_aprovada" in df.columns:
                df["transacao_aprovada"] = df["transacao_aprovada"].apply(lambda x: x.lower() == "true" if isinstance(x, str) else bool(x))
        return df

    except Exception as e:
        st.error(f"Erro ao buscar dados do Redis: {e}")
        return pd.DataFrame()

@st.cache_data(ttl="1")
def load_data():
    transactions = pd.read_sql("SELECT * FROM transacoes;", ENGINE, parse_dates=["data_horario"])
    transactions_scores = pd.read_sql("SELECT * FROM transacoes_scores;", ENGINE)

    if not transactions.empty:
        uuid_cols_t = ["id_transacao", "id_usuario_pagador", "id_usuario_recebedor"]
        for col in uuid_cols_t:
            if col in transactions.columns and transactions[col].dtype == 'object':
                transactions[col] = transactions[col].astype(str)

    if not transactions_scores.empty:
        if "id_transacao" in transactions_scores.columns and transactions_scores["id_transacao"].dtype == 'object':
            transactions_scores["id_transacao"] = transactions_scores["id_transacao"].astype(str)

    return transactions, transactions_scores

@st.cache_data
def load_data_constants():
    users = pd.read_sql("SELECT * FROM usuarios;", ENGINE)
    regions = pd.read_csv("data/regioes_estados_brasil.csv")

    if not users.empty:
        if "id_usuario" in users.columns and users["id_usuario"].dtype == 'object':
            users["id_usuario"] = users["id_usuario"].astype(str)

    return users, regions

# --- Preprocess Data ---
def preprocess_data(transactions, transactions_scores, users, regions):
    users = users.rename(columns={"id_regiao": "region_id_u"})
    transactions = transactions.rename(columns={"id_regiao": "region_id_t"})

    df = transactions.merge(users, left_on="id_usuario_pagador", right_on="id_usuario", how="left")

    df = df.merge(
        regions.rename(columns={"id_regiao": "region_id_t", "latitude": "lat_t", "longitude": "lon_t"}),
        on="region_id_t", how="left"
    ).merge(
        regions.rename(columns={"id_regiao": "region_id_u", "latitude": "lat_u", "longitude": "lon_u"}),
        on="region_id_u", how="left"
    ).merge(
        transactions_scores,
        on="id_transacao", how="left"
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
    stats = df.groupby("id_usuario_pagador")["valor_transacao"].agg(["mean", "std"]).reset_index()
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


# --- Load and Prepare ---
transactions, transactions_scores = load_data()
users, regions = load_data_constants()

if not transactions.empty:
    df = preprocess_data(transactions, transactions_scores, users, regions)
else:
    df = pd.DataFrame()

# --- Streamlit UI ---
st.sidebar.header("Redis Live View")
num_recent_transactions = st.sidebar.slider(
    "Recent Transactions",
    min_value=5,
    max_value=100,
    value=10,
    step=5
)

st.sidebar.divider()
st.sidebar.header("PostgreSQL Filters")

if not df.empty:
    selected_types = st.sidebar.multiselect("Payment Types", df["modalidade_pagamento"].unique(), default=list(df["modalidade_pagamento"].unique()))
    hour_range = st.sidebar.slider("Hour Range", 0, 23, (0, 23))

    filtered_df = df[
        (df["modalidade_pagamento"].isin(selected_types)) &
        (df["hour"].between(hour_range[0], hour_range[1]))
    ]
else:
    st.sidebar.warning("No historical data in PostgreSQL to filter.")
    filtered_df = pd.DataFrame()


today = date.today()
st.write(today)

# --- Analyses ---
last_mean_value = 0
last_mean_latency = 0

@st.fragment(run_every="2s")
def live():
    if REDIS_CLIENT:
        global last_mean_value, last_mean_latency
        
        df = df_from_redis(REDIS_CLIENT, num_recent_transactions)

        if df.empty:
            return

        col1, col2, col3 = st.columns(3)

        mean_value = df["valor_transacao"].mean()
        mean_latency = (df["latencia_total_ms"]).mean()


        mean_latency = (df["tempo_saida_resultado"] - df["data_horario"]).mean() / timedelta(milliseconds=1)
        
        with col1:
            st.metric(
                label=f"Latência média",
                value=f"{mean_latency :.2f} ms",
                delta=f"{mean_latency-last_mean_latency :.2f}",
                delta_color="inverse"
            )

        with col2:
            st.metric(
                label=f"Nº transações negadas",
                value=f"{(df["transacao_aprovada"] == 0).sum()}"
            )

        with col3:
            st.metric(
                label=f"Média últimas {len(df)} das transações",
                value=f"R$ {mean_value :.2f}",
                delta=f"{mean_value-last_mean_value :.2f}"
            )

        last_mean_value = mean_value

        last_mean_latency = mean_latency


        if not df.empty:
            st.subheader(f"Exibindo as {len(df)} Transações Mais Recentes")
            st.dataframe(df)
        else:
            st.info("Nenhum dado no Redis.")
    else:
        st.warning("Não foi possível conectar ao Redis.")


st.header("Live Transactions from Redis")
live()

st.divider()
st.header("Historical Analysis from PostgreSQL")

if filtered_df.empty:
    st.info("Não há dados no PostgreSQL.")

GREEN = "#2ecc71"
RED = "#e74c3c"

# 1 -------------------------------------------------------------------------
st.subheader("1. Visão Geral de Aprovação de Transações")
    
approval_counts = filtered_df["transacao_aprovada"].value_counts()
approved = approval_counts.get(True, 0)
rejected = approval_counts.get(False, 0)
total = approved + rejected

fig1 = go.Figure()
fig1.add_trace(go.Bar(
    y=["Status de Aprovação"],
    x=[rejected / total * 100],
    name=f"Rejeitadas ({rejected})",
    orientation='h',
    marker_color=RED,
    hovertemplate=f"Rejeitadas: {rejected} ({rejected/total*100:.1f}%)<extra></extra>",
))
fig1.add_trace(go.Bar(
    y=["Status de Aprovação"],
    x=[approved / total * 100],
    name=f"Aprovadas ({approved})",
    orientation='h',
    marker_color=GREEN,
    hovertemplate=f"Aprovadas: {approved} ({approved/total*100:.1f}%)<extra></extra>",
))
fig1.update_layout(
    barmode='stack',
    xaxis=dict(title="Percentage", range=[0, 100], ticksuffix="%"),
    yaxis=dict(showticklabels=False),
    height=200,
    legend=dict(orientation="h", yanchor="bottom", y=1.2, xanchor="center", x=0.5),
    margin=dict(l=20, r=20, t=20, b=20),
    showlegend=True,
)

st.plotly_chart(fig1, use_container_width=True)


# 2 -------------------------------------------------------------------------
st.subheader("2. Contagem de Transações vs Aprovação")

col_left, col_right = st.columns(2)

with col_left:
    st.caption("Por Valor da Transação (R$)")

    # Optionally trim out extreme outliers for clarity
    max_val = filtered_df["valor_transacao"].quantile(0.99)
    hist_df = filtered_df[filtered_df["valor_transacao"] <= max_val]

    approved_vals = hist_df[hist_df["transacao_aprovada"] == True]["valor_transacao"]
    rejected_vals = hist_df[hist_df["transacao_aprovada"] == False]["valor_transacao"]

    fig_val = go.Figure()

    fig_val.add_trace(go.Histogram(
        x=rejected_vals,
        nbinsx=50,
        name="Rejeitadas",
        marker_color=RED,
        opacity=0.6
    ))

    fig_val.add_trace(go.Histogram(
        x=approved_vals,
        nbinsx=50,
        name="Aprovadas",
        marker_color=GREEN,
        opacity=0.6
    ))

    fig_val.update_layout(
        barmode='overlay',
        xaxis_title="Valor da Transação (R$)",
        yaxis_title="Contagem",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(l=20, r=20, t=40, b=40),
        height=350
    )

    st.plotly_chart(fig_val, use_container_width=True)

with col_right:
    st.caption("Por Horário do Dia")

    # No filtering needed here
    approved_hours = filtered_df[filtered_df["transacao_aprovada"] == True]["hour"]
    rejected_hours = filtered_df[filtered_df["transacao_aprovada"] == False]["hour"]

    fig_hour = go.Figure()

    fig_hour.add_trace(go.Histogram(
        x=rejected_hours,
        xbins=dict(start=0, end=24, size=1),
        name="Rejeitadas",
        marker_color=RED,
        opacity=0.6
    ))

    fig_hour.add_trace(go.Histogram(
        x=approved_hours,
        xbins=dict(start=0, end=24, size=1),
        name="Aprovadas",
        marker_color=GREEN,
        opacity=0.6
    ))

    fig_hour.update_layout(
        barmode='overlay',
        xaxis=dict(title="Horário do Dia", tickmode="array", tickvals=list(range(24)), range=[-0.5, 23.5]),
        yaxis_title="Contagem",
        legend=dict(yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        margin=dict(l=20, r=20, t=40, b=40),
        height=350
    )

    st.plotly_chart(fig_hour, use_container_width=True)

# 3 -------------------------------------------------------------------------
st.subheader("3. Distribuição do Score de Risco por Faixa de Valor")
if "valor_transacao" in filtered_df.columns and "t5_score" in filtered_df.columns:
    bins = [0, 500, 1000, 2500, 5000, 10000, np.inf]
    labels = ["0-500", "500-1000", "1000-2500", "2500-5000", "5000-10000", ">10000"]
    plot_df = filtered_df[["valor_transacao", "t5_score", "transacao_aprovada"]].copy()
    plot_df["valor_bin"] = pd.cut(plot_df["valor_transacao"], bins=bins, labels=labels)

    fig_box = px.box(
        plot_df.dropna(subset=['valor_bin', 't5_score']),
        x="valor_bin",
        y="t5_score",
        color="transacao_aprovada",
        labels={
            "valor_bin": "Faixa de Valor da Transação (R$)",
            "t5_score": "Score de Risco (T5)",
            "transacao_aprovada": "Transação Aprovada?"
        },
        category_orders={"valor_bin": labels}
    )
    st.plotly_chart(fig_box, use_container_width=True, config={'displayModeBar': False})
else:
    st.warning("Colunas 'valor_transacao' ou 't5_score' não encontradas para gerar o gráfico.")

# 4 -------------------------------------------------------------------------
st.subheader("4. Mapa de Densidade: Concentração de Transações")
col_approved, col_denied = st.columns(2)
with col_approved:
    fig_approved = px.density_heatmap(
        filtered_df[filtered_df["transacao_aprovada"] == True],
        x="valor_transacao", y="t5_score", nbinsx=30, nbinsy=30,
        labels={"valor_transacao": "Valor", "t5_score": "Score"}
    )
    fig_approved.update_layout(
        coloraxis_showscale=False,
        title=dict(text="Transações Aprovadas", x=0.6, xanchor="center")
    )
    st.plotly_chart(fig_approved, use_container_width=True, config={"displayModeBar": False})

with col_denied:
    fig_denied = px.density_heatmap(
        filtered_df[filtered_df["transacao_aprovada"] == False],
        x="valor_transacao", y="t5_score", nbinsx=30, nbinsy=30,
        labels={"valor_transacao": "Valor", "t5_score": "Score"}
    )
    fig_denied.update_layout(
        coloraxis_showscale=False,
        title=dict(text="Transações Negadas", x=0.6, xanchor="center")
    )
    st.plotly_chart(fig_denied, use_container_width=True, config={"displayModeBar": False})

# 5 -------------------------------------------------------------------------
st.subheader("5. Score de Risco por Horário vs Aprovação de acordo com o horário")

df_plot = filtered_df.groupby("hour")[["time_score", "transacao_aprovada"]].mean().reset_index()

# Cria gráfico de barras para time_score
bar = alt.Chart(df_plot).mark_bar(opacity=0.6, color="#1f77b4").encode(
    x=alt.X("hour:O", axis=alt.Axis(labelAngle=0)),
    y=alt.Y("time_score:Q", title="Score Médio")
)

# Criar gráfico de linha para 'transacao_aprovada'
line = alt.Chart(df_plot).mark_line(color="#ff7f0e", strokeWidth=2).encode(
    x="hour:O",
    y=alt.Y("transacao_aprovada:Q", title="Taxa de Aprovação"),
)

# Combinar os dois
chart = (bar + line).properties(width=700, height=400)

st.altair_chart(chart, use_container_width=True)

# 6 -------------------------------------------------------------------------
st.subheader("6. Região vs Taxa de Aprovação")

df_region = (
    filtered_df.groupby("region_id_t")["transacao_aprovada"]
    .mean()
    .reset_index()
)

chart = alt.Chart(df_region).mark_bar().encode(
    x=alt.X("region_id_t:O", axis=alt.Axis(labelAngle=0), title="Região"),
    y=alt.Y("transacao_aprovada:Q", title="Taxa de Aprovação")
).properties(width=600, height=400)

st.altair_chart(chart, use_container_width=True)

approval_by_state = filtered_df.groupby("region_id_t", as_index=False)["transacao_aprovada"].mean()
approval_by_state.rename(columns={"region_id_t": "UF"}, inplace=True)

vmin = approval_by_state["transacao_aprovada"].min()
vmax = approval_by_state["transacao_aprovada"].max()

with open("data/br_states.json", "r", encoding="utf-8") as f:
    brazil_geojson = json.load(f)

fig_map = px.choropleth(
    approval_by_state,
    geojson=brazil_geojson,
    locations="UF",
    color="transacao_aprovada",
    color_continuous_scale="RdYlGn",
    range_color=(vmin, vmax),
    featureidkey="id",
    scope="south america",
    labels={"transacao_aprovada": "Taxa de Aprovação"},
    hover_name="UF",
    height=500
)
fig_map.update_geos(
    center={"lat": -14.2350, "lon": -51.9253},
    fitbounds="locations",
    visible=False
)
fig_map.update_layout(
    margin={"r":0, "t":0, "l":0, "b":0},
    coloraxis_colorbar=dict(title="Taxa de Aprovação", tickformat=".0%", ticks="outside")
)

st.plotly_chart(fig_map, use_container_width=True)


# 7 -------------------------------------------------------------------------
st.subheader("7. Negações por Saldo e Limite")

denial_counts = pd.DataFrame({
    "Limite": filtered_df["denied_by_limit"].sum(),
    "Saldo": filtered_df["denied_by_balance"].sum()
}, index=["Contagem"])

df_plot = denial_counts.T.reset_index()
df_plot.columns = ["Motivo", "Contagem"]

chart = alt.Chart(df_plot).mark_bar().encode(
    x=alt.X("Motivo:O", axis=alt.Axis(labelAngle=0)),
    y=alt.Y("Contagem:Q")
).properties(width=600, height=400)

st.altair_chart(chart, use_container_width=True)

# 8 -------------------------------------------------------------------------
st.subheader("8. Transações Negadas por Tipo de Pagamento")

denied_type_counts = (
    filtered_df[filtered_df["transacao_aprovada"] == False]
    .groupby("modalidade_pagamento")
    .size()
    .reset_index(name="count")
)

chart = alt.Chart(denied_type_counts).mark_bar().encode(
    x=alt.X("modalidade_pagamento:O", axis=alt.Axis(labelAngle=0), title="Tipo de Pagamento"),
    y=alt.Y("count:Q", title="Quantidade")
).properties(width=600, height=400)

st.altair_chart(chart, use_container_width=True)

# 9 -------------------------------------------------------------------------
st.subheader("9. Frequência de Transações por Hora")
st.line_chart(df.groupby("hour").size())

# 10 ------------------------------------------------------------------------
st.subheader("10. Distância Geográfica vs Aprovação")

distance_vs_approval = (
    filtered_df.groupby(["distance_bucket", "transacao_aprovada"], observed=False)
    .size()
    .unstack(fill_value=0)
    .reset_index()
)

distance_vs_approval.columns.name = None
distance_vs_approval = distance_vs_approval.rename(columns={True: "Aprovada", False: "Negada"})

df_long = distance_vs_approval.melt(id_vars="distance_bucket", var_name="Status", value_name="Contagem")

chart = alt.Chart(df_long).mark_bar().encode(
    x=alt.X("distance_bucket:O", sort=list(distance_vs_approval["distance_bucket"]), axis=alt.Axis(labelAngle=0), title="Faixa de Distância"),
    y=alt.Y("Contagem:Q", title="Quantidade"),
    color="Status:N"
).properties(width=600, height=400)

st.altair_chart(chart, use_container_width=True)

# # 8 -------------------------------------------------------------------------
# st.subheader("8. Score de Frequência vs Aprovação")
# st.line_chart(df.groupby("frequency_score")["transacao_aprovada"].mean())

# # 9 -------------------------------------------------------------------------
# st.subheader("9. Outliers de Valor de Transação (Z-score)")
# outliers = filtered_df[filtered_df["z_score"].abs() > 3]
# st.dataframe(outliers[["id_usuario_pagador", "valor_transacao", "z_score"]].head(10))

