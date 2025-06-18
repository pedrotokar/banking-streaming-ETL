# dashboard.py
import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import matplotlib.pyplot as plt
from geopy.distance import geodesic

# --- Load Data ---
@st.cache_data
def load_csv_data():
    return pd.read_csv("data/fake_transactions.csv", parse_dates=["timestamp"])

@st.cache_data
def load_sqlite_data():
    conn = sqlite3.connect("data/fake_bank.db")
    df = pd.read_sql("SELECT * FROM transactions", conn, parse_dates=["timestamp"])
    conn.close()
    return df


# Choose source
st.sidebar.title("Data Source")
data_source = st.sidebar.radio("Select source:", ["CSV", "SQLite"], index=1)

if data_source == "CSV":
    df = load_csv_data()
else:
    df = load_sqlite_data()

# --- Sidebar Filters ---
st.sidebar.title("Filters")
st.sidebar.markdown("Filter by transaction type or time of day")
transaction_type_filter = st.sidebar.multiselect(
    "Transaction Type", df["transaction_type"].unique(), default=df["transaction_type"].unique()
)
time_range = st.sidebar.slider("Hour of Day", 0, 23, (0, 23))

df["hour"] = pd.to_datetime(df["timestamp"]).dt.hour
filtered_df = df[
    df["transaction_type"].isin(transaction_type_filter) &
    df["hour"].between(time_range[0], time_range[1])
]


# --- Main ---
st.title("Banking Transactions Dashboard")
st.markdown("Visualization of risk analysis, approvals, and user behavior")

# 1. General Approvals
st.subheader("1. General Approvals")
approval_counts = filtered_df["approved"].value_counts()
st.bar_chart(approval_counts)

# 2. Transaction Frequency by Hour
df["hour"] = df["timestamp"].dt.hour
transactions_by_hour = df.groupby("hour").size()
st.subheader("2. Transaction Frequency by Hour")
st.line_chart(transactions_by_hour)

# 3. Time Risk Score (t7)
st.subheader("3. Time Risk Score vs Approval")
df["t7_score"] = (df["timestamp"].dt.hour - 12) / 12
st.scatter_chart(df[["t7_score", "approved"]])

# 4. Geographic Distance Payer-Receiver vs Approval
def compute_distance(row):
    p1 = (row["latitude_payer"], row["longitude_payer"])
    p2 = (row["latitude_receiver"], row["longitude_receiver"])
    return geodesic(p1, p2).km

st.subheader("4. Geographic Distance vs Approval")
df["distance_km"] = df.apply(compute_distance, axis=1)

distance_bins = [0, 50, 300, 1000, 10000]
distance_labels = ["<50km", "50-300km", "300-1000km", ">1000km"]
df["distance_band"] = pd.cut(df["distance_km"], bins=distance_bins, labels=distance_labels)

distance_group = df.groupby(["distance_band", "approved"], observed=False).size().unstack(fill_value=0)
st.bar_chart(distance_group)

# 5. Transaction Type vs Rejections
st.subheader("5. Transaction Type vs Rejections")
rejections_by_type = df[df["approved"] == False].groupby("transaction_type").size()
st.bar_chart(rejections_by_type)

# 6. Transaction Frequency per User (Frequency Score)
st.subheader("6. Transaction Frequency per User")
df["rounded_hour"] = df["timestamp"].dt.floor("h")
frequency = df.groupby(["payer_id", "rounded_hour"]).size().reset_index(name="frequency")

frequency_conditions = [
    frequency["frequency"] <= 3,
    frequency["frequency"].between(4, 10),
    frequency["frequency"] > 10
]
frequency_scores = [0, 0.5, 1]
frequency["frequency_score"] = np.select(frequency_conditions, frequency_scores)

st.dataframe(frequency.sort_values(by="frequency", ascending=False).head(10))
