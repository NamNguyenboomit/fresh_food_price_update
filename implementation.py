import streamlit as st
import pandas as pd
from supabase import create_client, Client
from io import BytesIO
from datetime import date

# -----------------------------
# Initialize Supabase Client
# -----------------------------
@st.cache_resource
def init_connection() -> Client:
    url = st.secrets["supabase"]["url"]
    key = st.secrets["supabase"]["key"]
    return create_client(url, key)

supabase = init_connection()

# -----------------------------
# Upload Excel File → sku_actual_changes
# -----------------------------
def upload_to_staging(df):
    # Truncate table before inserting new data
    supabase.rpc("truncate_table", {"table_name": "sku_actual_changes"}).execute()

    # Convert dataframe to list of dictionaries for bulk insert
    data_to_insert = df.to_dict(orient="records")

    # Insert all rows into Supabase
    response = supabase.table("sku_actual_changes").insert(data_to_insert).execute()
    if response.error:
        raise Exception(response.error.message)

# -----------------------------
# Run Stored Procedure
# -----------------------------
def run_procedure(valid_from, valid_to):
    # Call your stored procedure on Supabase
    response = supabase.rpc("process_sku_weekly_changes", {
        "valid_from": str(valid_from),
        "valid_to": str(valid_to)
    }).execute()
    if response.error:
        raise Exception(response.error.message)

# -----------------------------
# Fetch Updated sku_master
# -----------------------------
def fetch_sku_master():
    response = supabase.table("sku_master").select("*").order("store_id").limit(1000).execute()
    if response.error:
        raise Exception(response.error.message)
    return pd.DataFrame(response.data)

# -----------------------------
# Generic Table Loader
# -----------------------------
def load_table(table_name):
    response = supabase.table(table_name).select("*").limit(2000).execute()

    # The response is an object with a .data attribute
    if hasattr(response, "data"):
        data = response.data
    else:
        st.warning(f"⚠️ Unexpected response format for {table_name}")
        return pd.DataFrame()

    if not data:
        st.warning(f"No data found in table: {table_name}")
        return pd.DataFrame()

    # Convert data to DataFrame
    df = pd.DataFrame(data)
    return df
# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="SKU Weekly Update", layout="wide")
st.title("🧾 SKU Weekly Update Processor (Supabase Version)")

uploaded_file = st.file_uploader("📤 Upload SKU Actual Changes File", type=["xlsx", "csv"])
valid_from = st.date_input("📅 Valid From", date.today())
valid_to = st.date_input("📅 Valid To", date.today())

if uploaded_file is not None:
    # Detect file type and read accordingly
    file_name = uploaded_file.name.lower()
    if file_name.endswith(".csv"):
        df_uploaded = pd.read_csv(uploaded_file)
    else:
        df_uploaded = pd.read_excel(uploaded_file)

    st.write("### Preview of Uploaded Data")
    st.dataframe(df_uploaded.head())

    if st.button("🚀 Run Update Procedure"):
        try:
            with st.spinner("Processing updates..."):
                upload_to_staging(df_uploaded)
                run_procedure(valid_from, valid_to)

                df_master = fetch_sku_master()
                st.success("✅ Procedure executed successfully!")
                st.write("### Updated SKU Master Table")
                st.dataframe(df_master.head())

                # Export final result as Excel
                output = BytesIO()
                df_master.to_excel(output, index=False)
                st.download_button(
                    label="📥 Download Updated SKU Master",
                    data=output.getvalue(),
                    file_name="sku_master_updated.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        except Exception as e:
            st.error(f"❌ Error: {e}")

# -----------------------------
# Tabs to view all tables
# -----------------------------
st.divider()
st.subheader("📊 Database Tables Overview")

tab1, tab2, tab3, tab4 = st.tabs(["SKU Master", "Actual Changes", "Transaction Log", "SKU Log"])

with tab1:
    st.dataframe(load_table("sku_master"))

with tab2:
    st.dataframe(load_table("sku_actual_changes"))

with tab3:
    st.dataframe(load_table("transaction_log"))

with tab4:
    st.dataframe(load_table("sku_log"))
