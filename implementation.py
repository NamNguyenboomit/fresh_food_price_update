import streamlit as st
import pandas as pd
import psycopg2
from io import BytesIO
from datetime import date

# -----------------------------
# Database Connection Settings
# -----------------------------
def get_connection():
    return psycopg2.connect(
        host=st.secrets["supabase"]["host"],
        port=st.secrets["supabase"]["port"],
        database=st.secrets["supabase"]["database"],
        user=st.secrets["supabase"]["user"],
        password=st.secrets["supabase"]["password"]
    )

# -----------------------------
# Upload Excel File → sku_actual_changes
# -----------------------------
def upload_to_staging(df):
    conn = get_connection()
    cur = conn.cursor()

    # Optional: clean before insert (good for weekly overwrite)
    cur.execute("TRUNCATE TABLE sku_actual_changes;")

    insert_query = """
        INSERT INTO sku_actual_changes (
            store_id, old_product_code, old_sku_code,
            new_product_code, new_barcode, new_price,
            valid_from, valid_to
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
    """

    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row.get('store_id'),
            row.get('old_product_code'),
            row.get('old_sku_code'),
            row.get('new_product_code'),
            row.get('new_barcode'),
            row.get('new_price'),
            pd.to_datetime(row.get('valid_from')).date() if not pd.isna(row.get('valid_from')) else None,
            pd.to_datetime(row.get('valid_to')).date() if not pd.isna(row.get('valid_to')) else None
        ))

    conn.commit()
    cur.close()
    conn.close()

# -----------------------------
# Run Stored Procedure
# -----------------------------
def run_procedure(valid_from, valid_to):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("CALL process_sku_weekly_changes(%s, %s);", (valid_from, valid_to))
    conn.commit()
    cur.close()
    conn.close()

# -----------------------------
# Fetch Updated sku_master
# -----------------------------
def fetch_sku_master():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM sku_master ORDER BY store, barcode;", conn)
    conn.close()
    return df

# -----------------------------
# Generic Table Loader
# -----------------------------
def load_table(table_name):
    with get_connection() as conn:
        return pd.read_sql(f"SELECT * FROM {table_name} ORDER BY 1 DESC LIMIT 100;", conn)

# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="SKU Weekly Update", layout="wide")
st.title("🧾 SKU Weekly Update Processor")

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

