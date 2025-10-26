import streamlit as st
import pandas as pd
import psycopg2
from io import BytesIO
from datetime import date

# -----------------------------
# Database Connection Settings
# -----------------------------
def get_connection():
    conn = psycopg2.connect(
        host=st.secrets["postgres"]["host"],
        port=st.secrets["postgres"]["port"],
        dbname=st.secrets["postgres"]["dbname"],
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"]
    )
    return conn

# -----------------------------
# Upload Excel File → sku_actual_changes
# -----------------------------
def upload_to_staging(df):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE sku_actual_changes;")  # clear before insert

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO sku_actual_changes (
                store_id, old_product_code, old_sku_code,
                new_product_code, new_barcode, new_price,
                valid_from, valid_to
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            row['store_id'], row['old_product_code'], row['old_sku_code'],
            row['new_product_code'], row['new_barcode'], row['new_price'],
            row['valid_from'], row['valid_to']
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
    df = pd.read_sql("SELECT * FROM sku_master;", conn)
    conn.close()
    return df

# -----------------------------
# Streamlit UI
# -----------------------------
st.title("🧾 SKU Weekly Update Processor")

# Allow both Excel and CSV upload
uploaded_file = st.file_uploader("Upload SKU Actual Changes File", type=["xlsx", "csv"])

valid_from = st.date_input("Valid From", date.today())
valid_to = st.date_input("Valid To", date.today())

if uploaded_file is not None:
    # Detect file type and read accordingly
    file_name = uploaded_file.name.lower()
    if file_name.endswith(".csv"):
        df_uploaded = pd.read_csv(uploaded_file)
    else:
        df_uploaded = pd.read_excel(uploaded_file)

    st.write("Preview of Uploaded Data:", df_uploaded.head())

    if st.button("Run Update Procedure"):
        try:
            # Your existing functions
            upload_to_staging(df_uploaded)
            run_procedure(valid_from, valid_to)
            st.success("Procedure executed successfully!")

            df_master = fetch_sku_master()
            st.write("Updated SKU Master Table:", df_master.head())

            # Always export final result as Excel
            output = BytesIO()
            df_master.to_excel(output, index=False)
            st.download_button(
                label="Download Updated SKU Master",
                data=output.getvalue(),
                file_name="sku_master_updated.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            st.error(f"Error: {e}")