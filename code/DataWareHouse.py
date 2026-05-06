# %% [markdown]
#  # Data Engineering - Great Outdoors (DWH ETL)
#
#  Dit notebook bouwt een Data Warehouse (Stermodel) om Knelpunt 1, 2 en 6 op te lossen.
#
#
#
#  **Doelen per knelpunt:**
#
#  * **Knelpunt 1 (Verkopen & Klantgedrag):** `fact_sales`, `dim_customer`, `dim_product`, `dim_date`. Bevat ordernummers voor Market Basket analyse in Power BI.
#
#  * **Knelpunt 2 (Retourstromen):** `fact_returns`, gekoppeld aan `dim_return_reason`, `dim_product` en `dim_customer` (voor locaties).
#
#  * **Knelpunt 6 (Doelen vs Realisatie):** `fact_targets` vergelijken met `fact_sales`, geaggregeerd via `dim_sales_staff`, `dim_product` en `dim_date`.

# %% [markdown]
#  ### 1. Imports en Connecties

# %%
import sqlite3
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# Database locaties
db_sdm = "../data/SDM.db"
db_dwh = "../data/DWH.db"

# Connecties opzetten
conn_sdm = sqlite3.connect(db_sdm)
conn_dwh = sqlite3.connect(db_dwh)

print("Verbonden met SDM. Data extraheren...")


# %% [markdown]
#  ### 2. Data Extractie uit Source Data Model (SDM)
#
#  We laden de tabellen in als Pandas DataFrames zodat we ze makkelijk kunnen transformeren. Alle waarden in de SDM zijn TEXT, dus we moeten straks datatypes omzetten.

# %%
# Product & Dimensies
df_product = pd.read_sql("SELECT * FROM product", conn_sdm)
df_product_type = pd.read_sql("SELECT * FROM product_type", conn_sdm)
df_product_line = pd.read_sql("SELECT * FROM product_line", conn_sdm)

# Klant & Locaties
df_customer = pd.read_sql("SELECT * FROM customer", conn_sdm)
df_hq = pd.read_sql("SELECT * FROM customer_headquarters", conn_sdm)
df_segment = pd.read_sql("SELECT * FROM customer_segment", conn_sdm)
df_country = pd.read_sql("SELECT * FROM country", conn_sdm)
df_retailer_site = pd.read_sql("SELECT * FROM retailer_site", conn_sdm)

# Medewerkers
df_staff = pd.read_sql("SELECT * FROM sales_staff", conn_sdm)
df_branch = pd.read_sql("SELECT * FROM sales_branch", conn_sdm)

# Verkopen (Knelpunt 1 & 6)
df_order_header = pd.read_sql("SELECT * FROM order_header", conn_sdm)
df_order_details = pd.read_sql("SELECT * FROM order_details", conn_sdm)

# Retouren (Knelpunt 2)
df_returns = pd.read_sql("SELECT * FROM returned_item", conn_sdm)
df_return_reason = pd.read_sql("SELECT * FROM return_reason", conn_sdm)

# Doelstellingen (Knelpunt 6)
df_targets = pd.read_sql("SELECT * FROM sales_target", conn_sdm)


# %% [markdown]
#  ### 3. Dimensietabellen Bouwen (Transformaties)

# %%
print("Dimensies genereren...")

# --- DIM_PRODUCT ---
dim_product = df_product.merge(df_product_type, on='PRODUCT_TYPE_CODE', how='left')
dim_product = dim_product.merge(df_product_line, on='PRODUCT_LINE_CODE', how='left')

# Datatypes voor berekeningen
dim_product['PRODUCTION_COST'] = pd.to_numeric(dim_product['PRODUCTION_COST'], errors='coerce')
dim_product['MARGIN'] = pd.to_numeric(dim_product['MARGIN'], errors='coerce')

dim_product = dim_product[['PRODUCT_NUMBER', 'PRODUCT_NAME', 'PRODUCT_TYPE_EN', 'PRODUCT_LINE_EN', 'PRODUCTION_COST', 'MARGIN']]
dim_product.rename(columns={'PRODUCT_TYPE_EN': 'PRODUCT_TYPE', 'PRODUCT_LINE_EN': 'PRODUCT_LINE'}, inplace=True)


# --- DIM_CUSTOMER ---
# We koppelen sites aan de hoofdklant (headquarters) om regio en segment te krijgen
dim_customer = df_retailer_site[['RETAILER_SITE_CODE', 'RETAILER_CODE', 'CITY', 'REGION', 'COUNTRY_CODE']].copy()
dim_customer = dim_customer.merge(df_customer[['CUSTOMER_CODE', 'COMPANY_NAME', 'CUSTOMER_CODEMR']],
                                  left_on='RETAILER_CODE', right_on='CUSTOMER_CODE', how='left')
dim_customer = dim_customer.merge(df_hq[['CUSTOMER_CODEMR', 'SEGMENT_CODE']], on='CUSTOMER_CODEMR', how='left')
dim_customer = dim_customer.merge(df_segment[['SEGMENT_CODE', 'SEGMENT_NAME']], on='SEGMENT_CODE', how='left')
dim_customer = dim_customer.merge(df_country[['COUNTRY_CODE', 'COUNTRY']], on='COUNTRY_CODE', how='left')

dim_customer = dim_customer[['RETAILER_SITE_CODE', 'COMPANY_NAME', 'SEGMENT_NAME', 'CITY', 'REGION', 'COUNTRY']]


# --- DIM_SALES_STAFF ---
dim_sales_staff = df_staff.merge(df_branch, on='SALES_BRANCH_CODE', how='left')
dim_sales_staff = dim_sales_staff.merge(df_country, on='COUNTRY_CODE', how='left')
dim_sales_staff['STAFF_NAME'] = dim_sales_staff['FIRST_NAME'] + ' ' + dim_sales_staff['LAST_NAME']

dim_sales_staff = dim_sales_staff[['SALES_STAFF_CODE', 'STAFF_NAME', 'POSITION_EN', 'CITY', 'REGION', 'COUNTRY']]
dim_sales_staff.rename(columns={'POSITION_EN': 'POSITION', 'CITY': 'BRANCH_CITY', 'COUNTRY': 'BRANCH_COUNTRY'}, inplace=True)


# --- DIM_RETURN_REASON ---
dim_return_reason = df_return_reason[['RETURN_REASON_CODE', 'RETURN_DESCRIPTION_EN']].copy()
dim_return_reason.rename(columns={'RETURN_DESCRIPTION_EN': 'RETURN_REASON'}, inplace=True)


# %% [markdown]
#  ### 4. Feitentabellen Bouwen (Transformaties)

# %%
print("Feitentabellen genereren...")

# --- FACT_SALES (Knelpunt 1 & 6) ---
fact_sales = df_order_details.merge(df_order_header, on='ORDER_NUMBER', how='inner')

# Datatypes fixen
num_cols = ['QUANTITY', 'UNIT_COST', 'UNIT_PRICE', 'UNIT_SALE_PRICE']
for col in num_cols:
    fact_sales[col] = pd.to_numeric(fact_sales[col], errors='coerce')

fact_sales['ORDER_DATE'] = pd.to_datetime(fact_sales['ORDER_DATE'], errors='coerce')

# Berekende kolommen voor Power BI
fact_sales['REVENUE'] = fact_sales['QUANTITY'] * fact_sales['UNIT_SALE_PRICE']
fact_sales['TOTAL_COST'] = fact_sales['QUANTITY'] * fact_sales['UNIT_COST']
fact_sales['PROFIT'] = fact_sales['REVENUE'] - fact_sales['TOTAL_COST']

# Selecteer alleen keys en metrics
fact_sales = fact_sales[['ORDER_DETAIL_CODE', 'ORDER_NUMBER', 'ORDER_DATE', 'PRODUCT_NUMBER',
                         'RETAILER_SITE_CODE', 'SALES_STAFF_CODE', 'ORDER_METHOD_CODE',
                         'QUANTITY', 'UNIT_SALE_PRICE', 'REVENUE', 'TOTAL_COST', 'PROFIT']]


# --- FACT_RETURNS (Knelpunt 2) ---
# Retouren bevatten geen retailer direct in de tabel, dus we halen die via order_details en order_header
fact_returns = df_returns.merge(df_order_details[['ORDER_DETAIL_CODE', 'ORDER_NUMBER', 'PRODUCT_NUMBER']],
                                on='ORDER_DETAIL_CODE', how='inner')
fact_returns = fact_returns.merge(df_order_header[['ORDER_NUMBER', 'RETAILER_SITE_CODE']],
                                  on='ORDER_NUMBER', how='inner')

fact_returns['RETURN_QUANTITY'] = pd.to_numeric(fact_returns['RETURN_QUANTITY'], errors='coerce')
fact_returns['RETURN_DATE'] = pd.to_datetime(fact_returns['RETURN_DATE'], errors='coerce')

fact_returns = fact_returns[['RETURN_CODE', 'RETURN_DATE', 'ORDER_DETAIL_CODE', 'PRODUCT_NUMBER',
                             'RETAILER_SITE_CODE', 'RETURN_REASON_CODE', 'RETURN_QUANTITY']]


# --- FACT_TARGETS (Knelpunt 6) ---
fact_targets = df_targets.copy()
fact_targets['SALES_TARGET'] = pd.to_numeric(fact_targets['SALES_TARGET'], errors='coerce')

# Voor het stermodel moeten doelen aan de tijd-dimensie gekoppeld worden.
# Een doel is per maand ("SALES_PERIOD"). We zetten dit om naar de 1e dag van die maand.
fact_targets['TARGET_DATE'] = pd.to_datetime(
    fact_targets['SALES_YEAR'].astype(str) + '-' + fact_targets['SALES_PERIOD'].astype(str).str.zfill(2) + '-01',
    errors='coerce'
)

fact_targets = fact_targets[['SALES_STAFF_CODE', 'PRODUCT_NUMBER', 'TARGET_DATE', 'SALES_TARGET']]


# %%
from datetime import datetime
import os

def compare_values(a, b):
    """Vergelijk twee waarden, met speciale behandeling voor floats en NaN."""
    # Beide NaN = gelijk
    try:
        if pd.isna(a) and pd.isna(b):
            return True
    except (TypeError, ValueError):
        pass

    try:
        return round(float(a), 6) == round(float(b), 6)
    except (ValueError, TypeError):
        return str(a).strip() == str(b).strip()

def process_scd2(df_source, table_name, conn_dwh, business_key):
    vandaag = datetime.now().strftime('%Y-%m-%d')
    oneindig = '9999-12-31'

    # 1. Probeer de huidige data uit het DWH te laden
    try:
        df_dwh = pd.read_sql(f"SELECT * FROM {table_name}", conn_dwh)
        tabel_bestaat = True
        print("tabel_bestaat")
    except:
        df_dwh = pd.DataFrame()
        tabel_bestaat = False
        print("tabel_bestaat_niet")

    if not tabel_bestaat or df_dwh.empty:
        # --- INITIAL LOAD ---
        df_source = df_source.copy()
        df_source['Valid_From'] = vandaag
        df_source['Valid_To'] = oneindig
        df_source['Is_Active'] = 1
        df_source.insert(0, f"{table_name}_sk", range(1, len(df_source) + 1))

        df_source.to_sql(table_name, conn_dwh, if_exists='replace', index=False)
        print(f"Tabel {table_name} succesvol aangemaakt met SCD2 kolommen.")

    else:
        # --- INCREMENTAL LOAD (Type 2 logica) ---
        df_active = df_dwh[df_dwh['Is_Active'] == 1].copy()

        # Merge bron en doel
        df_merge = df_source.merge(df_active, on=business_key, how='left', suffixes=('', '_old'))

        # Bepaal de te vergelijken kolommen: alle bronkolommen behalve de business key
        compare_cols = [c for c in df_source.columns if c != business_key]

        new_records = []
        updates_to_old_sk = []
        next_sk = int(df_dwh[f"{table_name}_sk"].max()) + 1

        for _, row in df_merge.iterrows():
            # Geval A: Compleet nieuw record (niet gevonden in DWH)
            if pd.isna(row.get('Is_Active')):
                new_row = row[df_source.columns].copy()
                new_row[f"{table_name}_sk"] = next_sk
                new_row['Valid_From'] = vandaag
                new_row['Valid_To'] = oneindig
                new_row['Is_Active'] = 1
                new_records.append(new_row)
                next_sk += 1

            else:
                # Geval B: Check of ENIGE kolom gewijzigd is
                is_changed = any(
                    not compare_values(row[col], row[f"{col}_old"])
                    for col in compare_cols
                    if f"{col}_old" in row.index
                )

                if is_changed:
                    updates_to_old_sk.append(row[f"{table_name}_sk"])

                    new_row = row[df_source.columns].copy()
                    new_row[f"{table_name}_sk"] = next_sk
                    new_row['Valid_From'] = vandaag
                    new_row['Valid_To'] = oneindig
                    new_row['Is_Active'] = 1
                    new_records.append(new_row)
                    next_sk += 1

        # Oude records sluiten
        if updates_to_old_sk:
            df_dwh.loc[df_dwh[f"{table_name}_sk"].isin(updates_to_old_sk), 'Valid_To'] = vandaag
            df_dwh.loc[df_dwh[f"{table_name}_sk"].isin(updates_to_old_sk), 'Is_Active'] = 0

        # Nieuwe rijen toevoegen
        if new_records:
            df_final = pd.concat([df_dwh, pd.DataFrame(new_records)], ignore_index=True)
            df_final.to_sql(table_name, conn_dwh, if_exists='replace', index=False)
            print(f"SCD2 update uitgevoerd voor {table_name}: {len(new_records)} wijzigingen.")
            print(pd.DataFrame(new_records).to_string())
        else:
            print(f"Geen wijzigingen gevonden voor {table_name}.")

# %% [markdown]
#  ### 5. Inladen in Data Warehouse (DWH.db)

# %%
print("Opslaan in DWH.db...")

# Functie om overschrijven veilig te doen
def save_to_dwh(df, table_name):
    df.to_sql(table_name, conn_dwh, if_exists='replace', index=False)




# Dimensies opslaan
process_scd2(dim_product, 'dim_product', conn_dwh, 'PRODUCT_NUMBER')
process_scd2(dim_customer, 'dim_customer', conn_dwh, 'RETAILER_SITE_CODE')
save_to_dwh(dim_sales_staff, 'dim_sales_staff')
save_to_dwh(dim_return_reason, 'dim_return_reason')

# Feiten opslaan
save_to_dwh(fact_sales, 'fact_sales')
save_to_dwh(fact_returns, 'fact_returns')
save_to_dwh(fact_targets, 'fact_targets')

# Connecties sluiten
conn_sdm.close()
conn_dwh.close()

print("Succes! DWH.db is aangemaakt. Laad dit bestand in Power BI.")


# %% [markdown]
#  ### Hoe pas je dit toe in Power BI?
#
#  1. Open Power BI Desktop.
#
#  2. Kies Get Data -> ODBC (of via een Python Script / directe SQLite connector).
#
#  3. Laad de `dim_` en `fact_` tabellen in.
#
#  4. **Model Weergave:**
#
#     - Maak een nieuwe (Dax) Date Table in Power BI en koppel deze aan `fact_sales[ORDER_DATE]`, `fact_returns[RETURN_DATE]` en `fact_targets[TARGET_DATE]`.
#
#     - Koppel de `dim_` tabellen aan de bijbehorende `fact_` tabellen via de codes (`PRODUCT_NUMBER`, `RETAILER_SITE_CODE`, etc.).
#
#  5. **Visualisaties:**
#
#     - *Knelpunt 1:* Gebruik Revenue uit `fact_sales` gefilterd per regio (uit `dim_customer`) of datum. Voor combo's, plaats in DAX `ORDER_NUMBER` op de as of filter.
#
#     - *Knelpunt 2:* Plot `RETURN_QUANTITY` uit `fact_returns` tegen `dim_return_reason` en de `REGION` van `dim_customer`.
#
#     - *Knelpunt 6:* Maak een matrix met `dim_sales_staff[STAFF_NAME]` of `dim_product[PRODUCT_LINE]`. Zet `fact_sales[REVENUE]` naast `fact_targets[SALES_TARGET]`.


