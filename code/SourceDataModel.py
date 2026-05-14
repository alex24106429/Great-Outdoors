# %% [markdown]
#    # 1. Imports

# %%
import sqlite3
import os
import csv

# %% [markdown]
#    # 2. Laad de sqlite bestanden in

# %%
# BEFORE (fragile relative paths):
db_sdm = "../data/SDM.db"
db_crm = "../data/CRM-data.sqlite"
# ...

# AFTER (absolute paths relative to the script file):

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")

db_sdm = os.path.join(DATA_DIR, "SDM.db")
db_crm  = os.path.join(DATA_DIR, "CRM-data.sqlite")
db_gosales = os.path.join(DATA_DIR, "GO_SALES-data.sqlite")
db_gostaff = os.path.join(DATA_DIR, "GO_STAFF-data.sqlite")
csv_inventory   = os.path.join(DATA_DIR, "INVENTORY_LEVELS-data.csv")
csv_forecast    = os.path.join(DATA_DIR, "PRODUCT_FORECAST-data.csv")
csv_sales_target = os.path.join(DATA_DIR, "SALES_TARGET-data.csv")

# Controleer of het bestand al bestaat
db_exists = os.path.exists(db_sdm)

if db_exists:
    print(f"'{db_sdm}' bestaat al. Het bestand wordt niet verwijderd, maar de tabellen worden leeggemaakt...")
else:
    print(f"Aanmaken van de nieuwe '{db_sdm}' database...")

conn = sqlite3.connect(db_sdm)
cursor = conn.cursor()

# We zetten foreign keys tijdelijk uit tijdens het inladen van de data
# om insert errors door 'vuile' bron data te voorkomen.
cursor.execute("PRAGMA foreign_keys = OFF;")

# =========================================================================
# 1. DDL SCRIPT (Data Definition Language)
# Bevat alle originele tabellen + Cross-Database Associaties
# Gesorteerd op dependency order (basis tabellen eerst)
# =========================================================================
ddl_script = """
-- === BASIS TABELLEN (Geen Foreign Keys) ===

CREATE TABLE "age_group" (
    [AGE_GROUP_CODE] TEXT, [UPPER_AGE] TEXT, [LOWER_AGE] TEXT,
    PRIMARY KEY ([AGE_GROUP_CODE])
);

CREATE TABLE "customer_segment" (
    [SEGMENT_CODE] TEXT, [LANGUAGE] TEXT, [SEGMENT_NAME] TEXT, [SEGMENT_DESCRIPTION] TEXT,
    PRIMARY KEY ([SEGMENT_CODE])
);

CREATE TABLE "customer_type" (
    [CUSTOMER_TYPE_CODE] TEXT, [CUSTOMER_TYPE_EN] TEXT,
    PRIMARY KEY ([CUSTOMER_TYPE_CODE])
);

CREATE TABLE "sales_territory" (
    [SALES_TERRITORY_CODE] TEXT, [TERRITORY_NAME_EN] TEXT,
    PRIMARY KEY ([SALES_TERRITORY_CODE])
);

CREATE TABLE "country" (
    [COUNTRY_CODE] TEXT, [COUNTRY] TEXT, [LANGUAGE] TEXT, [CURRENCY_NAME] TEXT,
    PRIMARY KEY ([COUNTRY_CODE])
);

CREATE TABLE "order_method" (
    [ORDER_METHOD_CODE] TEXT, [ORDER_METHOD_EN] TEXT,
    PRIMARY KEY ([ORDER_METHOD_CODE])
);

CREATE TABLE "product_line" (
    [PRODUCT_LINE_CODE] TEXT, [PRODUCT_LINE_EN] TEXT,
    PRIMARY KEY ([PRODUCT_LINE_CODE])
);

CREATE TABLE "return_reason" (
    [RETURN_REASON_CODE] TEXT, [RETURN_DESCRIPTION_EN] TEXT,
    PRIMARY KEY ([RETURN_REASON_CODE])
);

CREATE TABLE "course" (
    [COURSE_CODE] TEXT, [COURSE_DESCRIPTION] TEXT,
    PRIMARY KEY ([COURSE_CODE])
);

CREATE TABLE "satisfaction_type" (
    [SATISFACTION_TYPE_CODE] TEXT, [SATISFACTION_TYPE_DESCRIPTION] TEXT,
    PRIMARY KEY ([SATISFACTION_TYPE_CODE])
);

-- === NIVEAU 1 TABELLEN ===

CREATE TABLE "crm_country" (
    "COUNTRY_CODE" TEXT, "COUNTRY_EN" TEXT, "FLAG_IMAGE" TEXT, "SALES_TERRITORY_CODE" TEXT,
    PRIMARY KEY("COUNTRY_CODE"),
    FOREIGN KEY("SALES_TERRITORY_CODE") REFERENCES "sales_territory"("SALES_TERRITORY_CODE")
);

CREATE TABLE "customer_headquarters" (
    "CUSTOMER_CODEMR" TEXT, "CUSTOMER_NAME" TEXT, "ADDRESS1" TEXT, "ADDRESS2" TEXT,
    "CITY" TEXT, "REGION" TEXT, "POSTAL_ZONE" TEXT, "COUNTRY_CODE" TEXT,
    "PHONE" TEXT, "FAX" TEXT, "SEGMENT_CODE" TEXT,
    PRIMARY KEY("CUSTOMER_CODEMR"),
    FOREIGN KEY("SEGMENT_CODE") REFERENCES "customer_segment"("SEGMENT_CODE")
);

CREATE TABLE "product_type" (
    [PRODUCT_TYPE_CODE] TEXT, [PRODUCT_LINE_CODE] TEXT, [PRODUCT_TYPE_EN] TEXT,
    PRIMARY KEY ([PRODUCT_TYPE_CODE]),
    FOREIGN KEY ([PRODUCT_LINE_CODE]) REFERENCES [product_line]([PRODUCT_LINE_CODE]) ON DELETE RESTRICT
);

CREATE TABLE "sales_branch" (
    "SALES_BRANCH_CODE" TEXT, "ADDRESS1" TEXT, "ADDRESS2" TEXT, "CITY" TEXT,
    "REGION" TEXT, "POSTAL_ZONE" TEXT, "COUNTRY_CODE" TEXT,
    PRIMARY KEY("SALES_BRANCH_CODE"),
    FOREIGN KEY("COUNTRY_CODE") REFERENCES "country"("COUNTRY_CODE") ON DELETE RESTRICT
);

CREATE TABLE "sales_office" (
    [SALES_OFFICE_CODE] TEXT, [STREET] TEXT, [ADDITION] TEXT, [CITY] TEXT,
    [REGION] TEXT, [ZIPCODE] TEXT, [COUNTRY_CODE] TEXT,
    PRIMARY KEY ([SALES_OFFICE_CODE]),
    -- CROSS-DB FK:
    FOREIGN KEY([COUNTRY_CODE]) REFERENCES "country"("COUNTRY_CODE")
);

-- === NIVEAU 2 TABELLEN ===

CREATE TABLE "customer" (
    "CUSTOMER_CODE" TEXT, "CUSTOMER_CODEMR" TEXT, "COMPANY_NAME" TEXT, "CUSTOMER_TYPE_CODE" TEXT,
    PRIMARY KEY("CUSTOMER_CODE"),
    FOREIGN KEY("CUSTOMER_CODEMR") REFERENCES "customer_headquarters"("CUSTOMER_CODEMR"),
    FOREIGN KEY("CUSTOMER_TYPE_CODE") REFERENCES "customer_type"("CUSTOMER_TYPE_CODE")
);

CREATE TABLE "sales_demographic" (
    "DEMOGRAPHIC_CODE" TEXT, "CUSTOMER_CODEMR" TEXT, "AGE_GROUP_CODE" TEXT, "SALES_PERCENT" TEXT,
    FOREIGN KEY("AGE_GROUP_CODE") REFERENCES "age_group"("AGE_GROUP_CODE") ON DELETE CASCADE,
    FOREIGN KEY("CUSTOMER_CODEMR") REFERENCES "customer_headquarters"("CUSTOMER_CODEMR") ON DELETE CASCADE
);

CREATE TABLE "product" (
    [PRODUCT_NUMBER] TEXT, [INTRODUCTION_DATE] TEXT, [PRODUCT_TYPE_CODE] TEXT, [PRODUCTION_COST] TEXT,
    [MARGIN] TEXT, [PRODUCT_IMAGE] TEXT, [LANGUAGE] TEXT, [PRODUCT_NAME] TEXT, [DESCRIPTION] TEXT,
    PRIMARY KEY ([PRODUCT_NUMBER]),
    FOREIGN KEY ([PRODUCT_TYPE_CODE]) REFERENCES [product_type]([PRODUCT_TYPE_CODE]) ON DELETE RESTRICT
);

CREATE TABLE "sales_staff" (
    [SALES_STAFF_CODE] TEXT, [FIRST_NAME] TEXT, [LAST_NAME] TEXT, [POSITION_EN] TEXT, [WORK_PHONE] TEXT,
    [EXTENSION] TEXT, [FAX] TEXT, [EMAIL] TEXT, [DATE_HIRED] TEXT, [SALES_BRANCH_CODE] TEXT,
    PRIMARY KEY ([SALES_STAFF_CODE]),
    FOREIGN KEY ([SALES_BRANCH_CODE]) REFERENCES [sales_branch]([SALES_BRANCH_CODE]) ON DELETE RESTRICT
);

CREATE TABLE "sales_representative" (
    "SALES_REPRESENTATIVE_CODE" TEXT, "FIRST_NAME" TEXT, "LAST_NAME" TEXT, "POSITION_EN" TEXT,
    "WORK_PHONE" TEXT, "EXTENSION" TEXT, "FAX" TEXT, "EMAIL" TEXT, "DATE_HIRED" TEXT,
    "SALES_OFFICE_CODE" TEXT, "MANAGER_CODE" TEXT,
    PRIMARY KEY("SALES_REPRESENTATIVE_CODE"),
    FOREIGN KEY("MANAGER_CODE") REFERENCES "sales_representative"("SALES_REPRESENTATIVE_CODE"),
    FOREIGN KEY("SALES_OFFICE_CODE") REFERENCES "sales_office"("SALES_OFFICE_CODE")
);

-- === NIVEAU 3 TABELLEN ===

CREATE TABLE "customer_store" (
    "CUSTOMER_SITE_CODE" TEXT, "CUSTOMER_CODE" TEXT, "STREET" TEXT, "ADDITION" TEXT,
    "CITY" TEXT, "STATE" TEXT, "ZIPCODE" TEXT, "COUNTRY_CODE" TEXT, "ACTIVE_INDICATOR" TEXT,
    PRIMARY KEY("CUSTOMER_SITE_CODE"),
    FOREIGN KEY("COUNTRY_CODE") REFERENCES "crm_country"("COUNTRY_CODE"),
    FOREIGN KEY("CUSTOMER_CODE") REFERENCES "customer"("CUSTOMER_CODE")
);

CREATE TABLE "retailer_site" (
    [RETAILER_SITE_CODE] TEXT, [RETAILER_CODE] TEXT, [ADDRESS1] TEXT, [ADDRESS2] TEXT,
    [CITY] TEXT, [REGION] TEXT, [POSTAL_ZONE] TEXT, [COUNTRY_CODE] TEXT, [ACTIVE_INDICATOR] TEXT,
    PRIMARY KEY ([RETAILER_SITE_CODE]),
    -- CROSS-DB FKs:
    FOREIGN KEY([RETAILER_CODE]) REFERENCES "customer"("CUSTOMER_CODE"),
    FOREIGN KEY([COUNTRY_CODE]) REFERENCES "country"("COUNTRY_CODE")
);

CREATE TABLE "satisfaction" (
    "YEAR" TEXT, "SALES_REPRESENTATIVE_CODE" TEXT, "SATISFACTION_TYPE_CODE" TEXT,
    FOREIGN KEY ([SALES_REPRESENTATIVE_CODE]) REFERENCES [sales_representative]([SALES_REPRESENTATIVE_CODE]) ON DELETE CASCADE,
    FOREIGN KEY ([SATISFACTION_TYPE_CODE]) REFERENCES [satisfaction_type]([SATISFACTION_TYPE_CODE]) ON DELETE CASCADE
);

CREATE TABLE "training" (
    "YEAR" TEXT, "SALES_REPRESENTATIVE_CODE" TEXT, "COURSE_CODE" TEXT,
    FOREIGN KEY ([COURSE_CODE]) REFERENCES [course]([COURSE_CODE]) ON DELETE CASCADE,
    FOREIGN KEY ([SALES_REPRESENTATIVE_CODE]) REFERENCES [sales_representative]([SALES_REPRESENTATIVE_CODE]) ON DELETE CASCADE
);

-- voor INVENTORY_LEVELS-data.csv
CREATE TABLE "inventory_levels" (
    "INVENTORY_YEAR" TEXT, "INVENTORY_MONTH" TEXT, "PRODUCT_NUMBER" TEXT, "INVENTORY_COUNT" TEXT,
    PRIMARY KEY("INVENTORY_YEAR", "INVENTORY_MONTH", "PRODUCT_NUMBER"),
    FOREIGN KEY("PRODUCT_NUMBER") REFERENCES "product"("PRODUCT_NUMBER") ON DELETE CASCADE
);

-- voor PRODUCT_FORECAST-data.csv
CREATE TABLE "product_forecast" (
    "PRODUCT_NUMBER" TEXT, "YEAR" TEXT, "MONTH" TEXT, "EXPECTED_VOLUME" TEXT,
    PRIMARY KEY("PRODUCT_NUMBER", "YEAR", "MONTH"),
    FOREIGN KEY("PRODUCT_NUMBER") REFERENCES "product"("PRODUCT_NUMBER") ON DELETE CASCADE
);

-- voor SALES_TARGET-data.csv
CREATE TABLE "sales_target" (
    "SALES_STAFF_CODE" TEXT, "SALES_YEAR" TEXT, "SALES_PERIOD" TEXT,
    "RETAILER_NAME" TEXT, "PRODUCT_NUMBER" TEXT, "SALES_TARGET" TEXT, "RETAILER_CODE" TEXT,
    PRIMARY KEY("SALES_STAFF_CODE", "SALES_YEAR", "SALES_PERIOD", "PRODUCT_NUMBER", "RETAILER_CODE"),
    FOREIGN KEY("SALES_STAFF_CODE") REFERENCES "sales_staff"("SALES_STAFF_CODE") ON DELETE CASCADE,
    FOREIGN KEY("PRODUCT_NUMBER") REFERENCES "product"("PRODUCT_NUMBER") ON DELETE CASCADE,
    FOREIGN KEY("RETAILER_CODE") REFERENCES "customer"("CUSTOMER_CODE") ON DELETE CASCADE
);

-- === NIVEAU 4 TABELLEN ===

CREATE TABLE "customer_contact" (
    [CUSTOMER_CONTACT_CODE] TEXT, [CUSTOMER_SITE_CODE] TEXT, [FIRST_NAME] TEXT, [LAST_NAME] TEXT,
    [JOB_POSITION_EN] TEXT, [EXTENSION] TEXT, [FAX] TEXT, [E_MAIL] TEXT, [GENDER] TEXT,
    PRIMARY KEY ([CUSTOMER_CONTACT_CODE]),
    -- IMPLICIETE FK TOEGEVOEGD:
    FOREIGN KEY ([CUSTOMER_SITE_CODE]) REFERENCES "customer_store"("CUSTOMER_SITE_CODE")
);

-- === NIVEAU 5 TABELLEN ===

CREATE TABLE "order_header" (
    [ORDER_NUMBER] TEXT, [RETAILER_NAME] TEXT, [RETAILER_SITE_CODE] TEXT, [RETAILER_CONTACT_CODE] TEXT,
    [SALES_STAFF_CODE] TEXT, [SALES_BRANCH_CODE] TEXT, [ORDER_DATE] TEXT, [ORDER_METHOD_CODE] TEXT,
    PRIMARY KEY ([ORDER_NUMBER]),
    FOREIGN KEY ([ORDER_METHOD_CODE]) REFERENCES [order_method]([ORDER_METHOD_CODE]) ON DELETE RESTRICT,
    FOREIGN KEY ([RETAILER_SITE_CODE]) REFERENCES [retailer_site]([RETAILER_SITE_CODE]) ON DELETE RESTRICT,
    FOREIGN KEY ([SALES_BRANCH_CODE]) REFERENCES [sales_branch]([SALES_BRANCH_CODE]) ON DELETE RESTRICT,
    FOREIGN KEY ([SALES_STAFF_CODE]) REFERENCES [sales_staff]([SALES_STAFF_CODE]) ON DELETE RESTRICT,
    -- CROSS-DB FK:
    FOREIGN KEY ([RETAILER_CONTACT_CODE]) REFERENCES "customer_contact"("CUSTOMER_CONTACT_CODE")
);

-- === NIVEAU 6 TABELLEN ===

CREATE TABLE "order_details" (
    [ORDER_DETAIL_CODE] TEXT, [ORDER_NUMBER] TEXT, [PRODUCT_NUMBER] TEXT,
    [QUANTITY] TEXT, [UNIT_COST] TEXT, [UNIT_PRICE] TEXT, [UNIT_SALE_PRICE] TEXT,
    PRIMARY KEY ([ORDER_DETAIL_CODE]),
    FOREIGN KEY ([ORDER_NUMBER]) REFERENCES [order_header]([ORDER_NUMBER]) ON DELETE RESTRICT,
    FOREIGN KEY ([PRODUCT_NUMBER]) REFERENCES [product]([PRODUCT_NUMBER]) ON DELETE RESTRICT
);

-- === NIVEAU 7 TABELLEN ===

CREATE TABLE "returned_item" (
    [RETURN_CODE] TEXT, [RETURN_DATE] TEXT, [ORDER_DETAIL_CODE] TEXT, [RETURN_REASON_CODE] TEXT, [RETURN_QUANTITY] TEXT,
    PRIMARY KEY ([RETURN_CODE]),
    FOREIGN KEY ([ORDER_DETAIL_CODE]) REFERENCES [order_details]([ORDER_DETAIL_CODE]) ON DELETE RESTRICT,
    FOREIGN KEY ([RETURN_REASON_CODE]) REFERENCES [return_reason]([RETURN_REASON_CODE]) ON DELETE RESTRICT
);
"""

if not db_exists:
    # Voer het schema uit in de nieuwe database
    cursor.executescript(ddl_script)
    print("Schema succesvol aangemaakt.")
else:
    # Haal alle tabellen op in de bestaande database en maak ze leeg
    # (Dit pakt automatisch ook de nieuwe CSV-tabellen mee als ze al bestaan)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
    tables = cursor.fetchall()
    for table in tables:
        cursor.execute(f'DELETE FROM "{table[0]}";')
    print("Bestaande tabellen zijn succesvol leeggemaakt.")

# =========================================================================
# 2. DATA INLADEN VANUIT DE BRON DATABASES (SQLite)
# =========================================================================
print("Koppelen van de bron databases (SQLite)...")
cursor.execute(f"ATTACH DATABASE '{db_crm}' AS crm;")
cursor.execute(f"ATTACH DATABASE '{db_gosales}' AS gosales;")
cursor.execute(f"ATTACH DATABASE '{db_gostaff}' AS gostaff;")

# Lijst van (bron_alias, tabel_naam)
tabellen_mapping = [
    # CRM
    ("crm", "age_group"), ("crm", "customer_segment"), ("crm", "customer_type"),
    ("crm", "sales_territory"), ("crm", "crm_country"), ("crm", "customer_headquarters"),
    ("crm", "customer"), ("crm", "sales_demographic"), ("crm", "customer_store"),
    ("crm", "customer_contact"),
    # GO_SALES
    ("gosales", "country"), ("gosales", "order_method"), ("gosales", "product_line"),
    ("gosales", "return_reason"), ("gosales", "product_type"), ("gosales", "sales_branch"),
    ("gosales", "product"), ("gosales", "sales_staff"), ("gosales", "retailer_site"),
    ("gosales", "order_header"), ("gosales", "order_details"), ("gosales", "returned_item"),
    # GO_STAFF
    ("gostaff", "course"), ("gostaff", "satisfaction_type"), ("gostaff", "sales_office"),
    ("gostaff", "sales_representative"), ("gostaff", "satisfaction"), ("gostaff", "training")
]

print("Data kopiëren vanuit SQLite naar het Source Data Model...")
for db_alias, table in tabellen_mapping:
    try:
        cursor.execute(f"INSERT INTO main.{table} SELECT * FROM {db_alias}.{table};")
        # print(f"  - Data voor '{table}' succesvol gekopieerd.")
    except sqlite3.Error as e:
        print(f"Fout bij het kopiëren van '{table}' uit '{db_alias}': {e}")

# =========================================================================
# 3. DATA INLADEN VANUIT DE CSV BESTANDEN
# =========================================================================
print("Data kopiëren vanuit CSV bestanden naar het Source Data Model...")

# 3.1 Inladen: INVENTORY_LEVELS-data.csv
if os.path.exists(csv_inventory):
    try:
        with open(csv_inventory, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader) # Sla de header over
            cursor.executemany("INSERT INTO inventory_levels (INVENTORY_YEAR, INVENTORY_MONTH, PRODUCT_NUMBER, INVENTORY_COUNT) VALUES (?, ?, ?, ?)", reader)
        print(f"Data voor 'inventory_levels' succesvol gekopieerd vanuit {csv_inventory}.")
    except Exception as e:
        print(f"Fout bij het inladen van {csv_inventory}: {e}")
else:
    print(f"Waarschuwing: Bestand {csv_inventory} niet gevonden.")

# 3.2 Inladen: PRODUCT_FORECAST-data.csv
if os.path.exists(csv_forecast):
    try:
        with open(csv_forecast, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader) # Sla de header over
            cursor.executemany("INSERT INTO product_forecast (PRODUCT_NUMBER, YEAR, MONTH, EXPECTED_VOLUME) VALUES (?, ?, ?, ?)", reader)
        print(f"Data voor 'product_forecast' succesvol gekopieerd vanuit {csv_forecast}.")
    except Exception as e:
        print(f"Fout bij het inladen van {csv_forecast}: {e}")
else:
    print(f"Waarschuwing: Bestand {csv_forecast} niet gevonden.")

# 3.3 Inladen: SALES_TARGET-data.csv
if os.path.exists(csv_sales_target):
    try:
        with open(csv_sales_target, 'r', encoding='latin-1') as f:
            reader = csv.reader(f)
            next(reader) # Sla de header over
            cursor.executemany(
                "INSERT INTO sales_target (SALES_STAFF_CODE, SALES_YEAR, SALES_PERIOD, RETAILER_NAME, PRODUCT_NUMBER, SALES_TARGET, RETAILER_CODE) VALUES (?, ?, ?, ?, ?, ?, ?)",
                reader
            )
        print(f"Data voor 'sales_target' succesvol gekopieerd vanuit {csv_sales_target}.")
    except Exception as e:
        print(f"Fout bij het inladen van {csv_sales_target}: {e}")
else:
    print(f"Waarschuwing: Bestand {csv_sales_target} niet gevonden.")

# Opslaan en afsluiten
conn.commit()
conn.close()

print("\nProces afgerond! Het bestand 'SDM.db' is met succes gegenereerd/gevuld.")






