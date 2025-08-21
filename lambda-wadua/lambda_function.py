import boto3
import pandas as pd
import uuid
import duckdb
import os
from io import StringIO

# ---------- CONFIGURACIÓN INICIAL CRÍTICA ----------
# Establecer HOME y DUCKDB_HOME antes de cualquier operación
os.environ['HOME'] = '/tmp'
os.environ['DUCKDB_HOME'] = '/tmp/duckdb_home'

# Crear los directorios necesarios
os.makedirs('/tmp/duckdb_home', exist_ok=True)

# ---------- EXTRACT ----------
def extract_data_from_s3():
    s3_client = boto3.client("s3")
    bucket_name = 'wadua-ecommerce-data'

    files_map = {
        "raw-data/Fecom Inc Customer List.csv": "customer",
        "raw-data/Fecom Inc Geolocations.csv": "geolocations",
        "raw-data/Fecom Inc Order Items.csv": "orders_items",
        "raw-data/Fecom Inc Order Payments.csv": "orders_payments",
        "raw-data/Fecom Inc Orders.csv": "orders",
        "raw-data/Fecom Inc Products.csv": "products",
        "raw-data/Fecom Inc Sellers List.csv": "sellers",
        "raw-data/Fecom_Inc_Order_Reviews_No_Emojis.csv": "orders_review"
    }

    dfs = {}
    for file_name, df_name in files_map.items():
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        data = obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(data), sep=';')
        dfs[df_name] = df

    return dfs

# ---------- TRANSFORM ----------
def transform_data(dfs):
    transformed = {}

    # CUSTOMER
    customer = dfs['customer']
    existing_ids = set(customer['Customer_Trx_ID'].dropna())

    def generate_unique_id(existing_ids):
        while True:
            new_id = uuid.uuid4().hex
            if new_id not in existing_ids:
                existing_ids.add(new_id)
                return new_id

    customer['Customer_Trx_ID'] = customer['Customer_Trx_ID'].apply(
        lambda x: generate_unique_id(existing_ids) if pd.isna(x) or str(x).lower() == 'nan' else x
    )

    customer = customer.drop_duplicates(subset=['Subscriber_ID'], keep='first')
    customer['First_Order_Date'] = customer['First_Order_Date'].fillna(customer['Subscribe_Date'])
    transformed['customer'] = customer

    # GEOLOCATIONS
    geolocations = dfs['geolocations']
    geolocations = geolocations.dropna()

    # Si las columnas tienen comas en lugar de puntos (en lat/lon), corrígelo antes (opcional)
    geolocations['Geo_Lat'] = geolocations['Geo_Lat'].astype(str).str.replace(',', '.').astype(float)
    geolocations['Geo_Lon'] = geolocations['Geo_Lon'].astype(str).str.replace(',', '.').astype(float)

    # Aplicar el código postal representativo a cada fila según la ciudad
    postal_by_city = geolocations.groupby("Geolocation_City")["Geo_Postal_Code"].agg(lambda x: x.mode().iloc[0])
    geolocations["Geo_Postal_Code_Mode"] = geolocations["Geolocation_City"].map(postal_by_city)

    geolocations = geolocations.drop_duplicates()

    transformed['geolocations'] = geolocations

    # PRODUCTS
    products = dfs['products']

    products['Product_Category_Name'] = products['Product_Category_Name'].fillna('unknown')

    for col in ['Product_Weight_Gr', 'Product_Length_Cm', 'Product_Height_Cm', 'Product_Width_Cm']:
        median_val = products[col].median()
        products[col] = products[col].fillna(median_val)

    transformed['products'] = products

    # SELLERS LIST

    sellers = dfs['sellers']
    transformed['sellers'] = sellers

    # ORDERS

    orders = dfs['orders']

    # Definir la fecha bandera
    date_flag = pd.Timestamp("1900-01-01")

    # Reemplazar nulos en las tres columnas de fecha
    orders = orders.fillna({
        'Order_Approved_At': date_flag,
        'Order_Delivered_Carrier_Date': date_flag,
        'Order_Delivered_Customer_Date': date_flag
    })

    orders = orders[orders["Customer_Trx_ID"].isin(customer["Customer_Trx_ID"])]

    transformed['orders'] = orders

    # ORDERS ITEMS

    orders_items = dfs['orders_items']
    orders_items = orders_items[orders_items["Order_ID"].isin(orders["Order_ID"])]
    transformed['orders_items'] = orders_items

    # ORDERS PAYMENTS

    orders_payments = dfs['orders_payments']
    orders_payments = orders_payments[orders_payments["Order_ID"].isin(orders["Order_ID"])]

    transformed['orders_payments'] = orders_payments

    # ORDERS REVIEW

    orders_reviews_no_emojis = dfs['orders_review']

    def fill_message(row):
        if pd.isna(row['Review_Comment_Message_En']):
            if row['Review_Score'] >= 4:
                return 'Positive review without comment'
            elif row['Review_Score'] == 3:
                return 'Neutral review without comment'
            else:
                return 'Negative review without comment'
        return row['Review_Comment_Message_En']

    orders_reviews_no_emojis['Review_Comment_Message_En'] = orders_reviews_no_emojis.apply(fill_message, axis=1)

    orders_reviews_no_emojis['Review_Comment_Title_En'] = orders_reviews_no_emojis['Review_Comment_Title_En'].fillna('No title')

    orders_reviews_no_emojis = orders_reviews_no_emojis.drop_duplicates(subset=['Review_ID'], keep='first')
    orders_reviews_no_emojis = orders_reviews_no_emojis[orders_reviews_no_emojis["Order_ID"].isin(orders["Order_ID"])]

    transformed['orders_review'] = orders_reviews_no_emojis

    return transformed

# ---------- LOAD ----------
def load_data(transformed):
    try:
        md_token = os.environ.get('MD_TOKEN')
        if not md_token:
            raise ValueError("MD_TOKEN no está configurado en las variables de entorno.")

        print("Configurando conexión a MotherDuck...")
        
        conn = duckdb.connect(f'md:wadua-db?token={md_token}')
        
        print("Conexión establecida exitosamente")

        # Cargar en un orden específico
        table_order = ['geolocations', 'customer', 'products', 'sellers', 'orders', 'orders_items', 'orders_payments', 'orders_review']  # Primero geolocations, luego customer

        for table_name in table_order:
            if table_name in transformed:
                df = transformed[table_name]
                print(f"Cargando tabla: {table_name}, filas: {len(df)}")
                conn.register("tmp_df", df)
                conn.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM tmp_df")
                print(f"Tabla {table_name} cargada exitosamente")

        conn.close()
        return True

    except Exception as e:
        print(f"Error cargando datos en MotherDuck: {e}")
        import traceback
        traceback.print_exc()
        return False

# ---------- HANDLER ----------
def lambda_handler(event, context):
    # Asegurar que las variables de entorno estén configuradas
    if 'HOME' not in os.environ:
        os.environ['HOME'] = '/tmp'
    if 'DUCKDB_HOME' not in os.environ:
        os.environ['DUCKDB_HOME'] = '/tmp/duckdb_home'
    
    os.makedirs('/tmp/duckdb_home', exist_ok=True)
    
    print("Iniciando proceso ETL...")
    dfs = extract_data_from_s3()
    if not dfs:
        return {"statusCode": 500, "body": "Error cargando datos desde S3."}

    transformed_dfs = transform_data(dfs)
    if transformed_dfs is None:
        return {"statusCode": 500, "body": "Error transformando datos."}

    success = load_data(transformed_dfs)
    if not success:
        return {"statusCode": 500, "body": "Error cargando datos en MotherDuck."}

    return {"statusCode": 200, "body": f"Proceso ETL exitoso. Tablas cargadas: {list(transformed_dfs.keys())}"}