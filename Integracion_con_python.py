import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import json
from pymongo import MongoClient

# Configuración de PostgreSQL
pg_config = {
    "host": "localhost",
    "port": "5432",
    "database": "lab07",
    "user": "postgres",
    "password": "password"
}

# Leer el CSV
csv_path = "Datos_para_SQL/pais_poblacion.csv"
df_pg = pd.read_csv(csv_path)

csv_path2 = "Datos_para_SQL/pais_envejecimiento.csv"
df_pg2 = pd.read_csv(csv_path2)

# Crear conexión a PostgreSQL
engine = create_engine(f"postgresql://{pg_config['user']}:{pg_config['password']}@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}")

# Insertar los datos 
df_pg.to_sql('poblacion', engine, if_exists='replace', index=False)

# Insertar los datos 
df_pg2.to_sql('envejecimiento', engine, if_exists='replace', index=False)

print("Datos CSV cargados en PostgreSQL")

# Conexión a MongoDB Atlas
mongo_uri = "mongodb+srv://<db_username>:<db_password>@cluster0.uegtbjj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(mongo_uri)
db = client["Lab07"]

# Función para cargar archivos JSON
def cargar_json(collection_name, archivos, campo_fuente):
    collection = db[collection_name]
    
    for archivo, fuente in archivos:
        with open(archivo, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            # Agregar el campo fuente (ej., continente)
            for doc in data:
                doc[campo_fuente] = fuente
            
            collection.insert_many(data)

# Archivos a cargar
archivos_turisticos = [
    ("Datos_para_MongoDB/costos_turisticos_america.json", "América"),
    ("Datos_para_MongoDB/costos_turisticos_asia.json", "Asia"),
    ("Datos_para_MongoDB/costos_turisticos_europa.json", "Europa"),
    ("Datos_para_MongoDB/costos_turisticos_africa.json", "África")
]

# Cargar datos turísticos
cargar_json("costo_turistico", archivos_turisticos, "fuente")

print("Datos turísticos cargados en MongoDB")

# Cargar el archivo de Big Mac
with open("Datos_para_MongoDB/paises_mundo_big_mac.json", 'r', encoding='utf-8') as f:
    data_big_mac = json.load(f)

# Insertar en la colección
db["big_mac_prices"].insert_many(data_big_mac)

print("Datos de Big Mac cargados en MongoDB")


# 1. Función para leer tablas desde PostgreSQL
def leer_tabla_postgres(engine, tabla):
    query = f"SELECT * FROM {tabla}"
    return pd.read_sql(query, engine)

# Leer las tablas de PostgreSQL
df_poblacion = leer_tabla_postgres(engine, "poblacion")
df_envejecimiento = leer_tabla_postgres(engine, "envejecimiento")

print("Datos leídos de PostgreSQL:")
print(df_poblacion.head())
print(df_envejecimiento.head())

# 2. Función para leer colecciones desde MongoDB
def leer_coleccion_mongo(collection):
    return pd.DataFrame(list(db[collection].find()))

# Leer las colecciones
df_big_mac = leer_coleccion_mongo("big_mac_prices")
df_turismo = leer_coleccion_mongo("costo_turistico")

print("Datos leídos de MongoDB:")
print(df_big_mac.head())
print(df_turismo.head())

# 3. Revisar y limpiar los datos
# Verificar valores nulos
print("Verificación de nulos----------------------------------")
print("Nulos población:")
print(df_poblacion.isnull().sum())
print("Nulos envejecimiento:")
print(df_envejecimiento.isnull().sum())
print("Nulos Big Mac:")
print(df_big_mac.isnull().sum())
print("Nulos turismo:")
print(df_turismo.isnull().sum())

# Verificar duplicados en el DataFrame df_big_mac
print("Duplicados en df_big_mac:")
print(df_big_mac[df_big_mac.duplicated(subset=["país", "continente"])].shape)

# Verificar duplicados en el DataFrame df_turismo
print("Duplicados en df_turismo:")
print(df_turismo[df_turismo.duplicated(subset=["país", "continente"])].shape)

# Verificar duplicados en el DataFrame df_poblacion
print("Duplicados en df_poblacion:")
print(df_poblacion[df_poblacion.duplicated(subset=["pais", "continente"])].shape)

# Verificar duplicados en el DataFrame df_envejecimiento
print("Duplicados en df_envejecimiento:")
print(df_envejecimiento[df_envejecimiento.duplicated(subset=["nombre_pais", "continente"])].shape)


# Eliminar duplicados si existen
df_poblacion.drop_duplicates(inplace=True)
df_envejecimiento.drop_duplicates(inplace=True)
df_big_mac.drop_duplicates(inplace=True)
# Seleccionar solo las columnas simples (excluir dict/list)
columnas_simples = [col for col in df_turismo.columns if not any(df_turismo[col].apply(lambda x: isinstance(x, (dict, list))))]

# Eliminar duplicados solo en columnas simples
df_turismo.drop_duplicates(subset=columnas_simples, inplace=True)

print("Limpieza completada.")

# Flatten nested dictionary columns in the 'costos_diarios_estimados_en_dólares' column
def flatten_costos_diarios(df):
    # Normalize the 'costos_diarios_estimados_en_dólares' column (which contains dictionaries)
    costos_flat = pd.json_normalize(df['costos_diarios_estimados_en_dólares'])
    
    # Add flattened columns back to the original dataframe, and drop the original 'costos_diarios_estimados_en_dólares' column
    df = df.drop(columns=['costos_diarios_estimados_en_dólares'])
    df = pd.concat([df, costos_flat], axis=1)
    
    return df

# Example: Assuming df_turismo is the dataframe with the nested column
df_turismo = flatten_costos_diarios(df_turismo)

df_poblacion.rename(columns={'pais':'país'}, inplace = True)
df_envejecimiento.rename(columns={'nombre_pais':'país'}, inplace = True)
df_poblacion.rename(columns={'poblacion':'población'}, inplace = True)
df_envejecimiento.rename(columns={'poblacion':'población'}, inplace = True)
df_envejecimiento.rename(columns={'region':'región'}, inplace = True)
df_poblacion.rename(columns={'costo_bajo_hospedaje':'hospedaje.precio_bajo_usd'}, inplace = True)
df_poblacion.rename(columns={'costo_promedio_comida':'comida.precio_promedio_usd'}, inplace = True)
df_poblacion.rename(columns={'costo_bajo_transporte':'transporte.precio_bajo_usd'}, inplace = True)
df_poblacion.rename(columns={'costo_promedio_entretenimiento':'entretenimiento.precio_promedio_usd'}, inplace = True)

# 4. Fusionar (merge) los DataFrames por 'país'
df_integrado = pd.merge(df_big_mac, df_turismo, on=["país","continente"], how="outer")
df_integrado = pd.merge(df_integrado, df_poblacion, on=["país","continente", "población", "hospedaje.precio_bajo_usd", "transporte.precio_bajo_usd", "entretenimiento.precio_promedio_usd", "comida.precio_promedio_usd"], how="outer")
df_integrado = pd.merge(df_integrado, df_envejecimiento, on=["país","continente", "población","capital", "región"], how="left")

# Eliminar las columnas de ID duplicadas si existen (ejemplo con 'id_x' y 'id_y')
df_integrado = df_integrado.loc[:, ~df_integrado.columns.str.contains('_id', case=False)]
df_integrado = df_integrado.loc[:, ~df_integrado.columns.str.contains('id_', case=False)]
df_integrado = df_integrado.loc[:, ~df_integrado.columns.str.contains('fuente', case=False)]

# Agregar una nueva columna de 'id' 
df_integrado['id'] = range(1, len(df_integrado) + 1)

df_integrado['tasa_de_envejecimiento'] = df_integrado.apply(
    lambda row: df_envejecimiento.loc[df_envejecimiento['país'] == row['país'], 'tasa_de_envejecimiento'].values[0] 
    if pd.isnull(row['tasa_de_envejecimiento']) else row['tasa_de_envejecimiento'],
    axis=1
)

print("Datos integrados:")
print(df_integrado.head())
print(df_integrado.columns)

# 5. Crear la tabla en el Data Warehouse (si no existe)
dw_table = "datawarehouse"

df_integrado.to_sql(dw_table, engine, if_exists='replace', index=False)

print(f"Datos cargados en la tabla '{dw_table}' del Data Warehouse.")

# Exportar los datos a un archivo CSV
df_integrado.to_csv('datos_integrados.csv', index=False)

print("Los datos han sido exportados a 'datos_integrados.csv'.")

