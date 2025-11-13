import pandas as pd
import os
import sqlite3

# --- 1. Carga de Datos (Extracción) ---
# Leemos los archivos que subiste directamente por su nombre
print("Cargando archivos CSV desde el entorno...")
try:
    # CORRECCIÓN: Se quitaron las barras '/' de los nombres de archivo
    df_orders = pd.read_csv("olist_orders_dataset.csv")
    df_items = pd.read_csv("olist_order_items_dataset.csv")
    df_payments = pd.read_csv("olist_order_payments_dataset.csv")
    df_products = pd.read_csv("olist_products_dataset.csv")
    df_customers = pd.read_csv("olist_customers_dataset.csv")
    
    print("¡Carga completada!")
    print(f"Pedidos (df_orders): {len(df_orders)} filas")
    print(f"Items (df_items): {len(df_items)} filas")

except FileNotFoundError as e:
    print(f"ERROR FATAL: No se pudo encontrar uno de los archivos. {e}")
    print("Por favor, asegúrate de que los archivos .csv estén en el directorio de trabajo.")
except Exception as e:
    print(f"Ha ocurrido un error inesperado durante la carga: {e}")

# --- 2. y 3. Transformación, Limpieza y Carga a SQL ---
# Solo continuamos si la carga fue exitosa (df_orders existe)
if 'df_orders' in locals():
    print("\n--- PASO 2: Iniciando limpieza y transformación de datos... ---")

    # Copia de seguridad
    df_orders_clean = df_orders.copy()

    # 1. Convertir Fechas (de 'object' a 'datetime')
    date_columns = ['order_purchase_timestamp', 
                    'order_delivered_customer_date', 
                    'order_estimated_delivery_date']
    for col in date_columns:
        df_orders_clean[col] = pd.to_datetime(df_orders_clean[col], errors='coerce') 
    print("Fechas convertidas a datetime.")

    # 2. Filtrar por Pedidos Entregados
    pedidos_entregados_count = df_orders_clean['order_status'].value_counts().get('delivered', 0)
    print(f"Filtrando por {pedidos_entregados_count} pedidos 'delivered'...")
    df_orders_clean = df_orders_clean[df_orders_clean['order_status'] == 'delivered'].copy()
    print(f"Pedidos después del filtro: {len(df_orders_clean)}")

    # 3. Crear Nuevas Columnas (Feature Engineering)
    df_orders_clean['tiempo_entrega_dias'] = (df_orders_clean['order_delivered_customer_date'] - 
                                              df_orders_clean['order_purchase_timestamp']).dt.days
    df_orders_clean['mes_año_compra'] = df_orders_clean['order_purchase_timestamp'].dt.to_period('M')
    print("Nuevas columnas 'tiempo_entrega_dias' y 'mes_año_compra' creadas.")

    # --- 2.2 Transformación: Unir DataFrames (JOINs) ---
    print("\nIniciando la unión de DataFrames (JOINS)...")
    df_master = pd.merge(df_orders_clean, df_items, on='order_id')
    df_master = pd.merge(df_master, df_products, on='product_id')
    df_master = pd.merge(df_master, df_payments, on='order_id')
    df_master = pd.merge(df_master, df_customers, on='customer_id')
    print("Unión de tablas completada.")

    # Limpieza Final de Duplicados
    df_master = df_master.drop_duplicates(subset=['order_id', 'order_item_id', 'payment_type'])
    df_master = df_master.copy()
    print(f"¡Tabla Maestra 'df_master' creada! Filas finales: {len(df_master)}")

    
    # --- PASO 2.3: ARREGLO CRÍTICO DEL DTYPE ---
    print("\nCorrigiendo Dtype 'Period' para compatibilidad con SQL...")
    # Convertimos 'Period' (ej. 2017-10) a 'Timestamp' (ej. 2017-10-01)
    df_master['mes_año_compra'] = df_master['mes_año_compra'].dt.to_timestamp()
    print("Columna 'mes_año_compra' convertida a Datetime.")

    
    # --- PASO 3: Cargar (Load) - De Pandas a una Base de Datos SQL ---
    print("\n--- PASO 3: Iniciando Carga a la Base de Datos SQL ---")
    
    DB_NAME = 'ecommerce_analysis.db'
    conn = None # Inicializamos conn fuera del try

    try:
        conn = sqlite3.connect(DB_NAME)
        print(f"Conexión establecida con la base de datos: {DB_NAME}")

        print(f"Cargando {len(df_master)} filas en la tabla 'master_table'...")
        df_master.to_sql(
            name='master_table',
            con=conn,
            if_exists='replace',
            index=False
        )
        print("¡Carga completada! El DataFrame 'df_master' está ahora en la base de datos SQL.")

        # Verificación
        print("\nEjecutando consulta 'SELECT COUNT(*) FROM master_table'...")
        df_verificacion = pd.read_sql_query("SELECT COUNT(*) FROM master_table", conn)
        conteo_filas_sql = df_verificacion.iloc[0,0]
        
        print(f"La tabla 'master_table' en '{DB_NAME}' tiene: {conteo_filas_sql} filas.")
        if conteo_filas_sql == len(df_master):
            print("¡Verificación exitosa! El conteo de filas coincide.")
        else:
            print("ERROR: El conteo de filas NO coincide.")

    except Exception as e:
        print(f"Ha ocurrido un error durante la carga a SQL: {e}")

    finally:
        if conn:
            conn.close()
            print(f"Conexión con '{DB_NAME}' cerrada.")

else:
    print("\nEl proceso se detuvo porque la carga de datos inicial (Paso 1) falló.")
    import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# --- 4. Análisis con SQL ---

DB_NAME = 'ecommerce_analysis.db'

def run_query(query):
    """
    Se conecta a la BD, ejecuta una consulta SQL 
    y devuelve los resultados en un DataFrame de Pandas.
    """
    try:
        conn = sqlite3.connect(DB_NAME)
        # pd.read_sql_query se encarga de abrir, ejecutar y cerrar
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        return pd.DataFrame() # Devuelve un DataFrame vacío en caso de error

# --- Respondiendo las Preguntas de Negocio ---

# P1: ¿Cuál es nuestro crecimiento de ingresos mes a mes?
print("\n--- P1: Crecimiento de Ingresos Mes a Mes ---")
query_ingresos_mes = """
SELECT
    -- 'strftime' es una función de SQL para formatear fechas
    strftime('%Y-%m', order_purchase_timestamp) AS mes_año,
    SUM(payment_value) AS ingresos_totales
FROM 
    master_table
GROUP BY 
    mes_año
ORDER BY 
    mes_año;
"""
df_ingresos_mes = run_query(query_ingresos_mes)
print(df_ingresos_mes.head())


# P2: ¿Cuál es el tiempo medio de entrega?
print("\n--- P2: Tiempo Medio de Entrega ---")
query_tiempo_entrega = """
SELECT
    AVG(tiempo_entrega_dias) AS media_dias_entrega
FROM 
    master_table;
"""
df_tiempo_entrega = run_query(query_tiempo_entrega)
print(df_tiempo_entrega)


# P3: ¿Cuáles son las 10 categorías de productos más vendidas?
print("\n--- P3: Top 10 Categorías de Productos (por ingresos) ---")
query_top_categorias = """
SELECT
    product_category_name,
    SUM(payment_value) AS ingresos_totales
FROM 
    master_table
WHERE
    product_category_name IS NOT NULL -- Excluimos nulos
GROUP BY 
    product_category_name
ORDER BY 
    ingresos_totales DESC -- Ordenamos de mayor a menor
LIMIT 10;
"""
df_top_categorias = run_query(query_top_categorias)
print(df_top_categorias)


# P4: ¿Cuáles son las 10 ciudades con más clientes?
print("\n--- P4: Top 10 Ciudades (por número de clientes únicos) ---")
query_top_ciudades = """
SELECT
    customer_city,
    COUNT(DISTINCT customer_unique_id) AS total_clientes
FROM 
    master_table
GROUP BY 
    customer_city
ORDER BY 
    total_clientes DESC
LIMIT 10;
"""
df_top_ciudades = run_query(query_top_ciudades)
print(df_top_ciudades)
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd # Importar de nuevo por si acaso

# --- 5. Visualización para el Portafolio ---

print("\n--- PASO 5: Iniciando Visualización de Resultados ---")

# Configuración de estilo de Seaborn para gráficos más bonitos
sns.set(style="whitegrid")

# --- Gráfico 1: Crecimiento de Ingresos Mes a Mes (Línea de Tiempo) ---
try:
    if 'df_ingresos_mes' in locals() and not df_ingresos_mes.empty:
        # Asegurarse de que 'mes_año' sea el índice para graficar
        # Hacemos una copia para no modificar el original
        df_ingresos_plot = df_ingresos_mes.set_index(pd.to_datetime(df_ingresos_mes['mes_año']))
        
        plt.figure(figsize=(14, 7)) # Tamaño del gráfico
        
        # Graficamos la serie de tiempo
        plt.plot(df_ingresos_plot.index, df_ingresos_plot['ingresos_totales'], 
                 marker='o', linestyle='-', color='b')
        
        plt.title('Crecimiento de Ingresos Totales (2016-2018)', fontsize=16)
        plt.xlabel('Mes y Año', fontsize=12)
        plt.ylabel('Ingresos Totales (en millones)', fontsize=12)
        plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1000000:.1f}M'))
        
        # Guardar el gráfico
        plt.savefig('ingresos_mes_a_mes.png', dpi=300, bbox_inches='tight')
        print("Gráfico 'ingresos_mes_a_mes.png' guardado.")
        # plt.show() # Descomenta esta línea si quieres ver el gráfico en Colab
        plt.close() # Cierra la figura para liberar memoria
        
    else:
        print("No se encontró 'df_ingresos_mes' o está vacío. Saltando Gráfico 1.")

except Exception as e:
    print(f"Error al crear Gráfico 1: {e}")


# --- Gráfico 2: Top 10 Categorías de Productos (Barras Horizontales) ---
try:
    if 'df_top_categorias' in locals() and not df_top_categorias.empty:
        
        plt.figure(figsize=(12, 8)) # Tamaño del gráfico
        
        # Seaborn hace esto muy fácil
        sns.barplot(
            x='ingresos_totales',
            y='product_category_name',
            data=df_top_categorias,
            palette='viridis' # Paleta de colores
        )
        
        plt.title('Top 10 Categorías de Productos por Ingresos', fontsize=16)
        plt.xlabel('Ingresos Totales (en millones)', fontsize=12)
        plt.ylabel('Categoría de Producto', fontsize=12)
        plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1000000:.1f}M'))

        # Guardar el gráfico
        plt.savefig('top_10_categorias.png', dpi=300, bbox_inches='tight')
        print("Gráfico 'top_10_categorias.png' guardado.")
        # plt.show() # Descomenta esta línea si quieres ver el gráfico en Colab
        plt.close()
        
    else:
        print("No se encontró 'df_top_categorias' o está vacío. Saltando Gráfico 2.")

except Exception as e:
    print(f"Error al crear Gráfico 2: {e}")