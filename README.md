# Proyecto BI End-to-End: Análisis de Ventas de E-Commerce (Olist)

Este proyecto es una simulación de un caso de negocio real y un análisis de Business Intelligence de principio a fin. El objetivo es procesar, analizar y visualizar los datos de la base de datos pública de E-Commerce brasileño de Olist (100.000 pedidos) para extraer insights de negocio.

El *pipeline* completo se ejecutó de la siguiente manera:
**ETL (Python) -> Base de Datos (SQLite) -> Análisis (SQL) -> Visualización (Python)**

---

## 1. El Problema de Negocio

El objetivo era actuar como Analista de BI para el gerente de "Olist" y responder a preguntas clave sobre el rendimiento del negocio:

* ¿Cuál es nuestro crecimiento de ingresos mes a mes?
* ¿Cuáles son las categorías de productos más vendidas y rentables?
* ¿Cuáles son las principales ciudades donde se concentran nuestros clientes?
* ¿Cuál es el tiempo medio de entrega de nuestros pedidos?

---

## 2. Stack de Herramientas (Tech Stack)

* **Lenguaje de Programación:** Python
* **ETL y Transformación:** Pandas
* **Base de Datos:** SQLite
* **Análisis y Consultas:** SQL
* **Visualización:** Matplotlib y Seaborn

---

## 3. El Proceso (ETL Pipeline)

El proceso se dividió en tres fases principales:

### Fase 1: Extracción, Transformación y Carga (ETL)
Usando **Pandas**, los 9 archivos CSV originales (pedidos, items, clientes, pagos, productos, etc.) fueron cargados.

* **Limpieza:** Se convirtieron las fechas de tipo `string` a `datetime`.
* **Filtrado:** Se filtró la base de datos para analizar únicamente pedidos con estatus `"delivered"`.
* **Feature Engineering:** Se crearon nuevas columnas, como `tiempo_entrega_dias` y `mes_año_compra`.
* **Unión (JOIN):** Se unieron 5 tablas clave (`orders`, `items`, `products`, `payments`, `customers`) en una única **`master_table`** de 112.636 filas.

### Fase 2: Carga en Base de Datos
La `master_table` limpia fue cargada en una base de datos **SQLite** (`ecommerce_analysis.db`). Esto simula un *Data Warehouse* centralizado, listo para el análisis.

### Fase 3: Análisis con SQL
Se escribieron consultas **SQL** para conectarse a la base de datos y responder a las preguntas de negocio.

---

## 4. Resultados e Insights de Negocio

### Insight 1: Crecimiento de Ingresos (2016-2018)
Se observa un crecimiento exponencial en 2017, con una fuerte estacionalidad y un pico en noviembre de 2017, seguido de una estabilización en 2018.

**Consulta SQL:**
```sql
SELECT
    strftime('%Y-%m', order_purchase_timestamp) AS mes_año,
    SUM(payment_value) AS ingresos_totales
FROM master_table
GROUP BY mes_año
ORDER BY mes_año;
