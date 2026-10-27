# DataMart S.A.S. — Pipeline ETL & Data Warehouse con Apache Airflow

Solución integral de ingeniería de datos de extremo a extremo para **DataMart S.A.S.** El pipeline ingesta datos transaccionales heterogéneos, los limpia aplicando reglas estrictas de negocio, los enriquece consumiendo una API REST interna construida en **FastAPI**, y los carga de forma idempotente en un Data Warehouse dimensional en **PostgreSQL**, todo orquestado con **Apache Airflow** dentro de **Docker**.

---

## Tabla de Contenidos
1. [Contexto del Negocio y Objetivos](#contexto-del-negocio-y-objetivos)
2. [Arquitectura del Sistema](#arquitectura-del-sistema)
3. [Estructura del Proyecto](#-estructura-del-proyecto)
4. [Ejecución Automatizada con run_pipeline.ps1](#ejecución-automatizada-con-run_pipelineps1)
5. [Instalación del Entorno Local con uv (Desarrollo y VS Code)](#instalación-del-entorno-local-con-uv-desarrollo-y-vs-code)
6. [Guía Rápida de Despliegue con Docker (< 5 minutos)](#guía-rápida-de-despliegue-con-docker--5-minutos)
7. [Validación Automática de Connections y Variables](#validación-automática-de-connections-y-variables)
8. [Ejecución del Pipeline ETL](#ejecución-del-pipeline-etl)
9. [Paso a paso para conectar DBeaver](#paso-a-paso-para-conectar-dbeaver)
10. [Esquema de Constelación de Hechos (Fact Constellation / Multi-Star Schema)](#esquema-de-constelación-de-hechos-fact-constellation--multi-star-schema)
11. [Documento de Decisiones Técnicas y Ambigüedades](#documento-de-decisiones-técnicas-y-ambigüedades)
12. [Comandos de Mantenimiento](#-comandos-de-mantenimiento)
---

## Contexto del Negocio y Objetivos

DataMart S.A.S. experimentó un crecimiento del 40% en transacciones, lo que paralizó a sus equipos de analítica y finanzas:
- Los datos de ventas se encontraban en volcados planos sin separación clara entre ventas y devoluciones.
- El catálogo de productos residía en un sistema legado sin estructura analítica.
- Existían diferencias en monedas, formatos de fecha y zonas horarias.
- El cierre financiero mensual tardaba entre 3 y 5 días de trabajo manual.

### Objetivos Alcanzados:
- **Automatización Docker 100%**: Levantamiento de la plataforma completa con un único comando (`docker-compose up -d`).
- **Cero Intervención Manual**: Inyección automática de conexiones (`AIRFLOW_CONN_*`) y variables (`AIRFLOW_VAR_*`).
- **Enriquecimiento de Datos**: Consumo de un microservicio FastAPI para clasificar productos en 5 categorías de negocio (*Electrónica, Hogar, Ropa, Deportes, Papelería*).
- **Separación Contable Estricta**: Aislamiento de ventas válidas (`fact_sales`), devoluciones tratadas en positivo (`fact_returns`) y registros defectuosos (`log_rejections`).
- **Idempotencia Garantizada**: Ejecuciones repetidas del DAG producen exactamente el mismo estado sin duplicar información.

---

## Arquitectura del Sistema

```text
                                 ┌────────────────────────┐
                                 │  FastAPI (Port 8000)   │
                                 │ Catálogo Legado Prod.  │
                                 └───────────┬────────────┘
                                             │ HTTP REST
                                             ▼
┌─────────────────────────┐      ┌────────────────────────┐      ┌─────────────────────────┐
│   Fuente Transaccional  │ ───► │  Apache Airflow 2.8.1  │ ───► │ PostgreSQL DWH (:5433)  │
│ Sales Transaction v.4a  │      │   Orquestador ETL      │      │     (Star Schema)       │
└─────────────────────────┘      └────────────────────────┘      └─────────────────────────┘
                                             │
                                             ▼
                                 ┌────────────────────────┐
                                 │ Postgres Metadata (:5432)
                                 │ Metadatos de Airflow   │
                                 └────────────────────────┘
```

### Servicios Contenerizados:
1. **`airflow-webserver` & `airflow-scheduler`**: Motor de Airflow 2.8.1 (LocalExecutor).
2. **`airflow-postgres`**: Base de datos dedicada exclusivamente a los metadatos de Airflow (`5432`).
3. **`dwh-postgres`**: Repositorio Analítico (Data Warehouse) en PostgreSQL con esquema `datamart_dw` (`5433`).
4. **`datamart-product-api`**: Microservicio FastAPI que expone endpoints paginados y por código del catálogo (`8000`).

---

## 📁 Estructura del Proyecto

```text
prueba-DataMart/
├── .env.example               # Plantilla de variables de entorno
├── .env                       # Variables de entorno activas (excluido en .gitignore)
├── .gitignore                 # Reglas de exclusión para Git
├── docker-compose.yml         # Orquestación de todos los contenedores
├── run_pipeline.ps1           # Script de automatización en un solo paso
├── README.md                  # Documentación maestra y guía de entrega
├── DECISIONS.md               # Documento de decisiones técnicas
├── api/                       # Microservicio FastAPI
│   ├── Dockerfile
│   ├── pyproject.toml
│   └── main.py
├── dags/                      # DAGs de Apache Airflow
│   └── datamart_etl_pipeline.py
├── data/                      # Volúmenes de datos transaccionales
│   └── Sales Transaction v.4a.csv
└── sql/                       # Scripts DDL y consultas de negocio
    ├── 01_init_dwh.sql        # Creación de esquema y tablas Star Schema
    └── 02_business_questions.sql # Consultas SQL que responden a la Sección 7
```

---

## Ejecución Automatizada con `run_pipeline.ps1`

Para ejecutar todo el pipeline de punta a punta de forma automática:

### 1. Crear el archivo `.env`
Copia la plantilla `.env.example` para inicializar tus variables de entorno:
```powershell
# En Windows PowerShell:
copy .env.example .env

# En Linux/macOS:
cp .env.example .env
```

### 2. Ejecutar el script automatizado
En Windows PowerShell, desde la raíz del proyecto:
```powershell
.\run_pipeline.ps1
```
*(Si PowerShell te pide permisos de ejecución, corre: `powershell -ExecutionPolicy Bypass -File .\run_pipeline.ps1`)*

---

## Instalación del Entorno Local con `uv` (Desarrollo y VS Code)

Si deseas inspeccionar el código localmente, depurar scripts o evitar advertencias de importación en tu editor (como VS Code):

### 1. Instalar `uv` (si aún no lo tienes)
- **Windows (PowerShell):**
  ```powershell
  powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
  ```
- **Linux/macOS:**
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

### 2. Crear y activar el entorno virtual
```bash
# Crear el entorno virtual en la raíz
uv venv

# Activar en Windows PowerShell:
.venv\Scripts\activate

# O activar en Linux/macOS:
source .venv/bin/activate
```

### 3. Instalar todas las dependencias del proyecto con `uv`
```bash
uv pip install pandas requests sqlalchemy apache-airflow pyarrow psycopg2-binary fastapi uvicorn
```

### 4. Configurar el Intérprete en VS Code
1. Presiona `Ctrl + Shift + P`.
2. Escribe: **`Python: Select Interpreter`**.
3. Selecciona el que dice: **`('.venv': venv)`**.

---

## Guía Rápida de Despliegue con Docker (< 5 minutos)

Para ejecutar el pipeline completo en producción o evaluación, **Docker gestiona automáticamente todas las dependencias**:

### 1. Clonar el repositorio
```bash
git clone https://github.com/millussion/prueba-DataMart.git
cd prueba-DataMart
```

### 2. Configurar variables de entorno
```bash
cp .env.example .env
```
*(En Windows PowerShell: `copy .env.example .env`)*

### 3. Verificar el archivo de datos
Confirma que el archivo transaccional esté presente en la carpeta `data/`:
```bash
data/Sales Transaction v.4a.csv
```

### 4. Levantar la infraestructura completa con un solo comando
```bash
docker-compose up -d
```
> **Nota:** Todos los servicios (Airflow, Metadatos, DWH, API de Productos) levantarán automáticamente sin intervención manual. Las tablas del Data Warehouse se crean solas durante el arranque mediante el script `01_init_dwh.sql`.

---

### Validación Automática de Connections y Variables:
Comprueba que Airflow inicializó las credenciales y rutas de forma nativa:
```bash
# Validar conexión al DWH PostgreSQL
docker exec -it airflow-scheduler airflow connections get POSTGRES_DWH

# Validar variables operativas del pipeline
docker exec -it airflow-scheduler airflow variables get RAW_DATA_PATH
docker exec -it airflow-scheduler airflow variables get PRODUCT_API_URL
```

---

## Ejecución del Pipeline ETL

Para disparar el DAG manualmente y monitorear su ejecución:

```bash
# 1. Despausar el DAG
docker exec -it airflow-scheduler airflow dags unpause datamart_etl_pipeline

# 2. Ejecutar el DAG inmediatamente
docker exec -it airflow-scheduler airflow dags trigger datamart_etl_pipeline

# 3. Monitorear estado de las tareas
docker exec -it airflow-scheduler airflow dags list-runs -d datamart_etl_pipeline
```

---

## Paso a paso para conectar DBeaver:

1. Abre **DBeaver**.
2. En la barra superior, haz clic en el ícono del enchufe con el signo más (**+**) o ve a **Base de datos > Nueva conexión**.
3. Selecciona **PostgreSQL** y haz clic en **Siguiente**.
4. Llena los campos con estos datos exactos de tu configuración:

| Campo | Valor |
| :--- | :--- |
| **Host** | `localhost` |
| **Puerto (Port)** | `5433` (¡Ojo! Es 5433, no 5432) |
| **Database** | `datamart_dwh` |
| **Username** | `dwh_user` |
| **Password** | `dwh_secure_password_2024` |

5. Haz clic en el botón **Probar conexión (Test Connection)**:
   - *(Si DBeaver te pide descargar el controlador de PostgreSQL, dale clic a "Descargar" y tardará 2 segundos).*
   - Te saldrá un mensaje en verde de conexión exitosa.
6. Haz clic en **Finalizar (Finish)**.

---

## Esquema de Constelación de Hechos (Fact Constellation / Multi-Star Schema)

<img width="1322" height="726" alt="Captura de pantalla 2026-10-27 090222" src="https://github.com/user-attachments/assets/63f7793e-573e-48b8-b0d0-09f0481dd232" />

### ¿Por qué NO es un Copo de Nieve (Snowflake)?
No es un copo de nieve porque las dimensiones (dim_products, dim_customers, dim_date) no están divididas en subtablas. Siguen siendo dimensiones planas y desnormalizadas típicas de una estrella.
### Constelación de Hechos 
Son dos estrellas que comparten las mismas dimensiones (lo que Kimball llama "Dimensiones Conformes" o Conformed Dimensions):
- Ambas tablas de hechos consultan exactamente a las mismas 3 dimensiones:

1. Un producto se vende (fact_sales) y el mismo producto se devuelve (fact_returns).
2. Un cliente compra (fact_sales) y el mismo cliente pide reembolso (fact_returns).
3. Ambos eventos ocurren en una fecha de dim_date.

### Si hubiéramos forzado una sola tabla de hechos:
- Habríamos tenido que mezclar números positivos y negativos, violando la regla de negocio que prohíbe mezclar devoluciones con ventas.
- Si un analista de BI hiciera un COUNT(*) o un promedio de ventas, las devoluciones falsearían el volumen real transaccionado.
---

## Documento de Decisiones Técnicas y Ambigüedades

**Acceso al documento completo:** Para una explicación detallada, justificación técnica y análisis en profundidad de todo el proceso de pensamiento, consulta el archivo  **[DECISIONS.md](DECISIONS.md)**.

---

## 🛑 Comandos de Mantenimiento

Para apagar los servicios manteniendo los datos persistidos en los volúmenes de Docker:
```bash
docker-compose down
```

Para reiniciar un servicio específico (por ejemplo, la API o el Scheduler):
```bash
docker-compose restart airflow-scheduler
docker-compose restart product-api
```