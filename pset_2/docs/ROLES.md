# Roles, ownership y contrato de interfaces

Cada rol es dueño de una carpeta. **No editen archivos de otro rol sin avisar** (trabajen en su rama, ver README).

| Rol | Dueño de | Entrega |
|---|---|---|
| 1. Orquestación (Kestra) | `kestra/`, servicios `postgres` + `kestra` del compose | Memo §2 Ingesta |
| 2. Analítico (dbt) | `dbt/` | Memo §3 Data Quality y §4 Transformaciones |
| 3. Datos (Spark) | `spark/`, servicios `spark-*` del compose | Memo §5 Spark/OBT y §6 Batch vs Streaming |
| 4. Tech Lead | `.env.example`, `README.md`, `docker-compose.yml` (integración), `docs/`, memo final | Memo §1 Arquitectura y §7 Limitaciones |

## Contrato de interfaces
Propuesta inicial para que las piezas encajen; el Tech Lead puede ajustarla (avisar al equipo si cambia).

- **Database:** `PSET2_DB` · **Warehouse:** `PSET2_WH` · **Rol:** `PSET2_ROLE` (creados por [snowflake_setup.sql](snowflake_setup.sql)).
- **Esquemas:** `BRONZE` (Kestra) → `SILVER` (dbt) → `GOLD` (dbt, star schema) → `OBT` (Spark).
- **Bronze (ROL 1 crea, ROL 2 consume con `source()`):** tablas `RAW_OPENALEX_WORKS`, `RAW_NSF_AWARDS` (y las que se necesiten), con el dato original intacto + columnas de auditoría `_loaded_at`, `_source_file`.
- **Silver (ROL 2):** `SLV_<entidad>` (ej. `SLV_WORKS`, `SLV_AWARDS`).
- **Gold (ROL 2):** `DIM_<entidad>` y `FACT_<evento>` — el grain de la tabla de hechos se documenta en `dbt/models/gold/schema.yml`.
- **OBT (ROL 3):** `OBT.OBT_<nombre>`, con grain definido y conteo validado contra `FACT_*`.
- **Orden:** Kestra carga Bronze → dispara `dbt build` (Silver/Gold) → dispara el job de Spark (OBT).

---

## Prompts por rol (pegar DESPUÉS del [Prompt Maestro](PROMPT_MAESTRO.md))

### 🛠️ Rol 1: Arquitecto de Orquestación (Infraestructura y Kestra)
Misión: que los datos fluyan automáticamente desde la nube hasta Snowflake sin intervención humana.
- Archivos: sección Kestra de `docker-compose.yml` y carpeta `kestra/`.
- Tareas:
  - Diseñar el flow YAML de Kestra que extraiga los datos de OpenAlex/NSF y los cargue en la capa Bronze de Snowflake usando `COPY INTO`.
  - Configurar un trigger y frecuencia de ejecución para el batch.
  - Implementar resiliencia: bloque de Exponential Backoff con Jitter y `maxInterval` para errores de red.
  - Documentar su parte (Sección 2: Ingesta) para el memo técnico. Incluir backfill de históricos.

### 📊 Rol 2: Ingeniero Analítico (dbt y Modelado Dimensional)
Misión: limpiar los datos crudos y darles estructura de negocio (Star Schema) dentro de Snowflake.
- Archivos: carpeta `dbt/` (modelos `.sql` y `schema.yml`).
- Tareas:
  - Analizar calidad de datos (duplicados, nulos) y generar la capa Silver. Documentar decisiones de limpieza con métricas concretas.
  - Construir la capa Gold (Star Schema), definiendo claramente el grain de la tabla de hechos.
  - Configurar dependencias con `source()` y `ref()`, y tests de calidad (`not_null`, `unique`, `relationships`) que validen reglas reales.
  - Asegurar materialización `incremental` con `append` en `dbt_project.yml`.
  - Redactar Secciones 3 y 4 (Data Quality y Transformaciones).

### ⚡ Rol 3: Ingeniero de Datos (Spark y Estrategia Batch)
Misión: tomar las tablas del modelo dimensional y aplanarlas en una One Big Table (OBT) para el modelo de IA.
- Archivos: carpeta `spark/` y sección Spark de `docker-compose.yml`.
- Tareas:
  - Conectar Spark a Snowflake, leer el Star Schema de GOLD y construir la OBT.
  - Validar que los joins no introduzcan duplicados ni alteren el conteo de observaciones.
  - Escribir la OBT de vuelta en Snowflake (esquema `OBT`).
  - Redactar Secciones 5 y 6 (Spark/OBT y Batch vs Streaming), justificando formalmente por qué batch es ideal y qué tendría que cambiar para justificar streaming. No implementar streaming.

### 🧠 Rol 4: Líder Técnico e Integrador
Misión: que el repositorio funcione al presionar un botón.
- Archivos: `.env.example`, `README.md`, memo final `PSet2_memo_<apellido_1>_<apellido_2>.pdf`.
- Tareas: estructura del repo, unir los compose en una sola red, README a prueba de balas, unificar el memo (máx. 6 páginas + diagrama + Limitaciones), verificar que no se suban credenciales.

---

## Checklist de entrega (rúbrica del PSet)
- [ ] Kestra: ingesta sin cargas manuales, trigger/frecuencia, retries y backfill funcionando (15%)
- [ ] Calidad y limpieza con métricas y justificación (20%)
- [ ] dbt + Bronze/Silver/Gold (15%) · Star schema con grain y diagrama (15%)
- [ ] Spark + OBT validada, resultado en Snowflake (15%)
- [ ] Reproducibilidad con `docker compose up` (10%)
- [ ] Memo ≤ 6 págs, diagrama de arquitectura, sección Limitaciones (10%)
