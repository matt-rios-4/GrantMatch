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
- **Bronze (ROL 1 crea, ROL 2 consume con `source()`):** tablas `RAW_OPENALEX_AWARDS`, `RAW_OPENALEX_FUNDERS`, `RAW_OPENALEX_TOPICS` y `RAW_OPENALEX_WORKS`, con el dato original intacto en `RAW` (VARIANT) + columnas de auditoría `_loaded_at`, `_source_file`, `_source_row_number`.
- **Silver (ROL 2):** `SLV_<entidad>` (ej. `SLV_WORKS`, `SLV_AWARDS`).
- **Gold (ROL 2):** `DIM_<entidad>` y `FACT_<evento>` — el grain de la tabla de hechos se documenta en `dbt/models/gold/schema.yml`.
- **OBT (ROL 3):** `OBT.OBT_<nombre>`, con grain definido y conteo validado contra `FACT_*`.
- **Orden:** Kestra carga Bronze → dispara `dbt build` (Silver/Gold) → dispara el job de Spark (OBT).

---

## Checklist de entrega (rúbrica del PSet)
- [ ] Kestra: ingesta sin cargas manuales, trigger/frecuencia, retries y backfill funcionando (15%)
- [ ] Calidad y limpieza con métricas y justificación (20%)
- [ ] dbt + Bronze/Silver/Gold (15%) · Star schema con grain y diagrama (15%)
- [ ] Spark + OBT validada, resultado en Snowflake (15%)
- [ ] Reproducibilidad con `docker compose up` (10%)
- [ ] Memo ≤ 6 págs, diagrama de arquitectura, sección Limitaciones (10%)
