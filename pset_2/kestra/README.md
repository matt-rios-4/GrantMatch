# Kestra (ROL 1)
UI: http://localhost:8080 (usuario/clave de KESTRA_USER / KESTRA_PASSWORD en `.env`).
Los `.yml` de `kestra/flows/` se sincronizan automaticamente (namespace `pset2`).
Credenciales en flows: `{{ envs.snowflake_user }}`, `{{ envs.snowflake_password }}`, ... (nunca hardcodear).

## Prueba de ingesta acotada

`copy_single_openalex_author_file` está preparado para cargar **un solo** archivo
Parquet de `authors/` mediante `FILES`, hacia
`PSET2_DB.BRONZE.RAW_OPENALEX_AUTHORS`. No tiene trigger ni valor por defecto para
el archivo: antes de ejecutarlo, confirma el nombre exacto con `LIST` y usa solo
su ruta relativa al stage, por ejemplo `authors/<archivo>.parquet`. No ejecutes
una carga de la carpeta `authors/`.

Pasos y SQL de diagnóstico: [docs/ingesta_kestra.md](../docs/ingesta_kestra.md).
