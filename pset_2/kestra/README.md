# Kestra (ROL 1)
UI: http://localhost:8080 (usuario/clave de KESTRA_USER / KESTRA_PASSWORD en `.env`).
Los `.yml` de `kestra/flows/` se sincronizan automaticamente (namespace `pset2`).
Credenciales en flows: `{{ envs.snowflake_user }}`, `{{ envs.snowflake_password }}`, ... (nunca hardcodear).
