# Sesión activa

> Vacío = sin sesión activa. Rellena esto al comenzar a trabajar.

## Estado

- **Feature en curso:** 015 — Dimensión sexo
- **Status:** pending (spec en elicitación con el humano)
- **Inicio de sesión:** 2026-09-17
- **Rama:** feature/015_dimension_sexo
- **Ejecutado por:** Kerin Mindiola (kerinmindiola@gmail.com)

## Plan (tasks de specs/015_dimension_sexo/tasks.md)

 - [x] E0 — Preparar harness: rama feature/015_dimension_sexo, kickstart.json local y registro en feature_list.json.
 - [x] E1 — Elicitar requerimientos de la dimensión sexo con el humano (contrato de entrada, dominio, regla id_genero, DDL, idempotencia, resolución en hechos, contrato de salida DataFrame).
 - [x] E2 — Redactar `specs/015_dimension_sexo/requirements.md` en EARS.
 - [x] E3 — Redactar `specs/015_dimension_sexo/design.md` con ADRs.
 - [x] E4 — Redactar `specs/015_dimension_sexo/tasks.md` con trazabilidad R ↔ Tests.
 - [x] E5 — Marcar `spec_ready` en feature_list.json y detenerse para aprobación humana.

Decisiones del spec (2026-09-17): salida solo DataFrame (sin persistencia);
categoría `SIN INFORMACION` (9) y mapeo de `id_genero` nulo/0/inválido → 9;
resolución en hechos y DDL `tb_dim_sexo` fuera de alcance (solo documentar);
duplicado con distinta descripción aborta el lote; normalización UPPER + strip
con acentos ignorados solo al comparar.

Spec `specs/015_dimension_sexo/` en `spec_ready`, pendiente de aprobación humana.

## Notas

La feature 014 continúa `in_progress` por decisión del humano; trabajar 015 en
paralelo es una excepción documentada del `one_feature_at_a_time`.

Harness en este entorno: `bash init.sh` no funciona por CRLF/Windows; se ejecutó
vía WSL con `wsl -e bash -c "tr -d '\r' < init.sh | bash"` y terminó `[OK] Entorno
listo` (python3 3.14.4, rama feature/015_dimension_sexo). `kickstart.json` se
creó localmente desde el ejemplo (gitignored). Pendiente de verificar en la fase
de implementación: openpyxl, Ruff y pytest no confirmados en el entorno.