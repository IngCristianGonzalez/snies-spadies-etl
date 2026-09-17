#!/usr/bin/env bash
# init.sh — Verificación del harness (AGNÓSTICO de stack)
# Debe terminar con exit code 0 y mensaje [OK] Entorno listo.
# Si falla en cualquier paso, termina con exit code != 0.
#
# El stack se declara en kickstart.json (project.stack, runtime.tool,
# verification.steps). No hay comandos hardcodeados por tecnología.
#
# Uso: bash init.sh [--full]
#   --full  además ejecuta los verification.steps de kickstart.json.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok()   { echo -e "${GREEN}[OK]${NC}   $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

FULL=0
if [[ "${1:-}" == "--full" ]]; then FULL=1; fi

# ─── JSON helper (python3 preferido, node como fallback) ─────────────
# Uso: jget "<expr python sobre d>"  ej: jget "d['project']['stack']"
jget() {
    local expr="$1"
    if command -v python3 &>/dev/null; then
        python3 -c "import json; d=json.load(open('kickstart.json')); print($expr)" 2>/dev/null && return 0
    fi
    if command -v node &>/dev/null; then
        node -e "const d=require('./kickstart.json'); console.log(($expr));" 2>/dev/null && return 0
    fi
    return 1
}

echo ""
echo "=== Harness agnóstico — Verificación de entorno ==="
echo ""

# ─── 0. kickstart.json ───────────────────────────────────────────────
echo "--- [0/5] kickstart.json ---"

if [[ ! -f "kickstart.json" ]]; then
    fail "kickstart.json no encontrado. Cópialo de kickstart.json.example (no está en git)."
fi

STACK=$(jget "d['project']['stack']" || echo "")
RUNTIME_TOOL=$(jget "d['runtime']['tool']" || echo "")
MIN_VERSION=$(jget "d.get('runtime',{}).get('minVersion','')" || echo "")
EXEC_NAME=$(jget "d['team']['executorName']" || echo "")
EXEC_EMAIL=$(jget "d['team']['executorEmail']" || echo "")

[[ -z "$STACK" || "$STACK" == "None" ]] && fail "kickstart.json: falta project.stack (ej: node, dotnet, python)."
[[ -z "$RUNTIME_TOOL" || "$RUNTIME_TOOL" == "None" ]] && fail "kickstart.json: falta runtime.tool (ej: node, dotnet, python3)."
if [[ -z "$EXEC_NAME" || -z "$EXEC_EMAIL" || "$EXEC_NAME" == *"Tu Nombre"* ]]; then
    warn "team.executorName/Email sin rellenar en kickstart.json (tracking de progress/ lo exige)."
else
    ok "Ejecutor: $EXEC_NAME ($EXEC_EMAIL)"
fi
ok "kickstart.json válido — stack: $STACK, runtime: $RUNTIME_TOOL"

# ─── 1. Runtime del stack declarado ──────────────────────────────────
echo ""
echo "--- [1/5] Runtime ($RUNTIME_TOOL) ---"

if ! command -v "$RUNTIME_TOOL" &>/dev/null; then
    fail "'$RUNTIME_TOOL' no encontrado en PATH (stack '$STACK' declarado en kickstart.json)."
fi

VERSION="(desconocida)"
case "$RUNTIME_TOOL" in
    node)    VERSION=$(node --version 2>/dev/null || echo "") ;;
    dotnet)  VERSION=$(dotnet --version 2>/dev/null || echo "") ;;
    python3|python) VERSION=$(python3 --version 2>/dev/null || $RUNTIME_TOOL --version 2>/dev/null || echo "") ;;
    *)       VERSION=$("$RUNTIME_TOOL" --version 2>/dev/null || echo "") ;;
esac
[[ -z "$VERSION" ]] && fail "No se pudo determinar la versión de '$RUNTIME_TOOL'."
ok "$RUNTIME_TOOL $VERSION"

if [[ -n "$MIN_VERSION" && "$MIN_VERSION" != "None" && "$MIN_VERSION" != "" ]]; then
    HAVE=$(echo "$VERSION" | grep -oE '[0-9]+(\.[0-9]+)?' | head -1)
    if [[ "$(printf '%s\n%s' "$MIN_VERSION" "$HAVE" | sort -V | head -1)" != "$MIN_VERSION" ]]; then
        fail "Se exige $RUNTIME_TOOL >= $MIN_VERSION, hay $HAVE."
    fi
    ok "Versión mínima $MIN_VERSION cumplida ($HAVE)"
fi

# ─── 2. Harness docs ─────────────────────────────────────────────────
echo ""
echo "--- [2/5] Harness docs ---"

REQUIRED_DOCS=(
    "AGENTS.md"
    "feature_list.json"
    "ENGINES.md"
    "progress/current.md"
    "progress/history.md"
)

for doc in "${REQUIRED_DOCS[@]}"; do
    if [[ ! -f "$doc" ]]; then
        fail "Archivo de harness faltante: $doc"
    fi
    ok "$doc"
done

OPTIONAL_DOCS=(
    "docs/architecture.md"
    "docs/conventions.md"
    "docs/specs.md"
    "docs/verification.md"
)
for doc in "${OPTIONAL_DOCS[@]}"; do
    if [[ -f "$doc" ]]; then
        ok "$doc (referencia)"
    else
        warn "$doc ausente (doc de referencia opcional)"
    fi
done

# ─── 3. Git ──────────────────────────────────────────────────────────
echo ""
echo "--- [3/5] Git ---"

if ! command -v git &>/dev/null; then
    fail "git no encontrado en PATH."
fi

BRANCH=$(git branch --show-current 2>/dev/null || echo "")
if [[ -z "$BRANCH" ]]; then
    fail "No se está dentro de un repositorio git."
fi
ok "Rama activa: $BRANCH"

if [[ "$BRANCH" == "master" || "$BRANCH" == "main" || "$BRANCH" == "dev" ]]; then
    warn "Estás en la rama base ($BRANCH). El trabajo debe hacerse en feature/*."
fi

if [[ ! "$BRANCH" =~ ^feature/ && ! "$BRANCH" =~ ^(main|master|dev)$ ]]; then
    warn "Rama '$BRANCH' no es main/master/dev ni feature/*. Se requiere aprobación humana."
fi

# ─── 4. Estado de progreso ───────────────────────────────────────────
echo ""
echo "--- [4/5] Estado de progreso ---"

if [[ -f "progress/current.md" ]]; then
    CURRENT_CONTENT=$(grep -v "^\s*$" "progress/current.md" | grep -v "^#" | grep -v "^>" | grep -v "^- " | head -5 || true)
    if [[ -n "$CURRENT_CONTENT" ]]; then
        warn "progress/current.md no está vacío. Posible sesión previa no cerrada."
    else
        ok "progress/current.md limpio"
    fi
fi

# ─── 5. Pasos de verificación del stack ──────────────────────────────
echo ""
echo "--- [5/5] Verificación ($STACK) ---"

STEPS=$(python3 -c "
import json
d = json.load(open('kickstart.json'))
for s in d.get('verification', {}).get('steps', []):
    print(s['workdir'] + ' ||| ' + s['name'] + ' ||| ' + s['run'])
" 2>/dev/null || node -e "
const d = require('./kickstart.json');
for (const s of (d.verification?.steps ?? [])) console.log(s.workdir + ' ||| ' + s.name + ' ||| ' + s.run);
" 2>/dev/null || echo "")

if [[ -z "$STEPS" ]]; then
    warn "Sin verification.steps en kickstart.json. Define al menos build/test del stack."
else
    echo "$STEPS" | while IFS= read -r line; do
        W=$(echo "$line" | awk -F' \\|\\|\\| ' '{print $1}')
        N=$(echo "$line" | awk -F' \\|\\|\\| ' '{print $2}')
        R=$(echo "$line" | awk -F' \\|\\|\\| ' '{print $3}')
        echo "  paso '$N': [$W] $R"
    done
    if [[ "$FULL" == "1" ]]; then
        echo ""
        echo "$STEPS" | while IFS= read -r line; do
            W=$(echo "$line" | awk -F' \\|\\|\\| ' '{print $1}')
            N=$(echo "$line" | awk -F' \\|\\|\\| ' '{print $2}')
            R=$(echo "$line" | awk -F' \\|\\|\\| ' '{print $3}')
            if [[ ! -d "$W" ]]; then
                echo -e "${RED}[FAIL]${NC} paso '$N': workdir '$W' no existe."; exit 1
            fi
            (cd "$W" && bash -c "$R") || { echo -e "${RED}[FAIL]${NC} paso '$N' falló."; exit 1; }
            echo -e "${GREEN}[OK]${NC}   paso '$N'"
        done || fail "verification.steps falló. Revisa los errores antes de continuar."
        ok "verification.steps (--full) — OK"
    else
        warn "Pasos listados (no ejecutados). Usa 'bash init.sh --full' o ejecútalos en la fase de implementación/revisión."
    fi
fi

echo ""
echo -e "${GREEN}=== [OK] Entorno listo ===${NC}"
echo ""
