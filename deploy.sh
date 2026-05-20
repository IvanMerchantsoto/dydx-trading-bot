#!/usr/bin/env bash
# =============================================================================
# deploy.sh  —  Sube el código actualizado a tu VM de Google Cloud y
#               reinicia el bot con nohup.
#
# USO:
#   chmod +x deploy.sh
#   ./deploy.sh                          # deploy interactivo (pide confirmación)
#   ./deploy.sh --dry-run                # muestra qué haría sin ejecutar nada
#   ./deploy.sh --yes                    # sin confirmaciones
#   INSTANCE=mi-vm ZONE=us-central1-a ./deploy.sh   # sin discovery
#
# REQUISITOS:
#   - gcloud CLI instalado y autenticado (gcloud auth login)
#   - python3 y pip en la VM
# =============================================================================

set -euo pipefail

# ── Colores ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
step()  { echo -e "\n${BOLD}==> $*${NC}"; }

# ── Defaults ─────────────────────────────────────────────────────────────────
DRY_RUN=false
SKIP_CONFIRM=false

# Archivos a subir (rutas relativas desde la carpeta que contiene este script)
FILES_TO_UPLOAD=(
    "program/constants.py"
    "program/func_exit_pairs.py"
    "program/main.py"
    "program/close_all.py"
    "program/func_position_guard.py"
    "program/func_kpis.py"
    "program/func_risk_off.py"
    "program/func_entry_pairs.py"
    "program/func_bot_agent.py"
    "program/func_private.py"
    "program/func_exit_pairs.py"
    "program/func_cointegration.py"
    "program/func_public.py"
    "program/func_logging.py"
    "program/func_messaging.py"
    "program/func_pnl.py"
    "program/func_utils.py"
    "program/func_connections.py"
)

# Ruta del proyecto en la VM (sin trailing slash)
REMOTE_DIR="/home/\$REMOTE_USER/DYDX/program"

# Parse args
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
        --yes|-y)  SKIP_CONFIRM=true ;;
        --help|-h)
            echo "Uso: $0 [--dry-run] [--yes]"
            echo "  INSTANCE=nombre ZONE=zona $0   # para saltar discovery"
            exit 0
            ;;
    esac
done

# ── Verificar gcloud ──────────────────────────────────────────────────────────
step "Verificando gcloud CLI"
if ! command -v gcloud &>/dev/null; then
    error "gcloud no está instalado. Instálalo desde: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

GCLOUD_ACCOUNT=$(gcloud config get-value account 2>/dev/null || echo "")
if [ -z "$GCLOUD_ACCOUNT" ]; then
    error "No hay cuenta autenticada. Ejecuta: gcloud auth login"
    exit 1
fi
ok "Cuenta gcloud: $GCLOUD_ACCOUNT"

PROJECT=$(gcloud config get-value project 2>/dev/null || echo "")
if [ -z "$PROJECT" ]; then
    warn "No hay proyecto por defecto. Puedes setearlo con: gcloud config set project TU_PROYECTO"
fi

# ── Discovery de la VM ────────────────────────────────────────────────────────
step "Buscando VM con bot activo"

if [ -z "${INSTANCE:-}" ] || [ -z "${ZONE:-}" ]; then
    info "Listando instancias de Compute Engine..."
    echo ""
    gcloud compute instances list --format="table(name,zone,status,networkInterfaces[0].accessConfigs[0].natIP:label=IP)"
    echo ""

    # Intentar autodetectar buscando la que tiene python main.py corriendo
    VM_COUNT=$(gcloud compute instances list --format="value(name)" 2>/dev/null | wc -l | tr -d ' ')
    if [ "$VM_COUNT" -eq 1 ]; then
        INSTANCE=$(gcloud compute instances list --format="value(name)" 2>/dev/null)
        ZONE=$(gcloud compute instances list --format="value(zone)" 2>/dev/null)
        info "Solo hay 1 VM → usando: $INSTANCE (zona: $ZONE)"
    else
        echo -e "${YELLOW}Tienes varias VMs. Indica cuál usar:${NC}"
        read -r -p "  Nombre de la instancia: " INSTANCE
        read -r -p "  Zona (ej: us-central1-a): " ZONE
    fi
fi

ok "VM: $INSTANCE | Zona: $ZONE"

# ── Obtener usuario remoto ────────────────────────────────────────────────────
REMOTE_USER=$(gcloud compute ssh "$INSTANCE" --zone="$ZONE" --command="whoami" --quiet 2>/dev/null || echo "")
if [ -z "$REMOTE_USER" ]; then
    warn "No se pudo obtener usuario remoto automáticamente. Usando 'usuario' como placeholder."
    read -r -p "  Usuario en la VM (el que usas al hacer SSH): " REMOTE_USER
fi
ok "Usuario remoto: $REMOTE_USER"
REMOTE_DIR="/home/$REMOTE_USER/DYDX/program"

# ── Verificar que los archivos locales existen ────────────────────────────────
step "Verificando archivos locales"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MISSING=()
for f in "${FILES_TO_UPLOAD[@]}"; do
    local_path="$SCRIPT_DIR/$f"
    if [ ! -f "$local_path" ]; then
        MISSING+=("$f")
    fi
done

if [ ${#MISSING[@]} -gt 0 ]; then
    warn "Archivos no encontrados localmente (se saltarán):"
    for m in "${MISSING[@]}"; do echo "    $m"; done
fi

# ── Resumen antes de actuar ───────────────────────────────────────────────────
step "Resumen del deploy"
echo "  VM:          $INSTANCE ($ZONE)"
echo "  Usuario:     $REMOTE_USER"
echo "  Destino:     $REMOTE_DIR"
echo "  Archivos:    ${#FILES_TO_UPLOAD[@]} (total)"
if [ "$DRY_RUN" = true ]; then
    echo -e "  ${YELLOW}MODO DRY-RUN — no se ejecutará nada real${NC}"
fi

if [ "$SKIP_CONFIRM" = false ] && [ "$DRY_RUN" = false ]; then
    echo ""
    read -r -p "¿Continuar? [y/N] " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        echo "Abortado."
        exit 0
    fi
fi

# ── Función helper ────────────────────────────────────────────────────────────
run_remote() {
    local cmd="$1"
    if [ "$DRY_RUN" = true ]; then
        echo -e "  ${YELLOW}[DRY-RUN remote]${NC} $cmd"
    else
        gcloud compute ssh "$INSTANCE" --zone="$ZONE" --quiet --command="$cmd"
    fi
}

copy_file() {
    local src="$1"
    local dest="$2"
    if [ "$DRY_RUN" = true ]; then
        echo -e "  ${YELLOW}[DRY-RUN copy]${NC} $src → $dest"
    else
        gcloud compute scp "$src" "$INSTANCE:$dest" --zone="$ZONE" --quiet
    fi
}

# ── Paso 1: Backup en la VM ───────────────────────────────────────────────────
step "Creando backup en la VM"
BACKUP_DIR="$REMOTE_DIR/../backups/$(date +%Y%m%d_%H%M%S)"
run_remote "mkdir -p $BACKUP_DIR && cp $REMOTE_DIR/*.py $BACKUP_DIR/ 2>/dev/null || true"
ok "Backup creado en: $BACKUP_DIR"

# ── Paso 2: Encontrar y matar el proceso del bot ─────────────────────────────
step "Detectando proceso del bot en la VM"
BOT_PID=$(gcloud compute ssh "$INSTANCE" --zone="$ZONE" --quiet \
    --command="pgrep -f 'python.*main.py' 2>/dev/null || echo ''" 2>/dev/null || echo "")

if [ -z "$BOT_PID" ]; then
    warn "No se encontró proceso activo de main.py (quizás ya está detenido)"
else
    info "PID del bot: $BOT_PID"
    if [ "$DRY_RUN" = false ]; then
        run_remote "kill $BOT_PID 2>/dev/null || true"
        sleep 2
        # Verificar que realmente murió
        STILL_RUNNING=$(gcloud compute ssh "$INSTANCE" --zone="$ZONE" --quiet \
            --command="pgrep -f 'python.*main.py' 2>/dev/null || echo ''" 2>/dev/null || echo "")
        if [ -n "$STILL_RUNNING" ]; then
            warn "El proceso no murió con SIGTERM, usando SIGKILL..."
            run_remote "kill -9 $STILL_RUNNING 2>/dev/null || true"
            sleep 1
        fi
        ok "Proceso detenido"
    fi
fi

# ── Paso 3: Subir archivos ────────────────────────────────────────────────────
step "Subiendo archivos actualizados"
UPLOADED=0
FAILED=0
for f in "${FILES_TO_UPLOAD[@]}"; do
    local_path="$SCRIPT_DIR/$f"
    remote_path="$REMOTE_DIR/$(basename $f)"

    if [ ! -f "$local_path" ]; then
        continue
    fi

    echo -n "  Subiendo $(basename $f)... "
    if copy_file "$local_path" "$remote_path"; then
        echo -e "${GREEN}OK${NC}"
        UPLOADED=$((UPLOADED + 1))
    else
        echo -e "${RED}FAILED${NC}"
        FAILED=$((FAILED + 1))
    fi
done

ok "Subidos: $UPLOADED | Fallidos: $FAILED"

if [ "$FAILED" -gt 0 ]; then
    error "Algunos archivos fallaron. Revisa los errores antes de reiniciar."
    if [ "$SKIP_CONFIRM" = false ]; then
        read -r -p "¿Reiniciar de todas formas? [y/N] " confirm
        if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
            echo "Bot NO reiniciado. Corrige los errores y reinicia manualmente."
            exit 1
        fi
    fi
fi

# ── Paso 4: Verificar sintaxis Python en la VM ────────────────────────────────
step "Verificando sintaxis Python en la VM"
SYNTAX_OK=true
for f in constants.py main.py func_exit_pairs.py close_all.py; do
    result=$(gcloud compute ssh "$INSTANCE" --zone="$ZONE" --quiet \
        --command="cd $REMOTE_DIR && python3 -m py_compile $f 2>&1 && echo OK" 2>/dev/null || echo "ERROR")
    if [[ "$result" == *"OK"* ]]; then
        echo -e "  $f: ${GREEN}OK${NC}"
    else
        echo -e "  $f: ${RED}SYNTAX ERROR${NC} → $result"
        SYNTAX_OK=false
    fi
done

if [ "$SYNTAX_OK" = false ]; then
    error "Hay errores de sintaxis. El bot NO se reiniciará hasta que se corrijan."
    exit 1
fi

# ── Paso 5: Reiniciar el bot ──────────────────────────────────────────────────
step "Reiniciando el bot"
if [ "$DRY_RUN" = true ]; then
    warn "[DRY-RUN] No se reiniciará el bot"
else
    LOG_FILE="$REMOTE_DIR/../bot_stdout.log"
    run_remote "cd $REMOTE_DIR && nohup python3 main.py > $LOG_FILE 2>&1 & echo \$! > $REMOTE_DIR/../bot.pid && echo 'Bot iniciado con PID '\$(cat $REMOTE_DIR/../bot.pid)"
    sleep 3

    # Verificar que arrancó
    NEW_PID=$(gcloud compute ssh "$INSTANCE" --zone="$ZONE" --quiet \
        --command="pgrep -f 'python.*main.py' 2>/dev/null || echo ''" 2>/dev/null || echo "")

    if [ -n "$NEW_PID" ]; then
        ok "Bot corriendo con PID: $NEW_PID"
        echo ""
        info "Primeras líneas de log:"
        run_remote "tail -20 $LOG_FILE 2>/dev/null || echo '(log vacío aún)'"
    else
        error "El bot no parece haber arrancado. Revisa el log:"
        run_remote "tail -30 $LOG_FILE 2>/dev/null || echo '(sin log)'"
    fi
fi

# ── Resumen final ─────────────────────────────────────────────────────────────
step "Deploy completo"
echo ""
echo -e "${GREEN}${BOLD}✅ Deploy exitoso${NC}"
echo ""
echo "Comandos útiles después del deploy:"
echo ""
echo "  # Ver logs en tiempo real:"
echo "  gcloud compute ssh $INSTANCE --zone=$ZONE -- tail -f ~/DYDX/bot_stdout.log"
echo ""
echo "  # Ver logs estructurados (JSONL):"
echo "  gcloud compute ssh $INSTANCE --zone=$ZONE -- tail -f ~/DYDX/program/bot_run.log.jsonl | python3 -m json.tool"
echo ""
echo "  # Ver PID del bot:"
echo "  gcloud compute ssh $INSTANCE --zone=$ZONE -- pgrep -f 'python.*main.py'"
echo ""
echo "  # Detener el bot manualmente:"
echo "  gcloud compute ssh $INSTANCE --zone=$ZONE -- pkill -f 'python.*main.py'"
echo ""
echo "  # Cerrar todas las posiciones (sin reiniciar):"
echo "  gcloud compute ssh $INSTANCE --zone=$ZONE -- 'cd ~/DYDX/program && python3 close_all.py'"
echo ""
echo "  # Backup de los logs:"
echo "  gcloud compute scp $INSTANCE:~/DYDX/program/bot_run.log.jsonl ./bot_run_backup_\$(date +%Y%m%d).jsonl --zone=$ZONE"
