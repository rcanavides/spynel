#!/usr/bin/env bash
set -euo pipefail

UPSTREAM="${1:-upstream/main}"

if ! git rev-parse --git-dir >/dev/null 2>&1; then
    echo "ERROR: ejecutar dentro del repositorio Spynel."
    exit 1
fi

if ! git rev-parse --verify "$UPSTREAM" >/dev/null 2>&1; then
    echo "ERROR: no existe $UPSTREAM"
    echo "Ejecute primero: git fetch upstream --prune --tags"
    exit 1
fi

HEAD_SHA="$(git rev-parse HEAD)"
UPSTREAM_SHA="$(git rev-parse "$UPSTREAM")"
BASE_SHA="$(git merge-base HEAD "$UPSTREAM")"

TMPDIR_LOCAL="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_LOCAL"' EXIT

OURS="$TMPDIR_LOCAL/ours"
THEIRS="$TMPDIR_LOCAL/theirs"
DIRECT="$TMPDIR_LOCAL/direct"
WATCHED="$TMPDIR_LOCAL/watched"

git diff --name-only "$BASE_SHA..HEAD" | sort -u > "$OURS"
git diff --name-only "$BASE_SHA..$UPSTREAM" | sort -u > "$THEIRS"
comm -12 "$OURS" "$THEIRS" > "$DIRECT"

grep -E \
'^(AGENTS\.md|internal/(harness|orchestrator|config|app|cli)/)' \
"$THEIRS" > "$WATCHED" || true

echo "============================================================"
echo " SPYNEL UPSTREAM IMPACT CHECK"
echo "============================================================"
echo
echo "Rama local : $(git branch --show-current)"
echo "HEAD local : $HEAD_SHA"
echo "Upstream   : $UPSTREAM"
echo "HEAD remoto: $UPSTREAM_SHA"
echo "Merge base : $BASE_SHA"
echo

if [ "$BASE_SHA" = "$UPSTREAM_SHA" ]; then
    echo "UPSTREAM STATUS"
    echo "  OK - no hay commits oficiales nuevos respecto de nuestra base."
else
    COUNT="$(git rev-list --count "$BASE_SHA..$UPSTREAM")"
    echo "UPSTREAM STATUS"
    echo "  NUEVO - hay $COUNT commit(s) oficial(es) posteriores a nuestra base."
fi

echo
echo "ARCHIVOS MODIFICADOS POR NUESTRO FORK"
if [ -s "$OURS" ]; then
    sed 's/^/  /' "$OURS"
else
    echo "  ninguno"
fi

echo
echo "ARCHIVOS MODIFICADOS POR UPSTREAM"
if [ -s "$THEIRS" ]; then
    sed 's/^/  /' "$THEIRS"
else
    echo "  ninguno"
fi

echo
echo "RIESGO DIRECTO"
if [ -s "$DIRECT" ]; then
    echo "  ATENCION: upstream y nuestro fork modifican los mismos archivos:"
    sed 's/^/  ! /' "$DIRECT"
else
    echo "  OK - no hay archivos modificados por ambos lados."
fi

echo
echo "ZONAS ARQUITECTONICAS A VIGILAR"
if [ -s "$WATCHED" ]; then
    echo "  Upstream modificó partes sensibles para nuestro multi-harness:"
    sed 's/^/  ? /' "$WATCHED"
else
    echo "  OK - upstream no modificó las zonas críticas vigiladas."
fi

echo
echo "SIMULACION DE MERGE (NO MODIFICA EL REPOSITORIO)"
set +e
MERGE_OUTPUT="$(git merge-tree --write-tree HEAD "$UPSTREAM" 2>&1)"
MERGE_STATUS=$?
set -e

if [ "$MERGE_STATUS" -eq 0 ]; then
    echo "  OK - Git puede combinar ambas historias sin conflicto textual."
else
    echo "  CONFLICTO POTENCIAL"
    echo "$MERGE_OUTPUT" | sed 's/^/  /'
fi

echo
echo "============================================================"

if [ -s "$DIRECT" ] || [ "$MERGE_STATUS" -ne 0 ]; then
    echo "RESULTADO: REVISION MANUAL NECESARIA"
    exit 2
elif [ -s "$WATCHED" ]; then
    echo "RESULTADO: COMPATIBLE, PERO REVISAR ARQUITECTURA"
    exit 1
else
    echo "RESULTADO: BAJO RIESGO"
fi
