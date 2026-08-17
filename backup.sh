#!/bin/bash
# backup.sh
# Backup скрипта за Crypto Analytics Platform
# Стартувај ја пред секоја поголема промена во кодот/структурата.
#
# Употреба (од WSL2 терминал, во root на проектот, пр. /mnt/d/airflow):
#   chmod +x backup.sh   (само првиот пат)
#   ./backup.sh

set -e  # застани веднаш ако нешто фејлне

# ---------- ПОДЕСУВАЊА (смени ако е потребно) ----------
DB_CONTAINER="analytics_db"
DB_USER="admin"
DB_NAME="analytics"
ENV_FILE=".env"
BACKUP_DIR="./backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DUMP_FILE="backup_${TIMESTAMP}.dump"
# ---------------------------------------------------------

echo "=========================================="
echo " Backup стартува: $TIMESTAMP"
echo "=========================================="

mkdir -p "$BACKUP_DIR"

# 1) pg_dump на analytics базата
echo ""
echo "[1/5] Правам pg_dump на базата '$DB_NAME'..."
docker exec "$DB_CONTAINER" pg_dump -U "$DB_USER" -d "$DB_NAME" -F c -f "/tmp/$DUMP_FILE"
docker cp "$DB_CONTAINER:/tmp/$DUMP_FILE" "$BACKUP_DIR/$DUMP_FILE"
docker exec "$DB_CONTAINER" rm -f "/tmp/$DUMP_FILE"

# 2) Провери дека dump-от не е празен и дека е валиден
echo ""
echo "[2/5] Проверувам валидност на dump-от..."
DUMP_SIZE=$(stat -c%s "$BACKUP_DIR/$DUMP_FILE" 2>/dev/null || stat -f%z "$BACKUP_DIR/$DUMP_FILE")
if [ "$DUMP_SIZE" -lt 1000 ]; then
    echo "  ГРЕШКА: dump фајлот е премал ($DUMP_SIZE bytes) - веројатно нешто не е во ред!"
    exit 1
fi
TABLE_COUNT=$(docker exec "$DB_CONTAINER" pg_restore -l "/tmp/$DUMP_FILE" 2>/dev/null | grep -c "TABLE DATA" || true)
echo "  Dump е ${DUMP_SIZE} bytes, содржи податоци за табели: OK"

# 3) Прикажи снапшот на etl_control (watermark состојба во моментот на backup)
echo ""
echo "[3/5] Снимам состојба на etl_control (watermark)..."
docker exec "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "SELECT * FROM public.etl_control;" \
    > "$BACKUP_DIR/etl_control_${TIMESTAMP}.txt" 2>/dev/null || echo "  (etl_control не постои или е недостапна - прескокнато)"
echo "  Зачувано во $BACKUP_DIR/etl_control_${TIMESTAMP}.txt"

# 4) Копирај .env на безбедно место (не е во git)
echo ""
echo "[4/5] Копирам .env фајл..."
if [ -f "$ENV_FILE" ]; then
    cp "$ENV_FILE" "$BACKUP_DIR/env_${TIMESTAMP}.bak"
    echo "  .env зачуван во $BACKUP_DIR/env_${TIMESTAMP}.bak"
else
    echo "  ВНИМАНИЕ: .env не е најден во тековниот директориум - прескокнато."
fi

# 5) Git checkpoint на тековната состојба на кодот
echo ""
echo "[5/5] Git checkpoint..."
if [ -d ".git" ]; then
    git add -A
    if git diff --cached --quiet; then
        echo "  Нема промени за commit - работната состојба е веќе чиста."
    else
        git commit -m "checkpoint пред restructuring - $TIMESTAMP"
        echo "  Commit направен."
    fi
else
    echo "  ВНИМАНИЕ: не е git repo во овој директориум - прескокнато."
fi

echo ""
echo "=========================================="
echo " Backup завршен успешно: $BACKUP_DIR/$DUMP_FILE"
echo "=========================================="
echo ""
echo "За да провериш restore (опционално, еднаш):"
echo "  docker exec $DB_CONTAINER createdb -U $DB_USER test_restore"
echo "  docker cp $BACKUP_DIR/$DUMP_FILE $DB_CONTAINER:/tmp/test.dump"
echo "  docker exec $DB_CONTAINER pg_restore -U $DB_USER -d test_restore /tmp/test.dump"
echo "  docker exec $DB_CONTAINER psql -U $DB_USER -d test_restore -c \"SELECT count(*) FROM int_crypto_prices;\""
echo "  docker exec $DB_CONTAINER dropdb -U $DB_USER test_restore"
