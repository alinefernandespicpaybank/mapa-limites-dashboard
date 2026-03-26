import sys
import time
import json
import re
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# ── Conexão ──────────────────────────────────────────────────────────────────
DATABRICKS_HOST = "https://picpay-principal.cloud.databricks.com"
DATABRICKS_TOKEN = "DATABRICKS_PAT_PLACEHOLDER"  # substituído pelo GitHub Secret
WH_ID = "3b94f0935afb32db"
TAG = "/* source:hubai_nitro */"

w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

# ── Helpers ───────────────────────────────────────────────────────────────────
def run(sql, label, timeout=300):
    r = w.statement_execution.execute_statement(
        warehouse_id=WH_ID, statement=TAG + "\n" + sql, wait_timeout="0s"
    )
    sid = r.statement_id
    print(f"  [{label}] submetida: {sid}")
    for i in range(timeout // 5):
        s = w.statement_execution.get_statement(sid)
        if s.status.state == StatementState.SUCCEEDED:
            cols = [c.name for c in s.manifest.schema.columns]
            rows = s.result.data_array or []
            print(f"  [{label}] OK — {len(rows)} linhas")
            return cols, rows
        elif s.status.state in (StatementState.FAILED, StatementState.CANCELED):
            print(f"  [{label}] ERRO: {s.status.error}")
            return None, None
        time.sleep(5)
    print(f"  [{label}] TIMEOUT")
    return None, None

# ── Queries ───────────────────────────────────────────────────────────────────
print("Executando queries...")

# Antecipação de fatura
_, rows_antec = run("""
WITH BASE_FATURA AS (
  SELECT
    date_format(add_months(due_date, 1), 'yyyy-MM') AS mes_ref,
    COUNT(DISTINCT consumer_id) AS QTD,
    CASE
      WHEN payment > invoice_value THEN 'overpaid'
      WHEN payment < invoice_value THEN 'underpaid'
      ELSE 'paid'
    END AS status,
    SUM(payment - invoice_value) AS valor_final
  FROM picpay.card_operations.fis_invoices
  WHERE date_format(add_months(due_date, 1), 'yyyy-MM') <= date_format(current_date(), 'yyyy-MM')
  GROUP BY ALL
)
SELECT QTD, mes_ref, status
FROM BASE_FATURA
WHERE status IN ('overpaid')
  AND mes_ref >= '2025-01'
ORDER BY mes_ref
""", "antecipacao")

# ── Montar dados ──────────────────────────────────────────────────────────────
def to_int_list(rows, col_idx=0):
    return [int(r[col_idx]) for r in rows]

def month_label(yyyymm):
    """'2025-01' → 'jan/25'"""
    months = ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez"]
    y, m = yyyymm.split("-")
    return f"{months[int(m)-1]}/{y[2:]}"

# Antecipação
antec_values = to_int_list(rows_antec, 0) if rows_antec else []
antec_months = [month_label(r[1]) for r in rows_antec] if rows_antec else []
# current = penúltimo (último mês fechado)
antec_current = antec_values[-2] if len(antec_values) >= 2 else antec_values[-1] if antec_values else 0

# Data de referência dinâmica
today = datetime.today()
last_closed = month_label(f"{today.year}-{today.month-1:02d}" if today.month > 1 else f"{today.year-1}-12")
current_partial = month_label(f"{today.year}-{today.month:02d}")
last_update = today.strftime("%-d %b. %Y").lower()

print(f"  Antecipação: {len(antec_values)} meses, current={antec_current:,}")

# ── Atualizar HTML ────────────────────────────────────────────────────────────
print("Atualizando HTML...")

with open("index.html", "r", encoding="utf-8") as f:
    html = f.read()

def replace_data(html, key, new_values, new_current=None, extra_fields=None):
    """Substitui values e current de um bloco no DATA do JS."""
    vals_str = "[" + ",".join(str(v) for v in new_values) + "]"
    # values
    html = re.sub(
        rf'({re.escape(key)}:.*?values:\s*)\[[^\]]*\]',
        lambda m: m.group(1) + vals_str,
        html, flags=re.DOTALL
    )
    # current
    if new_current is not None:
        html = re.sub(
            rf'({re.escape(key)}:.*?current:\s*)\d+',
            lambda m: m.group(1) + str(new_current),
            html, flags=re.DOTALL
        )
    return html

# Atualizar antecipação
if antec_values:
    html = replace_data(html, "antecipacao", antec_values, antec_current)

# Atualizar data de atualização no nav
html = re.sub(
    r'Atualizado em: [\d/]+',
    f'Atualizado em: {today.strftime("%d/%m/%Y")}',
    html
)

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

print("HTML atualizado com sucesso.")
print(json.dumps({
    "antecipacao_meses": len(antec_values),
    "antecipacao_current": antec_current,
    "data_atualizacao": today.strftime("%d/%m/%Y")
}, indent=2))
