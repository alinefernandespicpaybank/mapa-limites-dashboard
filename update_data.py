import sys
import time
import json
import re
import os
import requests
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# ── Config ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST  = "https://picpay-principal.cloud.databricks.com"
DATABRICKS_TOKEN = os.environ.get("DB_PAT_TOKEN", "")
if not DATABRICKS_TOKEN:
    raise ValueError("DB_PAT_TOKEN não definido")

WH_ID = "3b94f0935afb32db"
TAG   = "/* source:hubai_nitro */"

NITRO_USER_TOKEN   = os.environ.get("NITRO_USER_TOKEN", "e11cd9ab771f")
NITRO_API_URL      = "https://nitro-link-api.ppay.me"
NITRO_API_FALLBACK = "https://ahfxd8cc43.execute-api.us-east-1.amazonaws.com"

# ── Databricks ────────────────────────────────────────────────────────────────
w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

def run_query(sql, label, timeout=300):
    r = w.statement_execution.execute_statement(
        warehouse_id=WH_ID,
        statement=TAG + "\n" + sql,
        wait_timeout="0s"
    )
    sid = r.statement_id
    print(f"  [{label}] submetida: {sid}")
    for _ in range(timeout // 5):
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
print("Executando queries no Databricks...")

_, rows_antec = run_query("""
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
def month_label(yyyymm):
    months = ["jan","fev","mar","abr","mai","jun","jul","ago","set","out","nov","dez"]
    y, m = yyyymm.split("-")
    return f"{months[int(m)-1]}/{y[2:]}"

antec_values  = [int(r[0]) for r in rows_antec] if rows_antec else []
antec_current = antec_values[-2] if len(antec_values) >= 2 else (antec_values[-1] if antec_values else 0)

today      = datetime.today()
update_str = today.strftime("%d/%m/%Y")

print(f"  antecipação: {len(antec_values)} meses, fev/26={antec_current:,}")

# ── Atualizar HTML ────────────────────────────────────────────────────────────
print("Atualizando index.html...")

with open("index.html", "r", encoding="utf-8") as f:
    html = f.read()

if antec_values:
    vals_str = "[" + ",".join(str(v) for v in antec_values) + "]"
    html = re.sub(
        r'(antecipacao:\s*\{[^}]*values:\s*)\[[^\]]*\]',
        lambda m: m.group(1) + vals_str,
        html, flags=re.DOTALL
    )
    html = re.sub(
        r'(antecipacao:\s*\{[^}]*current:\s*)\d+',
        lambda m: m.group(1) + str(antec_current),
        html, flags=re.DOTALL
    )

html = re.sub(r'Atualizado em: [\d/]+', f'Atualizado em: {update_str}', html)

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

print("  index.html atualizado.")

# ── Publicar no nitro-link ────────────────────────────────────────────────────
print("Publicando no nitro-link (link fixo via pasta)...")

def call_lambda(payload):
    for url in [NITRO_API_URL, NITRO_API_FALLBACK]:
        try:
            r = requests.post(url, json=payload, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"  Lambda {url} falhou: {e}")
    raise RuntimeError("Lambda API inacessível")

key        = f"html/{NITRO_USER_TOKEN}/f/mapa-limites/mapa-limites-dashboard.html"
public_url = f"https://nitro-link.ppay.me/{key}"

result     = call_lambda({"action": "upload", "key": key, "content_type": "text/html"})
upload_url = result["upload_url"]

with open("index.html", "rb") as f:
    content = f.read()

put = requests.put(upload_url, data=content, headers={"Content-Type": "text/html"}, timeout=60)
put.raise_for_status()

print(f"  Publicado com sucesso!")
print(f"  URL fixa: {public_url}")

with open("last_published_url.txt", "w") as f:
    f.write(public_url)
