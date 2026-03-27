import re
import os
from datetime import datetime

# ── Atualizar data no HTML ────────────────────────────────────────────────────
today      = datetime.today()
update_str = today.strftime("%d/%m/%Y")

print(f"Atualizando data para {update_str}...")

with open("index.html", "r", encoding="utf-8") as f:
    html = f.read()

html = re.sub(r'Atualizado em: [\d/]+', f'Atualizado em: {update_str}', html)

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

print("Feito.")
