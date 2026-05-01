import io
import re
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request, send_file

app = Flask(__name__)

CVM_ENDPOINT = "https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx/ListarDocumentos"
FRE_BASE_URL = "https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx"
DEFAULT_TIMEOUT = 45


def _normalizar_data(valor: str | None) -> str:
    if not valor:
        return ""
    try:
        return datetime.fromisoformat(valor).strftime("%d/%m/%Y")
    except ValueError:
        return valor


def listar_dfps(codigo_cvm: str, data_inicial: str = "", data_final: str = "") -> list[str]:
    payload: dict[str, Any] = {
        "codigoCVM": codigo_cvm,
        "dataIni": _normalizar_data(data_inicial),
        "dataFim": _normalizar_data(data_final),
        "tipoDocumento": "DFP",
        "setorAtividade": "",
        "categoriaEmissor": "",
        "situacaoDocumento": "",
    }

    response = requests.post(CVM_ENDPOINT, json=payload, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    body = response.json()

    html = body.get("d", "") if isinstance(body, dict) else ""
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    for anchor in soup.select("a[href*='frmGerenciaPaginaFRE.aspx']"):
        href = anchor.get("href", "")
        match = re.search(r"NumeroSequencialDocumento=\d+", href)
        if not match:
            continue
        numero = match.group(0).split("=")[1]
        full_url = f"{FRE_BASE_URL}?NumeroSequencialDocumento={numero}&CodigoTipoInstituicao=1"
        urls.append(full_url)

    # remove duplicados mantendo ordem
    return list(dict.fromkeys(urls))


def extrair_tabelas_dfp(url: str) -> pd.DataFrame:
    response = requests.get(url, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()

    tabelas = pd.read_html(io.StringIO(response.text), flavor="lxml")
    if not tabelas:
        return pd.DataFrame({"info": ["Nenhuma tabela encontrada"]})

    frames = []
    for idx, tabela in enumerate(tabelas, start=1):
        tabela = tabela.copy()
        tabela.insert(0, "dfp_origem", f"Tabela {idx}")
        frames.append(tabela)

    return pd.concat(frames, ignore_index=True)


@app.get("/health")
def health() -> tuple[dict[str, str], int]:
    return {"status": "ok"}, 200


@app.post("/fill-dfp")
def fill_dfp():
    data = request.get_json(silent=True) or {}

    codigo_cvm = str(data.get("codigo_cvm", "")).strip()
    data_inicial = str(data.get("data_inicial", "")).strip()
    data_final = str(data.get("data_final", "")).strip()

    if not codigo_cvm:
        return jsonify({"erro": "campo 'codigo_cvm' é obrigatório"}), 400

    try:
        links_dfp = listar_dfps(codigo_cvm, data_inicial, data_final)
    except Exception as exc:
        return jsonify({"erro": f"falha ao consultar CVM: {exc}"}), 502

    if not links_dfp:
        return jsonify({"erro": "nenhuma DFP encontrada para os filtros informados"}), 404

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        resumo = pd.DataFrame({"link_dfp": links_dfp})
        resumo.to_excel(writer, sheet_name="resumo", index=False)

        for i, link in enumerate(links_dfp, start=1):
            try:
                df = extrair_tabelas_dfp(link)
            except Exception as exc:
                df = pd.DataFrame({"erro": [str(exc)], "link_dfp": [link]})
            df.to_excel(writer, sheet_name=f"dfp_{i}"[:31], index=False)

    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name=f"dfp_cvm_{codigo_cvm}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
