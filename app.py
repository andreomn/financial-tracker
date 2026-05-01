import csv
import io
import logging
import re
import time
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, render_template, request, send_file

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("financial-tracker")

CVM_ENDPOINT = "https://www.rad.cvm.gov.br/ENET/frmConsultaExternaCVM.aspx/ListarDocumentos"
FRE_BASE_URL = "https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx"
CVM_COMPANIES_CSV_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"
DEFAULT_TIMEOUT = 45
COMPANY_CACHE_TTL = 60 * 60 * 6

_company_cache = {"ts": 0.0, "rows": []}
logger.info("app_starting")


def _normalizar_data(valor: str | None) -> str:
    if not valor:
        return ""
    try:
        return datetime.fromisoformat(valor).strftime("%d/%m/%Y")
    except ValueError:
        return valor


def _normalizar_texto(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", texto.lower()).strip()


def _decode_csv_bytes(content: bytes) -> str:
    for enc in ("utf-8-sig", "latin1", "cp1252"):
        try:
            return content.decode(enc)
        except Exception:
            continue
    return content.decode("latin1", errors="ignore")


def carregar_empresas_cvm() -> list[dict[str, str]]:
    agora = time.time()
    if _company_cache["rows"] and (agora - _company_cache["ts"] < COMPANY_CACHE_TTL):
        return _company_cache["rows"]

    logger.info("carregar_empresas_cvm_fetch_start")
    response = requests.get(CVM_COMPANIES_CSV_URL, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()

    decoded = _decode_csv_bytes(response.content)
    reader = csv.DictReader(io.StringIO(decoded), delimiter=';')
    rows: list[dict[str, str]] = []

    for row in reader:
        codigo = (row.get("CD_CVM") or row.get("CODIGO_CVM") or "").strip()
        nome = (row.get("DENOM_SOCIAL") or row.get("NOME_EMPRESARIAL") or "").strip()
        situacao = (row.get("SIT") or row.get("SITUACAO") or "").strip().upper()
        if not codigo or not nome:
            continue
        rows.append({"codigo_cvm": codigo, "nome": nome, "nome_norm": _normalizar_texto(nome), "situacao": situacao})

    if not rows:
        raise RuntimeError("Base de empresas da CVM veio vazia ou com colunas inesperadas.")

    _company_cache.update({"ts": agora, "rows": rows})
    logger.info("carregar_empresas_cvm_fetch_done total=%s", len(rows))
    return rows


def encontrar_empresa(consulta: str) -> dict[str, str] | None:
    consulta_norm = _normalizar_texto(consulta)
    if not consulta_norm:
        return None

    empresas = carregar_empresas_cvm()
    candidatas = [e for e in empresas if consulta_norm in e["nome_norm"]]

    if not candidatas:
        scored = sorted(empresas, key=lambda e: SequenceMatcher(None, consulta_norm, e["nome_norm"]).ratio(), reverse=True)
        candidatas = [e for e in scored[:20] if SequenceMatcher(None, consulta_norm, e["nome_norm"]).ratio() >= 0.45]

    if not candidatas:
        return None

    candidatas = sorted(candidatas, key=lambda e: (0 if e["situacao"] == "ATIVO" else 1, -SequenceMatcher(None, consulta_norm, e["nome_norm"]).ratio(), len(e["nome"])))
    return candidatas[0]


def _parse_links_from_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    for anchor in soup.select("a[href*='frmGerenciaPaginaFRE.aspx']"):
        href = anchor.get("href", "")
        match = re.search(r"NumeroSequencialDocumento=\d+", href)
        if not match:
            continue
        numero = match.group(0).split("=")[1]
        urls.append(f"{FRE_BASE_URL}?NumeroSequencialDocumento={numero}&CodigoTipoInstituicao=1")
    return list(dict.fromkeys(urls))


def listar_dfps_por_codigo(codigo_cvm: str, data_inicial: str = "", data_final: str = "") -> list[str]:
    payload: dict[str, Any] = {"codigoCVM": codigo_cvm, "dataIni": _normalizar_data(data_inicial), "dataFim": _normalizar_data(data_final), "tipoDocumento": "DFP", "setorAtividade": "", "categoriaEmissor": "", "situacaoDocumento": ""}
    logger.info("listar_dfps_por_codigo codigo=%s", codigo_cvm)
    response = requests.post(CVM_ENDPOINT, json=payload, timeout=DEFAULT_TIMEOUT)
    response.raise_for_status()
    body = response.json()
    html = body.get("d", "") if isinstance(body, dict) else ""
    links = _parse_links_from_html(html)
    logger.info("listar_dfps_por_codigo_done codigo=%s total_links=%s", codigo_cvm, len(links))
    return links


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


@app.errorhandler(Exception)
def handle_exception(exc):
    logger.exception("unhandled_exception")
    return jsonify({"erro": f"erro interno: {exc}"}), 500


@app.get("/")
def home():
    logger.info("route_home")
    return render_template("index.html")


@app.get("/health")
def health() -> tuple[dict[str, str], int]:
    return {"status": "ok"}, 200


@app.get("/empresas")
def listar_empresas():
    page = max(1, int(request.args.get("page", 1)))
    size = min(200, max(20, int(request.args.get("size", 100))))
    try:
        rows = carregar_empresas_cvm()
    except Exception as exc:
        logger.exception("listar_empresas_fail")
        return jsonify({"erro": f"falha ao carregar base de empresas CVM: {exc}"}), 502
    total = len(rows)
    start = (page - 1) * size
    end = start + size
    view = [{"codigo_cvm": e["codigo_cvm"], "nome": e["nome"], "situacao": e["situacao"]} for e in rows[start:end]]
    return jsonify({"total": total, "page": page, "size": size, "empresas": view})


@app.post("/buscar-dfps")
def buscar_dfps():
    data = request.get_json(silent=True) or {}
    empresa_consulta = str(data.get("empresa", "")).strip()
    logger.info("buscar_dfps empresa=%s", empresa_consulta)
    if not empresa_consulta:
        return jsonify({"erro": "campo 'empresa' é obrigatório"}), 400

    try:
        empresa = encontrar_empresa(empresa_consulta)
        if not empresa:
            return jsonify({"erro": "empresa não encontrada na base da CVM"}), 404
        links = listar_dfps_por_codigo(empresa["codigo_cvm"])
    except Exception as exc:
        logger.exception("buscar_dfps_fail")
        return jsonify({"erro": f"falha ao consultar CVM: {exc}"}), 502

    if not links:
        return jsonify({"erro": "nenhuma DFP encontrada para a empresa informada"}), 404

    return jsonify({"empresa": empresa["nome"], "codigo_cvm": empresa["codigo_cvm"], "links": links})


@app.post("/fill-dfp")
def fill_dfp():
    data = request.get_json(silent=True) or {}
    empresa_consulta = str(data.get("empresa", "")).strip()
    data_inicial = str(data.get("data_inicial", "")).strip()
    data_final = str(data.get("data_final", "")).strip()
    logger.info("fill_dfp empresa=%s", empresa_consulta)

    if not empresa_consulta:
        return jsonify({"erro": "campo 'empresa' é obrigatório"}), 400

    try:
        empresa = encontrar_empresa(empresa_consulta)
        if not empresa:
            return jsonify({"erro": "empresa não encontrada na base da CVM"}), 404
        links_dfp = listar_dfps_por_codigo(empresa["codigo_cvm"], data_inicial, data_final)
    except Exception as exc:
        logger.exception("fill_dfp_lookup_fail")
        return jsonify({"erro": f"falha ao consultar CVM: {exc}"}), 502

    if not links_dfp:
        return jsonify({"erro": "nenhuma DFP encontrada para os filtros informados"}), 404

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        pd.DataFrame({"empresa": [empresa["nome"]], "codigo_cvm": [empresa["codigo_cvm"]]}).to_excel(writer, sheet_name="empresa", index=False)
        pd.DataFrame({"link_dfp": links_dfp}).to_excel(writer, sheet_name="resumo", index=False)
        for i, link in enumerate(links_dfp, start=1):
            try:
                df = extrair_tabelas_dfp(link)
            except Exception as exc:
                logger.exception("fill_dfp_extract_fail link=%s", link)
                df = pd.DataFrame({"erro": [str(exc)], "link_dfp": [link]})
            df.to_excel(writer, sheet_name=f"dfp_{i}"[:31], index=False)

    output.seek(0)
    nome_arquivo = re.sub(r"[^a-zA-Z0-9_-]", "_", empresa["nome"])
    logger.info("fill_dfp_done empresa=%s total_links=%s", empresa["codigo_cvm"], len(links_dfp))
    return send_file(output, as_attachment=True, download_name=f"dfp_cvm_{nome_arquivo}.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
