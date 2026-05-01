# financial-tracker

Serviço Flask para automatizar o preenchimento de dados financeiros com base nas DFPs públicas da CVM.

## O que ele faz

1. Faz request para o endpoint `ListarDocumentos` da CVM filtrando por `DFP`.
2. Coleta os links `frmGerenciaPaginaFRE.aspx` retornados.
3. Abre cada DFP, extrai tabelas e consolida tudo em um arquivo Excel.
4. Retorna o Excel pronto para download.

## Endpoint principal

`POST /fill-dfp`

### JSON de entrada

```json
{
  "codigo_cvm": "9512",
  "data_inicial": "2025-01-01",
  "data_final": "2025-12-31"
}
```

- `codigo_cvm` é obrigatório.
- Datas são opcionais e aceitam ISO (`YYYY-MM-DD`) ou texto já em `DD/MM/YYYY`.

### Resposta

Arquivo `.xlsx` com:
- Aba `resumo`: lista dos links DFP encontrados.
- Abas `dfp_1`, `dfp_2`, ... com as tabelas extraídas de cada documento.

## Rodando com Docker

```bash
docker build -t financial-tracker .
docker run -p 8080:8080 financial-tracker
```
