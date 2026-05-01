# financial-tracker

Serviço Flask para consultar DFPs da CVM por **nome da empresa**, listar links e gerar um Excel consolidado.

## Funcionalidades

- Interface web (`GET /`) com campo de busca por nome da empresa.
- Ao pressionar Enter, consulta as DFPs mais recentes na CVM e mostra links em tabela.
- Botão para baixar Excel com todas as DFPs encontradas.

## Endpoints

- `GET /` página HTML.
- `GET /health` healthcheck.
- `POST /buscar-dfps` lista links DFP por empresa.
- `POST /fill-dfp` gera Excel consolidado.

### Exemplo de payload

```json
{
  "empresa": "Petrobras",
  "data_inicial": "2025-01-01",
  "data_final": "2025-12-31"
}
```

## Docker

```bash
docker build -t financial-tracker .
docker run -p 8080:8080 financial-tracker
```
