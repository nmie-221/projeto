# Hub Variaveis - Geração de Bases de Variáveis Parametrizáveis

Este projeto contém uma classe Python flexível para geração de bases de métricas customizadas, a partir de dados locais ou de tabelas Athena, com configuração via arquivos JSON.

## Principais recursos
- Suporte a diferentes granularidades (squad, comunidade, release_train).
- Leitura de dados locais (DataFrame) ou direto do Athena (via awswrangler).
- Configuração de variáveis, blocos e regras de agregação via JSON.
- Funções de agregação suportadas: soma, média, percentual, contagem_distinta.
- Filtros por período, estrutura, bloco e variável.
- Geração de relatório resumido.
- Compatível com ambientes locais e SageMaker.
- Help detalhado com exemplos.

## Instalação

1. Clone este repositório.
2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

## Exemplo de uso

```python
from hub_variaveis import GeradorParametros
import pandas as pd

df = pd.read_csv('base_teste.csv')
gerador = GeradorParametros(
    grao_estrutura='squad',
    grao_periodo='mensal',
    anomes_inicio=202401,
    anomes_fim=202412,
    definir_blocos=True,
    lista_blocos=['bugs'],
    definir_variaveis=False,
    definir_estruturas=False,
    gerar_relatorio=True
)
df_final, relatorio = gerador.processar(df)
print(df_final)
print(relatorio)
```

Para uso com Athena (AWS/SageMaker):
```python
import boto3
boto3_session = boto3.Session()
df_final, relatorio = gerador.processar_athena(
    tabela='nome_da_tabela',
    database='nome_do_database',
    boto3_session=boto3_session
)
```

## Como configurar variáveis e funções de agregação
Veja o arquivo `funcoes_agregacao_exemplos.txt` para exemplos de configuração JSON e guia de manutenção.

## Estrutura dos arquivos JSON
- Os arquivos ficam em `config_vars/<bloco>/<estrutura>.json`.
- Veja o guia no arquivo de exemplos para detalhes.

## Dependências principais
- pandas
- awswrangler

## Suporte
Consulte o help da classe (`gerador.help()`) ou o arquivo de exemplos. Para dúvidas, procure o responsável pelo hub.
