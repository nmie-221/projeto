import os
import json
import pandas as pd
from typing import List, Optional, Dict, Any
import awswrangler as wr

class GeradorParametros:
    def __init__(
        self,
        grao_estrutura: str,
        grao_periodo: str,
        anomes_inicio: int,
        anomes_fim: int,
        definir_blocos: bool = True,
        lista_blocos: Optional[List[str]] = None,
        definir_variaveis: bool = True,
        lista_variaveis: Optional[List[str]] = None,
        definir_estruturas: bool = True,
        lista_estruturas: Optional[List[str]] = None,
        gerar_relatorio: bool = True
    ):
        self.grao_estrutura = grao_estrutura
        self.grao_periodo = grao_periodo
        self.anomes_inicio = anomes_inicio
        self.anomes_fim = anomes_fim
        self.definir_blocos = definir_blocos
        self.lista_blocos = lista_blocos or []
        self.definir_variaveis = definir_variaveis
        self.lista_variaveis = lista_variaveis or []
        self.definir_estruturas = definir_estruturas
        self.lista_estruturas = lista_estruturas or []
        self.gerar_relatorio = gerar_relatorio
        self.config_dir = os.path.join(os.path.dirname(__file__), 'config_vars')

    def obter_filtros(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filtra o DataFrame de acordo com o período (anomes) e, opcionalmente, por estruturas específicas.
        Parâmetros:
            df (pd.DataFrame): DataFrame de entrada.
        Retorna:
            pd.DataFrame: DataFrame filtrado.
        """
        mask = (df['anomes'] >= self.anomes_inicio) & (df['anomes'] <= self.anomes_fim)
        df_filtrado = df[mask]
        if self.definir_estruturas and self.lista_estruturas:
            df_filtrado = df_filtrado[df_filtrado[self.grao_estrutura].isin(self.lista_estruturas)]
        return df_filtrado

    def obter_variaveis_json(self) -> Dict[str, Dict[str, Any]]:
        """
        Lê os arquivos de configuração JSON e retorna as variáveis e suas propriedades, considerando filtros de blocos e variáveis.
        Retorna:
            dict: Dicionário de variáveis e propriedades.
        """
        variaveis = {}
        blocos = self.lista_blocos if (self.definir_blocos and self.lista_blocos) else os.listdir(self.config_dir)
        for bloco in blocos:
            estrutura = self.grao_estrutura
            path = os.path.join(self.config_dir, bloco, f'{estrutura}.json')
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    for var, props in config.get('variaveis', {}).items():
                        if (not self.definir_variaveis) or (not self.lista_variaveis) or (var in self.lista_variaveis):
                            variaveis[var] = props
        return variaveis

    def obter_aggs(self, variaveis: Dict[str, Dict[str, Any]]):
        """
        Define o dicionário de agregações (agg_dict) e as listas de variáveis médias, percentuais e de contagem distinta,
        de acordo com as funções especificadas nos JSONs.
        Parâmetros:
            variaveis (dict): Dicionário de variáveis e propriedades.
        Retorna:
            tuple: (agg_dict, medias, percentuais, distintos)
        """
        agg_dict = {}
        medias = []
        percentuais = []
        distintos = []
        for var, props in variaveis.items():
            funcao = props.get('funcao')
            if funcao == 'soma':
                agg_dict[var] = 'sum'
            elif funcao == 'media':
                params = props.get('params', {})
                nom = params.get('nominador')
                den = params.get('denominador')
                if nom:
                    agg_dict[nom] = 'sum'
                if den:
                    agg_dict[den] = 'sum'
                medias.append((var, props))
            elif funcao == 'percentual':
                params = props.get('params', {})
                nom = params.get('nominador')
                den = params.get('denominador')
                if nom:
                    agg_dict[nom] = 'sum'
                if den:
                    agg_dict[den] = 'sum'
                percentuais.append((var, props))
            elif funcao == 'contagem_distinta':
                agg_dict[var] = 'nunique'
                distintos.append((var, props))
        return agg_dict, medias, percentuais, distintos

    def obter_chaves_groupby(self, df: pd.DataFrame):
        """
        Retorna as chaves de agrupamento (groupby) de acordo com a granularidade e o período definidos.
        Parâmetros:
            df (pd.DataFrame): DataFrame de entrada.
        Retorna:
            list: Lista de colunas para groupby.
        """
        if self.grao_estrutura == 'squad':
            if self.grao_periodo == 'acumulado_mensal':
                chaves = ['id_squad']
            elif self.grao_periodo == 'mensal':
                chaves = ['anomes', 'id_comunidade', 'id_release_train', 'id_squad']
            else:
                chaves = ['id_squad']
        elif self.grao_estrutura == 'release_train':
            if self.grao_periodo == 'acumulado_mensal':
                chaves = ['id_release_train']
            elif self.grao_periodo == 'mensal':
                chaves = ['anomes', 'id_comunidade', 'id_release_train']
            else:
                chaves = ['id_release_train']
        elif self.grao_estrutura == 'comunidade':
            if self.grao_periodo == 'acumulado_mensal':
                chaves = ['id_comunidade']
            elif self.grao_periodo == 'mensal':
                chaves = ['anomes', 'id_comunidade']
            else:
                chaves = ['id_comunidade']
        else:
            chaves = []
        return [c for c in chaves if c in df.columns]

    def aplicar_groupby(self, df: pd.DataFrame, agg_dict, medias, percentuais, distintos):
        """
        Aplica o groupby no DataFrame, usando as agregações personalizadas e calcula médias, percentuais e contagem distinta, se necessário.
        Parâmetros:
            df (pd.DataFrame): DataFrame de entrada.
            agg_dict (dict): Dicionário de agregações.
            medias (list): Lista de variáveis médias.
            percentuais (list): Lista de variáveis percentuais.
            distintos (list): Lista de variáveis de contagem distinta.
        Retorna:
            pd.DataFrame: DataFrame agrupado e agregado.
        """
        chaves = self.obter_chaves_groupby(df)
        if not chaves:
            raise ValueError('Nenhuma chave de agrupamento encontrada!')
        for chave in chaves:
            if chave not in df.columns:
                df[chave] = pd.NA
        df_agg = df.groupby(chaves, as_index=False).agg(agg_dict)
        # Calcula médias customizadas (soma do nominador / soma do denominador)
        for var, props in medias:
            params = props.get('params', {})
            nom = params.get('nominador')
            den = params.get('denominador')
            if nom in df_agg.columns and den in df_agg.columns:
                df_agg[var] = df_agg[nom] / df_agg[den].replace(0, pd.NA)
            else:
                df_agg[var] = pd.NA
        # Calcula percentuais customizados (soma do nominador / soma do denominador)
        for var, props in percentuais:
            params = props.get('params', {})
            nom = params.get('nominador')
            den = params.get('denominador')
            if nom in df_agg.columns and den in df_agg.columns:
                df_agg[var] = df_agg[nom] / df_agg[den].replace(0, pd.NA)
            else:
                df_agg[var] = pd.NA
        # Contagem distinta já é feita pelo groupby/agg, nada extra necessário
        colunas_finais = chaves + [v for v in agg_dict.keys() if v not in chaves] + [v for v, _ in medias] + [v for v, _ in percentuais]
        for chave in chaves:
            if chave not in df_agg.columns:
                df_agg[chave] = pd.NA
        return df_agg[colunas_finais]

    def gerar_relatorio_resumido(self, df: pd.DataFrame):
        """
        Gera um relatório resumido do DataFrame final, incluindo contagem de não nulos, ids distintos e períodos únicos.
        Parâmetros:
            df (pd.DataFrame): DataFrame de entrada.
        Retorna:
            dict: Resumo com contagem, ids distintos e períodos.
        """
        id_map = {
            'squad': 'id_squad',
            'comunidade': 'id_comunidade',
            'release_train': 'id_release_train'
        }
        id_col = id_map.get(self.grao_estrutura, 'id')
        resumo = {
            'contagem_nao_nulos': df.count(),
            'ids_distintos': df[id_col].nunique() if id_col in df.columns else None,
            'anomes': df['anomes'].unique() if 'anomes' in df.columns else None
        }
        return resumo

    def processar(self, df: pd.DataFrame):
        """
        Função principal: executa o fluxo completo de geração da base e retorna o DataFrame final e o relatório.
        Parâmetros:
            df (pd.DataFrame): DataFrame de entrada.
        Retorna:
            tuple: (DataFrame final, relatório)
        """
        df_filtrado = self.obter_filtros(df)
        variaveis = self.obter_variaveis_json()
        agg_dict, medias, percentuais, distintos = self.obter_aggs(variaveis)
        df_final = self.aplicar_groupby(df_filtrado, agg_dict, medias, percentuais, distintos)
        relatorio = self.gerar_relatorio_resumido(df_final) if self.gerar_relatorio else None
        return df_final, relatorio

    def ler_tabela_athena(self, tabela, database, boto3_session=None, col_anomes='anomes', **kwargs):
        """
        Lê uma tabela do Athena já aplicando o filtro de anomes, usando awswrangler.
        Parâmetros:
            tabela (str): Nome da tabela Athena.
            database (str): Nome do banco de dados Athena.
            boto3_session: Sessão boto3 opcional.
            col_anomes (str): Nome da coluna de período.
            **kwargs: Parâmetros adicionais para o awswrangler.
        Retorna:
            pd.DataFrame: DataFrame lido do Athena.
        """
        query = f'''
            SELECT * FROM {tabela}
            WHERE {col_anomes} >= {self.anomes_inicio}
              AND {col_anomes} <= {self.anomes_fim}
        '''
        df = wr.athena.read_sql_query(
            query,
            database=database,
            boto3_session=boto3_session,
            **kwargs
        )
        return df

    def processar_athena(self, tabela, database, boto3_session=None, col_anomes='anomes', **kwargs):
        """
        Lê os dados do Athena, processa e retorna o resultado final e o relatório.
        Parâmetros:
            tabela (str): Nome da tabela Athena.
            database (str): Nome do banco de dados Athena.
            boto3_session: Sessão boto3 opcional.
            col_anomes (str): Nome da coluna de período.
            **kwargs: Parâmetros adicionais para o awswrangler.
        Retorna:
            tuple: (DataFrame final, relatório)
        """
        df = self.ler_tabela_athena(tabela, database, boto3_session, col_anomes, **kwargs)
        return self.processar(df)

    def help(self):
        """
        Exibe um help detalhado e visual para o usuário, com instruções e exemplos de uso.
        """
        print("""
============================================================
|                 GeradorParametros - Ajuda                |
============================================================

*** Classe para geração de bases de variáveis customizadas ***

==================== COMO USAR (PASSO A PASSO) ====================

1. Importe a classe no seu script ou notebook:
    from hub_variaveis.gerador_base import GeradorParametros

2. (Opcional) Importe o pandas para ler arquivos CSV:
    import pandas as pd
    df = pd.read_csv('caminho/para/seu_arquivo.csv')

3. Crie uma instância da classe com os parâmetros desejados:
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

4. Para processar um DataFrame local:
    df_final, relatorio = gerador.processar(df)
    print(df_final)
    print(relatorio)

5. Para processar direto do Athena (em ambiente AWS/SageMaker):
    import boto3
    boto3_session = boto3.Session()
    df_final, relatorio = gerador.processar_athena(
        tabela='nome_da_tabela',
        database='nome_do_database',
        boto3_session=boto3_session
    )
    print(df_final)
    print(relatorio)

==================== PARÂMETROS PRINCIPAIS ====================
- grao_estrutura:        'squad', 'comunidade' ou 'release_train'
- grao_periodo:          'mensal' ou 'acumulado_mensal'
- anomes_inicio, anomes_fim: período de análise (ex: 202401, 202412)
- definir_blocos:        se True, filtra blocos pelos nomes em lista_blocos
- lista_blocos:          lista de blocos a considerar (ex: ['bugs'])
- definir_variaveis:     se True, filtra variáveis pelos nomes em lista_variaveis
- lista_variaveis:       lista de variáveis a considerar
- definir_estruturas:    se True, filtra estruturas pelos nomes em lista_estruturas
- lista_estruturas:      lista de estruturas a considerar
- gerar_relatorio:       se True, gera um resumo do resultado

==================== MÉTODOS PRINCIPAIS ====================
- processar(df):         executa todo o fluxo de geração da base e retorna (df_final, relatorio)
- processar_athena(...): executa todo o fluxo lendo direto do Athena
- help():               imprime esta mensagem de ajuda

==================== AGRUPAMENTOS AUTOMÁTICOS ====================
- Para squad:
    - acumulado_mensal: ['id_squad']
    - mensal: ['anomes', 'id_comunidade', 'id_release_train', 'id_squad']
- Para release_train:
    - acumulado_mensal: ['id_release_train']
    - mensal: ['anomes', 'id_comunidade', 'id_release_train']
- Para comunidade:
    - acumulado_mensal: ['id_comunidade']
    - mensal: ['anomes', 'id_comunidade']

==================== FUNÇÕES DE AGREGAÇÃO SUPORTADAS ====================
- soma, percentual, media, contagem_distinta

==================== EXEMPLO DE USO ====================
gerador = GeradorParametros(grao_estrutura='squad', grao_periodo='mensal', ...)
df_final, relatorio = gerador.processar(df)
df_final2, relatorio2 = gerador.processar_athena('tabela', 'database')
gerador.help()
============================================================
""")
