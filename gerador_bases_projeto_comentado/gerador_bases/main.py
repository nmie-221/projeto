import pandas as pd
from gerador_bases.bloco import BlocoGerador

def gerar_dados(estrutura, periodo, mes_inicio, mes_fim, df_entrada,
                definir_blocos=False, lista_blocos=None,
                definir_estruturas=False, lista_estruturas=None,
                definir_variaveis=False, lista_variaveis=None,
                gerar_relatorio=False):
    """
    Parâmetros:
        estrutura, periodo: granularidade
        mes_inicio, mes_fim: filtro período (strings tipo '202301')
        df_entrada: DataFrame com dados originais
        definir_blocos, lista_blocos: filtra blocos de indicadores
        definir_estruturas, lista_estruturas: filtra estruturas (squad, comunidade, etc)
        definir_variaveis, lista_variaveis: filtra variáveis no JSON
        gerar_relatorio: retorna dict com estatísticas básicas
    """
    blocos = lista_blocos if definir_blocos else ["indicadores_sociais"]
    resultados = []
    relatorios = {}

    for bloco in blocos:
        estruturas = lista_estruturas if definir_estruturas else [estrutura]

        for estr in estruturas:
            gerador = BlocoGerador(bloco, estr, periodo)

            # Filtro período
            df_filtrado = df_entrada[
                (df_entrada["anomes"] >= mes_inicio) &
                (df_entrada["anomes"] <= mes_fim)
            ].copy()

            # Filtro estrutura
            if estr == "squad":
                df_filtrado = df_filtrado[df_filtrado["id_squad"].notnull()]
            elif estr == "release_train":
                df_filtrado = df_filtrado[df_filtrado["id_release_train"].notnull()]
            elif estr == "comunidade":
                df_filtrado = df_filtrado[df_filtrado["id_comunidade"].notnull()]

            df_result = gerador.gerar(df_filtrado,
                                     variaveis_filtradas=(lista_variaveis if definir_variaveis else None))
            resultados.append(df_result)

            if gerar_relatorio:
                rel = {}
                for col in df_result.columns:
                    if col in gerador.config["chaves"]:
                        continue
                    rel[col] = {
                        "linhas_nao_nulas": df_result[col].count(),
                        "ids_distintos": df_result[gerador.config["chaves"][0]].nunique() if len(gerador.config["chaves"]) > 0 else None,
                        "anos_meses_distintos": df_result[gerador.config["chaves"][-1]].nunique() if len(gerador.config["chaves"]) > 1 else None,
                    }
                relatorios[f"{bloco}__{estr}__{periodo}"] = rel

    if len(resultados) == 1:
        return (resultados[0], relatorios) if gerar_relatorio else resultados[0]
    else:
        df_final = pd.concat(resultados, ignore_index=True)
        return (df_final, relatorios) if gerar_relatorio else df_final
