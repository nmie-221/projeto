from gerador_bases import BlocoGerador
import pandas as pd
import os

def gerar_dados(estrutura, periodo, mes_inicio, mes_fim,
                df_entrada,
                definir_blocos=False, lista_blocos=None,
                definir_variaveis=False, lista_variaveis=None):
    '''
    Função que itera pelos blocos e aplica geração de variáveis com filtros opcionais.
    '''
    path_base = "gerador_bases/config"
    resultados = []

    for bloco in os.listdir(path_base):
        # Se definir_blocos estiver ativado, filtra pelos blocos desejados
        if definir_blocos and bloco not in lista_blocos:
            continue

        # Monta caminho do JSON de config
        config_path = os.path.join(path_base, bloco, estrutura, f"{periodo}.json")
        if not os.path.exists(config_path):
            continue

        print(f"🔍 Gerando bloco: {bloco} | estrutura: {estrutura} | período: {periodo}")
        gerador = BlocoGerador(bloco, estrutura, periodo)

        # Se ativado, filtra as variáveis desejadas
        variaveis_filtradas = lista_variaveis if definir_variaveis else None

        # Executa a geração com os filtros aplicados
        df_saida = gerador.gerar(df_entrada, variaveis_filtradas=variaveis_filtradas)
        df_saida["bloco"] = bloco  # Adiciona coluna de controle do bloco
        resultados.append(df_saida)

    # Retorna todos os resultados concatenados
    if resultados:
        return pd.concat(resultados, ignore_index=True)
    else:
        return pd.DataFrame()

# Exemplo de execução da função
if __name__ == "__main__":
    df_entrada = pd.DataFrame({
        "id_comunidade": [101, 101, 102, 102],
        "anomes": ["202501", "202501", "202501", "202501"],
        "eventos": [20, 40, 30, 90],
        "concluidas": [10, 10, 20, 10],
        "total": [20, 20, 30, 30],
        "nota": [4.5, 4.7, 4.2, 4.9]
    })

    df_resultado = gerar_dados(
        estrutura="comunidade",
        periodo="mensal",
        mes_inicio="202501",
        mes_fim="202505",
        df_entrada=df_entrada,
        definir_blocos=True,
        lista_blocos=["indicadores_sociais"],
        definir_variaveis=True,
        lista_variaveis=["qtd_eventos", "nota_media"]
    )

    print(df_resultado)