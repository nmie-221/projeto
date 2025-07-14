def mostrar_ajuda():
    print(\"\"\"
==================== AJUDA: Gerador de Bases =====================

Função principal:
    gerar_dados(
        estrutura: str,
        periodo: str,
        mes_inicio: str,
        mes_fim: str,
        df_entrada: DataFrame,
        definir_blocos: bool = False,
        lista_blocos: list = None,
        definir_estruturas: bool = False,
        lista_estruturas: list = None,
        definir_variaveis: bool = False,
        lista_variaveis: list = None,
        gerar_relatorio: bool = False
    )

Configuração:
    JSON em: gerador_bases/config/[bloco]/[estrutura]/[periodo].json

Tratamentos suportados:
    - soma
    - media
    - media_ponderada
    - percentual

Customização:
    Adicione scripts em gerador_bases/custom_steps/[bloco]__[estrutura]__[periodo].py

===============================================================
\"\"\")
