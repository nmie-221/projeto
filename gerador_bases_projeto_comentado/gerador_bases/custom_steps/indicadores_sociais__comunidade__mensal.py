def aplicar(df_base, df_raw):
    df = df_base.copy()
    df["flag_critico"] = df["qtd_eventos"].apply(lambda x: 1 if x > 50 else 0)
    return df