import os
import json
import importlib.util

class BlocoGerador:
    def __init__(self, bloco: str, estrutura: str, periodo: str):
        path = os.path.join("gerador_bases", "config", bloco, estrutura, f"{periodo}.json")
        with open(path) as f:
            self.config = json.load(f)
        self.bloco = bloco
        self.estrutura = estrutura
        self.periodo = periodo

    def gerar(self, df_entrada, variaveis_filtradas=None):
        df_base = df_entrada[self.config["chaves"]].drop_duplicates().copy()
        for var in self.config.get("variaveis", []):
            if variaveis_filtradas and var["nome"] not in variaveis_filtradas:
                continue
            df_base = self._aplicar_tratamento(df_base, df_entrada, var)
        df_base = self._aplicar_custom(df_base, df_entrada)
        return df_base

    def _aplicar_tratamento(self, df_base, df_entrada, var):
        func = getattr(self, f"tratamento_{var['tratamento']}")
        return func(df_base, df_entrada, var["nome"], **var.get("params", {}))

    def _aplicar_custom(self, df_base, df_raw):
        if "custom_code" in self.config:
            script_name = f"{self.bloco}__{self.estrutura}__{self.periodo}.py"
            script_path = os.path.join("gerador_bases", "custom_steps", script_name)
            if os.path.exists(script_path):
                spec = importlib.util.spec_from_file_location("custom_mod", script_path)
                custom_mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(custom_mod)
                df_base = custom_mod.aplicar(df_base, df_raw)
        return df_base

    def tratamento_soma(self, df_base, df_raw, nome_var, campo):
        soma = df_raw.groupby(self.config["chaves"])[campo].sum().reset_index()
        return df_base.merge(soma.rename(columns={campo: nome_var}), on=self.config["chaves"], how="left")
