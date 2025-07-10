import os
import json
import importlib.util

class BlocoGerador:
    def __init__(self, bloco: str, estrutura: str, periodo: str):
        # Carrega o JSON de configuração baseado no caminho do bloco/estrutura/período
        path = os.path.join("gerador_bases", "config", bloco, estrutura, f"{periodo}.json")
        with open(path) as f:
            self.config = json.load(f)
        self.bloco = bloco
        self.estrutura = estrutura
        self.periodo = periodo

    def gerar(self, df_entrada, variaveis_filtradas=None):
        # Cria um dataframe base apenas com as chaves únicas
        df_base = df_entrada[self.config["chaves"]].drop_duplicates().copy()

        # Aplica os tratamentos das variáveis configuradas
        for var in self.config.get("variaveis", []):
            # Se um filtro de variáveis foi passado, ignora variáveis fora da lista
            if variaveis_filtradas and var["nome"] not in variaveis_filtradas:
                continue
            df_base = self._aplicar_tratamento(df_base, df_entrada, var)

        # Aplica um código customizado (caso tenha sido definido no JSON)
        df_base = self._aplicar_custom(df_base, df_entrada)
        return df_base

    def _aplicar_tratamento(self, df_base, df_entrada, var):
        # Seleciona e chama dinamicamente a função de tratamento correta (ex: soma_campo)
        func = getattr(self, f"tratamento_{var['tratamento']}")
        return func(df_base, df_entrada, var["nome"], **var.get("params", {}))

    def _aplicar_custom(self, df_base, df_raw):
        # Executa um script externo (caso `custom_code` esteja definido no JSON)
        if "custom_code" in self.config:
            script_name = f"{self.bloco}__{self.estrutura}__{self.periodo}.py"
            script_path = os.path.join("gerador_bases", "custom_steps", script_name)
            if os.path.exists(script_path):
                spec = importlib.util.spec_from_file_location("custom_mod", script_path)
                custom_mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(custom_mod)
                df_base = custom_mod.aplicar(df_base, df_raw)
            else:
                raise FileNotFoundError(f"Script customizado não encontrado: {script_path}")
        return df_base

    # Tratamento 1: soma simples de campo
    def tratamento_soma_campo(self, df_base, df_raw, nome_var, campo):
        soma = df_raw.groupby(self.config["chaves"])[campo].sum().reset_index()
        return df_base.merge(soma.rename(columns={campo: nome_var}), on=self.config["chaves"], how="left")

    # Tratamento 2: divisão de somatórios de dois campos
    def tratamento_soma_divisao(self, df_base, df_raw, nome_var, nominador, denominador):
        agreg = df_raw.groupby(self.config["chaves"])[[nominador, denominador]].sum().reset_index()
        agreg[nome_var] = agreg[nominador] / agreg[denominador]
        return df_base.merge(agreg[[*self.config["chaves"], nome_var]], on=self.config["chaves"], how="left")

    # Tratamento 3: média simples de campo
    def tratamento_media_campo(self, df_base, df_raw, nome_var, campo):
        media = df_raw.groupby(self.config["chaves"])[campo].mean().reset_index()
        return df_base.merge(media.rename(columns={campo: nome_var}), on=self.config["chaves"], how="left")