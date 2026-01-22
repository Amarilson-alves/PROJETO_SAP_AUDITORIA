# core/validator.py
import pandas as pd

class DataValidator:
    def __init__(self, logger):
        self.log = logger

    def validar_colunas(self, df, nome_arquivo, colunas_esperadas):
        colunas_atuais = set(df.columns)
        faltantes = [c for c in colunas_esperadas if c not in colunas_atuais]
        
        if faltantes:
            self.log.error(f"❌ Colunas faltantes em {nome_arquivo}: {faltantes}")
            return False
        
        self.log.info(f"✅ Colunas validadas para {nome_arquivo}.")
        return True

    def validar_valores_nulos(self, df, nome_arquivo, colunas_criticas):
        nulos = df[colunas_criticas].isnull().sum()
        if nulos.any():
            self.log.warning(f"⚠️ Valores nulos detectados em {nome_arquivo}: {nulos[nulos > 0].to_dict()}")
            return False
        return True