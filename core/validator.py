import pandas as pd
import logging

class DataValidator:
    def __init__(self, logger):
        self.log = logger

    def validar_colunas(self, df, nome_arquivo, colunas_esperadas):
        """Verifica se todas as colunas necessárias existem no arquivo."""
        colunas_atuais = set(df.columns)
        faltantes = set(colunas_esperadas) - colunas_atuais
        
        if faltantes:
            self.log.error(f"❌ Erro no arquivo {nome_arquivo}: Colunas faltando: {faltantes}")
            return False
        
        self.log.info(f"✅ Validação de colunas concluída para {nome_arquivo}.")
        return True

    def validar_integridade(self, df51):
        """Aplica o checklist de integridade MM."""
        if df51['Material'].isna().any():
            vazios = df51['Material'].isna().sum()
            self.log.warning(f"⚠️ MB51 contém {vazios} linhas com Material vazio.")
        
        # Verifica se há quantidades zeradas que deveriam ser ignoradas
        qtd_zero = (df51['Quantidade'] == 0).sum()
        if qtd_zero > 0:
            self.log.info(f"ℹ️ Ignorando {qtd_zero} registros com quantidade zero na MB51.")
            
        return True