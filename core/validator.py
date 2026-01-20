import logging

class DataValidator:
    def __init__(self, logger):
        self.log = logger

    def validar_mb51(self, df):
        colunas_obrigatorias = ['Centro', 'Material', 'Quantidade', 'Tipo de movimento']
        erro = False
        
        # Check 1: Colunas
        for col in colunas_obrigatorias:
            if col not in df.columns:
                self.log.error(f"Coluna obrigatória ausente na MB51: {col}")
                erro = True
        
        # Check 2: Linhas Vazias em campos chave
        vazios = df['Material'].isna().sum()
        if vazios > 0:
            self.log.warning(f"MB51 contém {vazios} linhas com Material vazio. Elas serão ignoradas.")
            
        # Check 3: Tipos de Movimento novos
        movs = df['Tipo de movimento'].unique()
        self.log.info(f"Movimentos detectados na MB51: {list(movs)}")
        
        return not erro

    def validar_mb52(self, df):
        if 'Utilização livre' not in df.columns:
            self.log.error("MB52 não possui a coluna 'Utilização livre'. Verifique o layout do SAP.")
            return False
        return True