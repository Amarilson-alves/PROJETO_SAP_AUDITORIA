import pandas as pd

class SAPReader:
    @staticmethod
    def limpar_sap_qtd(valor):
        if pd.isna(valor) or str(valor).strip() == "": 
            return 0.0
        try: 
            # Trata o padrão SAP de vírgula decimal
            return float(str(valor).replace(',', '.'))
        except: 
            return 0.0

    def carregar_mb51(self, caminho):
        return pd.read_excel(caminho, engine='calamine', 
                             dtype={'Material': str, 'Centro': str, 'Tipo de movimento': str})

    def carregar_mb52(self, caminho):
        return pd.read_excel(caminho, engine='calamine', 
                             dtype={'Material': str, 'Centro': str})

    def carregar_aldrei(self, caminho):
        return pd.read_excel(caminho, engine='calamine')