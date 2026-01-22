import pandas as pd

class SAPReader: # <-- Verifique se está exatamente assim
    @staticmethod
    def limpar_sap_qtd(valor):
        if pd.isna(valor) or str(valor).strip() == "": 
            return 0.0
        try: 
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
        # Implementação da busca dinâmica de cabeçalho que você validou
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        linha_cabecalho = next(i for i, row in df_raw.head(15).iterrows() 
                               if 'SKU' in [str(c).upper() for c in row.values] 
                               and 'APL X DRAFT' in [str(c).upper() for c in row.values])
        
        df = df_raw.iloc[linha_cabecalho + 1:].reset_index(drop=True)
        df.columns = [str(c).strip() for c in df_raw.iloc[linha_cabecalho]]
        return df.loc[:, ~df.columns.str.contains('^nan$|^Unnamed', case=False, na=True)]