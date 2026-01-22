import pandas as pd  # <-- Faltava esta importação
import unicodedata
import re

class SAPReader:
    @staticmethod
    def normalize_str(s):
        """Remove acentos, espaços extras e padroniza para maiúsculas."""
        # Agora o pd.isna funcionará corretamente
        if pd.isna(s): 
            return ""
        s = unicodedata.normalize('NFKD', str(s))
        return "".join(c for c in s if not unicodedata.combining(c)).upper().strip()

    @staticmethod
    def limpar_sap_qtd(valor):
        """Trata o padrão SAP de vírgula decimal."""
        if pd.isna(valor) or str(valor).strip() == "": 
            return 0.0
        try: 
            return float(str(valor).replace(',', '.'))
        except: 
            return 0.0

    def carregar_aldrei(self, caminho):
        """Busca dinâmica de cabeçalho com normalização de strings."""
        # Lê o arquivo sem cabeçalho para análise bruta
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        linha_cabecalho = None
        
        # Busca robusta nas primeiras 15 linhas
        for i, row in df_raw.head(15).iterrows():
            # Normalizamos cada célula da linha para comparar sem erro de acento ou espaço
            row_clean = [self.normalize_str(c) for c in row.values]
            if 'SKU' in row_clean and 'APL X DRAFT' in row_clean:
                linha_cabecalho = i
                break
        
        if linha_cabecalho is None:
            raise ValueError("❌ Cabeçalho Aldrei (SKU / APL x DRAFT) não encontrado nas primeiras 15 linhas.")

        # Reconstrói o DataFrame a partir da linha identificada
        df = df_raw.iloc[linha_cabecalho + 1:].reset_index(drop=True)
        df.columns = [str(c).strip() for c in df_raw.iloc[linha_cabecalho]]
        
        # Limpeza de colunas vazias ou fantasmas
        return df.loc[:, ~df.columns.str.contains('^nan$|^Unnamed', case=False, na=True)]

    def carregar_mb51(self, caminho):
        return pd.read_excel(caminho, engine='calamine', dtype={'Material': str, 'Centro': str, 'Tipo de movimento': str})

    def carregar_mb52(self, caminho):
        return pd.read_excel(caminho, engine='calamine', dtype={'Material': str, 'Centro': str})