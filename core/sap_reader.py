# core/sap_reader.py
import pandas as pd
import os
import unicodedata

class SAPReader:
    @staticmethod
    def normalize_str(s):
        """Remove acentos, espaços extras e padroniza para maiúsculas."""
        if pd.isna(s): return ""
        s = unicodedata.normalize('NFKD', str(s))
        return "".join(c for c in s if not unicodedata.combining(c)).upper().strip()

    @staticmethod
    def limpar_sap_qtd(valor):
        """
        Converte quantidades do padrão SAP (string com vírgula) para float.
        Ex: '10,50' -> 10.5
        """
        if pd.isna(valor) or str(valor).strip() == "": 
            return 0.0
        try: 
            # Remove pontos de milhar e troca vírgula por ponto decimal
            val_clean = str(valor).replace('.', '').replace(',', '.')
            return float(val_clean)
        except: 
            return 0.0

    def _validar_e_carregar(self, caminho, colunas_dtype, nome_rotulo):
        """Método interno para validação robusta de arquivos."""
        if not os.path.exists(caminho):
            raise FileNotFoundError(f"Arquivo {nome_rotulo} não encontrado em: {caminho}")
        
        df = pd.read_excel(caminho, engine='calamine', dtype=colunas_dtype)
        
        if df.empty:
            raise ValueError(f"O arquivo {nome_rotulo} está vazio.")
        return df

    def carregar_mb51(self, caminho):
        return self._validar_e_carregar(caminho, {'Material': str, 'Centro': str, 'Tipo de movimento': str}, "MB51")

    def carregar_mb52(self, caminho):
        return self._validar_e_carregar(caminho, {'Material': str, 'Centro': str}, "MB52")

    def carregar_aldrei(self, caminho):
        """Busca dinâmica de cabeçalho com detecção de SKU e APL X DRAFT."""
        if not os.path.exists(caminho):
            raise FileNotFoundError(f"Arquivo Aldrei não encontrado em: {caminho}")
            
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        linha_cabecalho = None
        
        for i, row in df_raw.head(15).iterrows():
            row_clean = [self.normalize_str(c) for c in row.values]
            if 'SKU' in row_clean and 'APL X DRAFT' in row_clean:
                linha_cabecalho = i
                break
        
        if linha_cabecalho is None:
            raise ValueError("Cabeçalho 'SKU' / 'APL x DRAFT' não localizado nas primeiras 15 linhas do Aldrei.")

        df = df_raw.iloc[linha_cabecalho + 1:].reset_index(drop=True)
        df.columns = [str(c).strip() for c in df_raw.iloc[linha_cabecalho]]
        return df.loc[:, ~df.columns.str.contains('^nan$|^Unnamed', case=False, na=True)]