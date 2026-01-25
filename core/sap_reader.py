# core/sap_reader.py
import pandas as pd
import os
import unicodedata

class SAPReader:
    @staticmethod
    def normalize_str(s):
        """Remove acentos, espaços e o sufixo .0 de IDs."""
        if pd.isna(s): return ""
        s = unicodedata.normalize('NFKD', str(s))
        texto = "".join(c for c in s if not unicodedata.combining(c)).upper().strip()
        return texto[:-2] if texto.endswith('.0') else texto

    @staticmethod
    def converter_sap_br(valor):
        """
        Converte valores numéricos mantendo a precisão float.
        Trata R$, espaços, pontos de milhar e vírgula decimal.
        """
        if pd.isna(valor) or str(valor).strip() == "": return 0.0
        if isinstance(valor, (int, float)): return float(valor)
        
        texto = str(valor).strip().upper().replace('R$', '').replace(' ', '')
        
        # Tratamento de sinal negativo no final (Ex: '100-')
        multiplicador = 1
        if texto.endswith('-'):
            multiplicador = -1
            texto = texto.replace('-', '')

        try:
            if ',' in texto:
                texto = texto.replace('.', '').replace(',', '.')
            return float(texto) * multiplicador
        except:
            return 0.0

    def carregar_mapa_mb52(self, caminho):
        if not os.path.exists(caminho): raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
        
        # Leitura rápida sem cabeçalho para achar a linha correta
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        
        start = 0
        for i, row in df_raw.head(20).iterrows():
            linha = [str(c).strip().upper() for c in row.values]
            if 'CENTRO' in linha: start = i; break
        
        df = df_raw.iloc[start+1:].copy()
        mapa = {}

        for _, r in df.iterrows():
            try:
                c = self.normalize_str(r.iloc[0])
                s = self.normalize_str(r.iloc[1])
                d = self.normalize_str(r.iloc[4])
                
                # Conversão segura usando sua lógica
                q = self.converter_sap_br(r.iloc[5]) 
                v = self.converter_sap_br(r.iloc[6]) 
                
                chave = (s, c, d)
                if chave not in mapa: mapa[chave] = {'qtd': 0.0, 'valor': 0.0}
                
                mapa[chave]['qtd'] += q
                mapa[chave]['valor'] += v
            except: continue
        return mapa

    def carregar_mapa_centros(self, caminho):
        """Usa lógica de índices fixos (K=10, D=3) que funciona na base do usuário."""
        # Tratativa para nome do arquivo (Singular/Plural)
        if not os.path.exists(caminho):
            if os.path.exists(caminho.replace('Centro.xlsx', 'Centros.xlsx')):
                caminho = caminho.replace('Centro.xlsx', 'Centros.xlsx')
            else:
                raise FileNotFoundError(f"Arquivo de Centros não encontrado: {caminho}")
        
        df = pd.read_excel(caminho, engine='calamine', header=None)
        mapa = {}
        
        # Pula cabeçalho se existir 'Cen.' na coluna D
        start_row = 1 if isinstance(df.iloc[0, 3], str) and 'CEN' in str(df.iloc[0, 3]).upper() else 0

        for i, row in df.iloc[start_row:].iterrows():
            try:
                # Índices FIXOS conforme seu código original
                id_obra = self.normalize_str(row.iloc[10]) # Coluna K
                centro = self.normalize_str(row.iloc[3])   # Coluna D
                
                if id_obra and id_obra != 'NAN':
                    mapa[id_obra] = centro
            except: continue
        return mapa

    def carregar_aldrei(self, caminho):
        if not os.path.exists(caminho): raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")
        
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        head = None
        for i, row in df_raw.head(20).iterrows():
            linha = [str(c).strip().upper() for c in row.values]
            if 'SKU' in linha and any('APL' in x for x in linha):
                head = i; break
        
        if head is None: raise ValueError("Cabeçalho não encontrado no Aldrei.")

        df = df_raw.iloc[head+1:].reset_index(drop=True)
        df.columns = [str(c).strip() for c in df_raw.iloc[head]]
        return df.loc[:, ~df.columns.str.contains('^nan$|^Unnamed', case=False, na=True)]