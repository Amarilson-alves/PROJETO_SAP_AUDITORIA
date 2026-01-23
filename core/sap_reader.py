# core/sap_reader.py
import pandas as pd
import os
import unicodedata

class SAPReader:
    @staticmethod
    def normalize_str(s):
        if pd.isna(s): return ""
        s = unicodedata.normalize('NFKD', str(s))
        texto = "".join(c for c in s if not unicodedata.combining(c)).upper().strip()
        return texto[:-2] if texto.endswith('.0') else texto

    @staticmethod
    def limpar_valor(valor):
        """Trata a conversão de valores monetários e quantidades do SAP."""
        if isinstance(valor, (int, float)): return float(valor)
        if pd.isna(valor) or str(valor).strip() == "": return 0.0
        try:
            texto = str(valor).strip().replace('R$', '').replace(' ', '')
            # Se houver ponto e vírgula (ex: 1.234,56), remove o ponto e troca a vírgula por ponto
            if '.' in texto and ',' in texto:
                texto = texto.replace('.', '').replace(',', '.')
            # Se houver apenas vírgula (ex: 1234,56), troca por ponto
            elif ',' in texto:
                texto = texto.replace(',', '.')
            return float(texto)
        except:
            return 0.0

    def carregar_mapa_mb52(self, caminho):
        if not os.path.exists(caminho): raise FileNotFoundError(f"MB52 ausente: {caminho}")
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        
        start = 0
        for i, row in df_raw.head(15).iterrows():
            if 'CENTRO' in [str(c).upper() for c in row.values]:
                start = i; break
        
        df = df_raw.iloc[start+1:].copy()
        mapa = {}
        for _, r in df.iterrows():
            try:
                c, s, d = self.normalize_str(r.iloc[0]), self.normalize_str(r.iloc[1]), self.normalize_str(r.iloc[4])
                # Usando a nova função de limpeza corrigida
                q = self.limpar_valor(r.iloc[5])
                v = self.limpar_valor(r.iloc[6])
                chave = (s, c, d)
                if chave not in mapa: mapa[chave] = {'qtd': 0.0, 'valor': 0.0}
                mapa[chave]['qtd'] += q
                mapa[chave]['valor'] += v
            except: continue
        return mapa

    def carregar_mapa_centros(self, caminho):
        df = pd.read_excel(caminho, engine='calamine')
        return {self.normalize_str(r.iloc[10]): self.normalize_str(r.iloc[3]) for _, r in df.iterrows() if not pd.isna(r.iloc[10])}

    def carregar_aldrei(self, caminho):
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        head = next(i for i, row in df_raw.head(15).iterrows() if 'SKU' in [str(c).upper() for c in row.values])
        df = df_raw.iloc[head+1:].reset_index(drop=True)
        df.columns = [str(c).strip() for c in df_raw.iloc[head]]
        return df.loc[:, ~df.columns.str.contains('^nan$|^Unnamed', case=False, na=True)]