# core/sap_reader.py
import pandas as pd
import os
import unicodedata

class SAPReader:
    def __init__(self, config):
        self.config = config

    @staticmethod
    def normalize_str(s):
        if pd.isna(s): return ""
        s = unicodedata.normalize('NFKD', str(s))
        texto = "".join(c for c in s if not unicodedata.combining(c)).upper().strip()
        return texto[:-2] if texto.endswith('.0') else texto

    @staticmethod
    def converter_sap_br(valor):
        if pd.isna(valor) or str(valor).strip() == "": return 0.0
        if isinstance(valor, (int, float)): return float(valor)
        texto = str(valor).strip().upper().replace('R$', '').replace(' ', '')
        multi = -1 if texto.endswith('-') else 1
        if multi == -1: texto = texto.replace('-', '')
        try:
            if ',' in texto: texto = texto.replace('.', '').replace(',', '.')
            return float(texto) * multi
        except: return 0.0

    def carregar_mapa_mb52(self):
        caminho = self.config['arquivos']['mb52']
        if not os.path.exists(caminho): raise FileNotFoundError(f"MB52 não: {caminho}")
        
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        start = 0
        for i, row in df_raw.head(20).iterrows():
            if 'CENTRO' in [str(c).strip().upper() for c in row.values]:
                start = i; break
        
        df = df_raw.iloc[start+1:].copy()
        mapa = {}
        for _, r in df.iterrows():
            try:
                c, s, d = self.normalize_str(r.iloc[0]), self.normalize_str(r.iloc[1]), self.normalize_str(r.iloc[4])
                q, v = self.converter_sap_br(r.iloc[5]), self.converter_sap_br(r.iloc[6])
                chave = (s, c, d)
                if chave not in mapa: mapa[chave] = {'qtd': 0.0, 'valor': 0.0}
                mapa[chave]['qtd'] += q; mapa[chave]['valor'] += v
            except: continue
        return mapa

    def carregar_historico_movimentos(self):
        """
        Lê a MB51 e filtra apenas movimentos de baixa (Aplicação).
        Retorna um dicionário: (SKU, CENTRO) -> Soma das Baixas
        """
        caminho = self.config['arquivos']['mb51']
        if not os.path.exists(caminho): 
            print(f"⚠️ AVISO: MB51 não encontrada em {caminho}. Validação contábil será ignorada.")
            return {}

        print("   ⏳ Lendo histórico MB51 (pode demorar um pouco)...")
        # Lê apenas colunas essenciais para economizar memória (Ajuste índices se necessário)
        # Assumindo: Col 0=Centro, Col 1=Material, Col 2=Movimento, Col 5=Qtd
        df = pd.read_excel(caminho, engine='calamine') 
        
        # Encontrar nomes das colunas dinamicamente
        col_cen = next(c for c in df.columns if 'CENTRO' in str(c).upper())
        col_mat = next(c for c in df.columns if 'MATERIAL' in str(c).upper())
        col_mov = next(c for c in df.columns if 'MOVIMENTO' in str(c).upper() or 'TP.MOV' in str(c).upper())
        col_qtd = next(c for c in df.columns if 'QUANTIDADE' in str(c).upper() or 'QTD' in str(c).upper())

        movimentos_validos = [str(m) for m in self.config['movimentos_baixa']]
        
        # Filtrar apenas baixas
        df_baixas = df[df[col_mov].astype(str).isin(movimentos_validos)].copy()
        
        mapa_baixas = {}
        for _, row in df_baixas.iterrows():
            try:
                s = self.normalize_str(row[col_mat])
                c = self.normalize_str(row[col_cen])
                q = abs(self.converter_sap_br(row[col_qtd])) # Baixa é negativo, convertemos para absoluto
                
                chave = (s, c)
                mapa_baixas[chave] = mapa_baixas.get(chave, 0.0) + q
            except: continue
            
        return mapa_baixas

    def carregar_mapa_centros(self):
        caminho = self.config['arquivos']['centros']
        # Tratativa Singular/Plural
        if not os.path.exists(caminho):
            alt = caminho.replace('Centro.xlsx', 'Centros.xlsx')
            if os.path.exists(alt): caminho = alt
            else: raise FileNotFoundError(f"Centros não: {caminho}")
            
        df = pd.read_excel(caminho, engine='calamine', header=None)
        mapa = {}
        idx_id = self.config['indices_fixos']['centro_col_id']
        idx_cen = self.config['indices_fixos']['centro_col_nome']
        
        # Tenta achar onde começam os dados
        start = 1 if isinstance(df.iloc[0, idx_cen], str) and 'CEN' in str(df.iloc[0, idx_cen]).upper() else 0

        for i, row in df.iloc[start:].iterrows():
            try:
                k, v = self.normalize_str(row.iloc[idx_id]), self.normalize_str(row.iloc[idx_cen])
                if k and k != 'NAN': mapa[k] = v
            except: continue
        return mapa

    def carregar_aldrei(self):
        caminho = self.config['arquivos']['aldrei']
        if not os.path.exists(caminho): raise FileNotFoundError(f"Aldrei não: {caminho}")
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        head = next(i for i, row in df_raw.head(20).iterrows() if 'SKU' in [str(c).upper() for c in row.values])
        df = df_raw.iloc[head+1:].reset_index(drop=True)
        df.columns = [str(c).strip() for c in df_raw.iloc[head]]
        return df.loc[:, ~df.columns.str.contains('^nan$|^Unnamed', case=False, na=True)]