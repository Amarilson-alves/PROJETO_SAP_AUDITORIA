# core/sap_reader.py
import pandas as pd
import os
import unicodedata

class SAPReader:
    @staticmethod
    def normalize_str(s):
        """Padroniza strings: Remove acentos, espaços e o sufixo .0 de números."""
        if pd.isna(s): return ""
        # Converte para string, normaliza unicode e joga pra maiúsculo
        s = unicodedata.normalize('NFKD', str(s))
        texto = "".join(c for c in s if not unicodedata.combining(c)).upper().strip()
        # Remove '.0' se existir no final (ex: '1234.0' vira '1234')
        return texto[:-2] if texto.endswith('.0') else texto

    @staticmethod
    def limpar_valor(valor):
        """
        Converte valores SAP para float corretamente.
        Trata: '1.000,00', '1.000,00-', '1000.00'
        """
        if isinstance(valor, (int, float)): return float(valor)
        if pd.isna(valor) or str(valor).strip() == "": return 0.0
        
        # Remove R$ e espaços
        texto = str(valor).strip().upper().replace('R$', '').replace(' ', '')
        
        # Verifica sinal negativo no final (Padrão SAP: '500,00-')
        multiplicador = 1
        if texto.endswith('-'):
            multiplicador = -1
            texto = texto.replace('-', '')

        try:
            # Lógica: Se tem vírgula, ela é o decimal. Removemos os pontos de milhar.
            if ',' in texto:
                texto = texto.replace('.', '').replace(',', '.')
            return float(texto) * multiplicador
        except:
            return 0.0

    def carregar_mapa_mb52(self, caminho):
        if not os.path.exists(caminho): raise FileNotFoundError(f"MB52 não encontrada: {caminho}")
        
        # Leitura rápida sem cabeçalho para encontrar a linha certa
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        
        start = 0
        # Procura onde começa o cabeçalho 'CENTRO'
        for i, row in df_raw.head(20).iterrows():
            linha = [str(c).strip().upper() for c in row.values]
            if 'CENTRO' in linha:
                start = i; break
        
        df = df_raw.iloc[start+1:].copy()
        mapa = {}

        # Itera acumulando valores
        for _, r in df.iterrows():
            try:
                # Índices padrão MB52: 0=Centro, 1=Material, 4=Depósito
                # 5=Utilização Livre (Qtd), 6=Valor (Monetário)
                c = self.normalize_str(r.iloc[0])
                s = self.normalize_str(r.iloc[1])
                d = self.normalize_str(r.iloc[4])
                
                q = self.limpar_valor(r.iloc[5])
                v = self.limpar_valor(r.iloc[6])
                
                chave = (s, c, d)
                if chave not in mapa: 
                    mapa[chave] = {'qtd': 0.0, 'valor': 0.0}
                
                mapa[chave]['qtd'] += q
                mapa[chave]['valor'] += v
            except: continue
            
        return mapa

    def carregar_mapa_centros(self, caminho):
        """
        Lógica 'Linguiça' Refinada: Usa índices fixos (10 e 3) que funcionavam.
        """
        if not os.path.exists(caminho): 
            # Tenta plural ou singular
            if os.path.exists(caminho.replace('Centro.xlsx', 'Centros.xlsx')):
                caminho = caminho.replace('Centro.xlsx', 'Centros.xlsx')
            else:
                raise FileNotFoundError(f"Arquivo de Centros não encontrado: {caminho}")
        
        df = pd.read_excel(caminho, engine='calamine', header=None)
        mapa = {}
        
        # Pula cabeçalho se houver
        start_row = 1 if isinstance(df.iloc[0, 3], str) and 'CEN' in str(df.iloc[0, 3]).upper() else 0

        for i, row in df.iloc[start_row:].iterrows():
            try:
                # REVERTER PARA ÍNDICES FIXOS (O que funcionava no seu código original)
                # Coluna K (índice 10) = ID
                # Coluna D (índice 3) = Centro
                id_obra = self.normalize_str(row.iloc[10])
                centro = self.normalize_str(row.iloc[3])
                
                if id_obra and id_obra != 'NAN':
                    mapa[id_obra] = centro
            except: continue
            
        return mapa

    def carregar_aldrei(self, caminho):
        if not os.path.exists(caminho): raise FileNotFoundError(f"Aldrei não encontrado: {caminho}")
        
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        head = None
        for i, row in df_raw.head(20).iterrows():
            linha = [str(c).strip().upper() for c in row.values]
            if 'SKU' in linha: # Critério simplificado para achar cabeçalho
                head = i; break
        
        if head is None: raise ValueError("Cabeçalho não encontrado no arquivo Aldrei.")

        df = df_raw.iloc[head+1:].reset_index(drop=True)
        df.columns = [str(c).strip() for c in df_raw.iloc[head]]
        # Remove colunas sem nome
        df = df.loc[:, ~df.columns.str.contains('^nan$|^Unnamed', case=False, na=True)]
        # Remove linhas totalmente vazias
        return df.dropna(how='all')