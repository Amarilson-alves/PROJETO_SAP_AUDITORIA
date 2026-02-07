# core/sap_reader.py
import pandas as pd
import numpy as np
import os
import re
import unicodedata
from datetime import datetime

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

    # --- LEITURA BÁSICA ---
    def carregar_mapa_mb52(self):
        caminho = self.config['arquivos']['mb52']
        if not os.path.exists(caminho): raise FileNotFoundError(f"MB52 não encontrada: {caminho}")
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        start = 0
        for i, row in df_raw.head(20).iterrows():
            if 'CENTRO' in [str(c).strip().upper() for c in row.values]:
                start = i; break
        df = df_raw.iloc[start+1:].copy()
        mapa = {}
        evidencias = [] 
        for idx_original, r in df.iterrows():
            try:
                c = self.normalize_str(r.iloc[0])
                s = self.normalize_str(r.iloc[1])
                d = self.normalize_str(r.iloc[4])
                desc = str(r.iloc[2]).strip()
                q = self.converter_sap_br(r.iloc[5])
                v = self.converter_sap_br(r.iloc[6])
                chave = (s, c, d)
                if chave not in mapa: mapa[chave] = {'qtd': 0.0, 'valor': 0.0}
                mapa[chave]['qtd'] += q
                mapa[chave]['valor'] += v
                evidencias.append({'LINHA_EXCEL': idx_original + 1, 'CENTRO': c, 'SKU': s, 'DESCRIÇÃO': desc, 'DEPÓSITO': d, 'QTD_REGISTRO': q, 'VALOR_REGISTRO': v})
            except: continue
        return mapa, pd.DataFrame(evidencias)

    def carregar_historico_movimentos(self): return {} 

    def carregar_mapa_centros(self):
        caminho = self.config['arquivos']['centros']
        if not os.path.exists(caminho): return {}
        df = pd.read_excel(caminho, engine='calamine', header=None)
        mapa = {}
        idx_id = self.config['indices_fixos']['centro_col_id']
        idx_cen = self.config['indices_fixos']['centro_col_nome']
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

    # ---------------------------------------------------------
    # 🕵️‍♂️ AUDITORIA CONTÍNUA (ENTERPRISE GOLD MASTER)
    # ---------------------------------------------------------
    def gerar_raio_x_amed(self, mapa_mb52_referencia):
        caminho_mb51 = self.config['arquivos']['mb51']
        caminho_dim = self.config['arquivos']['dim_movimentos']
        
        if not os.path.exists(caminho_mb51) or not os.path.exists(caminho_dim):
            return pd.DataFrame()

        print("   ☢️ Iniciando Motor de Auditoria Contínua (Multicausa + Responsabilidade)...")
        
        # 1. Carrega CSV de Definições
        df_dim = pd.read_csv(caminho_dim, sep=';', dtype=str)
        df_dim['BWART'] = df_dim['BWART'].str.strip()
        if 'TIPO_ESPECIAL' not in df_dim.columns: df_dim['TIPO_ESPECIAL'] = 'PADRAO'

        # Listas para validação rápida
        lista_saida = df_dim[df_dim['SENTIDO_AMED'] == 'SAIDA']['BWART'].unique()
        
        # 2. Leitura da MB51
        df = pd.read_excel(caminho_mb51, engine='calamine')

        def get_col(patterns):
            for pat in patterns:
                found = next((c for c in df.columns if pat.upper() in str(c).upper()), None)
                if found: return found
            return None

        # Mapeamento
        col_centro = get_col(['CENTRO', 'WERKS'])           
        col_dep = get_col(['DEPÓSITO', 'DEPOSITO', 'LGORT']) 
        col_mat = get_col(['MATERIAL', 'MAT.'])             
        col_desc = get_col(['TEXTO BREVE', 'DESCRIÇÃO'])    
        col_qtd = get_col(['QUANTIDADE', 'QTD'])            
        col_data = get_col(['DATA DE LANÇAMENTO', 'DATA LANC.', 'BUDAT']) 
        col_mov = get_col(['MOVIMENTO', 'BWART'])           
        col_rec = get_col(['RECEBEDOR', 'RECEP.', 'WEMPF']) 
        col_val = get_col(['MONTANTE', 'VALOR', 'DMBTR'])   
        col_doc = get_col(['DOC.MATERIAL', 'DOCUMENTO'])    
        col_item = get_col(['ITEM', 'ITEM DO DOCUMENTO']) 
        col_nome1 = get_col(['NOME 1', 'NOME1'])            
        col_user = get_col(['USUÁRIO', 'USUARIO', 'USNAM']) 
        col_lote = get_col(['LOTE', 'CHARG']) 

        # 3. Normalização
        if col_data: df[col_data] = pd.to_datetime(df[col_data], errors='coerce')
        
        df[col_mov] = df[col_mov].astype(str).str.strip()
        df[col_dep] = df[col_dep].astype(str).str.strip().str.upper()
        df[col_mat] = df[col_mat].apply(lambda x: self.normalize_str(x))
        df[col_centro] = df[col_centro].apply(lambda x: self.normalize_str(x))

        df['ID_LIMPO'] = df[col_rec].fillna('SEM_ID').astype(str).str.strip()
        df.loc[df['ID_LIMPO'].isin(['', 'nan', '0', '0.0']), 'ID_LIMPO'] = 'SEM_ID'
        
        if col_lote:
            df['LOTE_LIMPO'] = df[col_lote].fillna('SEM_LOTE').astype(str).str.strip()
            df.loc[df['LOTE_LIMPO'] == '', 'LOTE_LIMPO'] = 'SEM_LOTE'
        else:
            df['LOTE_LIMPO'] = 'SEM_LOTE'

        # 4. MERGE COM CSV (SINGLE SOURCE OF TRUTH)
        df_merged = df.merge(df_dim[['BWART', 'SENTIDO_AMED', 'TIPO_ESPECIAL']], 
                             left_on=col_mov, right_on='BWART', how='left')

        df_merged['SENTIDO_REAL'] = np.where(
            df_merged[col_dep].str.contains('AMED', na=False),
            df_merged['SENTIDO_AMED'],
            'NEUTRO'
        )

        # 5. RECONSTRUÇÃO LIFO (STACK)
        df_proc = df_merged[df_merged['SENTIDO_REAL'] != 'NEUTRO'].copy()
        grupos = df_proc.groupby(['ID_LIMPO', col_mat, col_centro, col_dep, 'LOTE_LIMPO'])
        analise = []
        hoje = datetime.now()

        print(f"   📊 Processando {len(grupos)} pilhas de estoque...")

        for (id_dono, material, centro, deposito, lote), grupo in grupos:
            
            # Ordenação Atômica
            cols_sort = [col_data, col_doc]
            if col_item: cols_sort.append(col_item)
            grupo = grupo.sort_values(by=cols_sort)

            pilha_estoque = []
            historico_consumo = [] 
            doc_furo = None

            for idx, row in grupo.iterrows():
                qtd = abs(self.converter_sap_br(row[col_qtd]))
                val_total = abs(self.converter_sap_br(row[col_val])) if col_val else 0
                sentido = row['SENTIDO_REAL']
                tipo_especial = str(row['TIPO_ESPECIAL']) 
                data_mov = row[col_data]
                mov = row[col_mov]

                if sentido == 'ENTRADA':
                    valor_unit = (val_total / qtd) if qtd > 0 else 0
                    
                    status_entrada = "OK"
                    if tipo_especial == 'ESTORNO':
                        tem_consumo_previo = any(
                            (h['mov'] in lista_saida) and (h['data'] <= data_mov)
                            for h in historico_consumo
                        )
                        if not tem_consumo_previo:
                            status_entrada = "IRREGULAR_SEM_HISTORICO"

                    pilha_estoque.append({
                        'qtd': qtd,
                        'valor_unit': valor_unit,
                        'data': data_mov,
                        'mov': mov,
                        'tipo_especial': tipo_especial,
                        'doc': row[col_doc],
                        'user': row[col_user],
                        'status_audit': status_entrada,
                    })

                elif sentido == 'SAIDA':
                    historico_consumo.append({'mov': mov, 'data': data_mov})
                    qtd_a_baixar = qtd
                    
                    # Furo imediato
                    if not pilha_estoque and doc_furo is None:
                        doc_furo = f"{mov}-{row[col_doc]}"
                    
                    while qtd_a_baixar > 0.001 and pilha_estoque:
                        lote_atual = pilha_estoque[-1]
                        if lote_atual['qtd'] <= qtd_a_baixar:
                            qtd_a_baixar -= lote_atual['qtd']
                            pilha_estoque.pop()
                        else:
                            lote_atual['qtd'] -= qtd_a_baixar
                            qtd_a_baixar = 0
                    
                    # Furo residual
                    if qtd_a_baixar > 0.001 and doc_furo is None:
                        doc_furo = f"{mov}-{row[col_doc]}"

            # === RESULTADOS ===
            saldo_reconstruido = sum(item['qtd'] for item in pilha_estoque)
            
            # Check MB52
            chave_mb52 = (material, centro, deposito)
            saldo_mb52 = mapa_mb52_referencia.get(chave_mb52, {}).get('qtd', 0)
            tem_divergencia = (saldo_reconstruido > 0.1 and saldo_mb52 < 0.01)

            if saldo_reconstruido < 0.001 and id_dono != 'SEM_ID' and not doc_furo and not tem_divergencia:
                continue

            if pilha_estoque:
                sobra_ref = pilha_estoque[0] 
                dt_entrada = sobra_ref['data']
                mov_entrada = sobra_ref['mov']
                tipo_entrada = sobra_ref['tipo_especial']
                doc_entrada = sobra_ref['doc']
                user_entrada = sobra_ref['user']
                status_origem = sobra_ref['status_audit']
                valor_real_parado = sum(item['qtd'] * item['valor_unit'] for item in pilha_estoque)
                aging = (hoje - dt_entrada).days if pd.notnull(dt_entrada) else 0
            else:
                dt_entrada = None; mov_entrada = "FLUXO"; tipo_entrada = "N/A"; doc_entrada = "-"; user_entrada = "-"; aging = 0
                valor_real_parado = 0
                status_origem = "OK"

            # --- MATRIZ DE RISCO & STATUS (SCORING MULTICAUSA) ---
            status_lista = []
            score_risco = 0
            causa = "INDEFINIDA"
            tipo_acao = "OPERACIONAL" # Default: Operacional, Financeira, Sistêmica
            acao = "Monitorar"
            log_detalhe = []

            dt_fmt = dt_entrada.strftime('%d/%m/%Y') if pd.notnull(dt_entrada) else "-"

            # 1. DIVERGÊNCIA MB52 (Prioridade de Alerta)
            if tem_divergencia:
                status_lista.append("DIVERGÊNCIA_SISTÊMICA")
                score_risco = max(score_risco, 95)
                tipo_acao = "SISTÊMICA"
                log_detalhe.append(f"[ERRO SISTÊMICO] Saldo Robô: {saldo_reconstruido} vs MB52: ZERO.")

            # 2. FURO DE PILHA (Erro Contábil Grave)
            if doc_furo:
                status_lista.append("FURO_CONTÁBIL")
                causa = "CONSUMO S/ LASTRO"
                score_risco = max(score_risco, 100) # Máximo
                tipo_acao = "FINANCEIRA" # Requer ajuste contábil 701/702
                log_detalhe.append(f"[FURO] Saída doc {doc_furo} sem entrada histórica suficiente.")

            # 3. SEM ID (Erro Operacional Grave)
            if id_dono == 'SEM_ID':
                status_lista.append("SEM_ID")
                causa = "MATERIAL ÓRFÃO"
                score_risco = max(score_risco, 100)
                tipo_acao = "OPERACIONAL" # Cobrar usuário
                acao = "Regularizar ID ou Estornar"
                log_detalhe.append(f"[ÓRFÃO] Usuário entrada: {user_entrada}.")

            # 4. ENTRADA IRREGULAR (Fraude/Erro)
            if status_origem == "IRREGULAR_SEM_HISTORICO":
                status_lista.append("PROCEDIMENTO_INVÁLIDO")
                causa = f"ENTRADA IRREGULAR ({mov_entrada})"
                score_risco = max(score_risco, 90)
                tipo_acao = "OPERACIONAL"
                acao = "Estornar Entrada"
                log_detalhe.append(f"[PROCEDIMENTO] {mov_entrada} em {dt_fmt} sem saída prévia.")

            # 5. AGING (Estagnação)
            if aging > 90:
                status_lista.append("ESTAGNADO")
                if score_risco < 80: score_risco = 80
                log_detalhe.append(f"[AGING] {aging} dias parado.")
                if causa == "INDEFINIDA": 
                    causa = "MATERIAL PARADO"
                    tipo_acao = "LOGÍSTICA"
                acao = "Devolver ao CD"

            # 6. TIPOS DE ENTRADA (Se não for erro grave)
            if score_risco < 90:
                if tipo_entrada == 'ESTORNO':
                    causa = "RETORNO DE OBRA"
                    acao = "Reaplicar Urgente"
                    tipo_acao = "LOGÍSTICA"
                    score_risco = max(score_risco, 60)
                elif tipo_entrada == 'SOBRA_INV':
                    causa = "SOBRA FÍSICA"
                    acao = "Validar origem"
                    tipo_acao = "OPERACIONAL"
                    score_risco = max(score_risco, 40)
                elif tipo_entrada == 'TRANSFORMACAO':
                    causa = "TRANSFORMAÇÃO (309)"
                    score_risco = max(score_risco, 70)
                elif tipo_entrada == 'COMPRA':
                    causa = "COMPRA NOVA"
                    acao = "Validar aplicação"
                    tipo_acao = "LOGÍSTICA"
                    score_risco = max(score_risco, 20)

            # Consolidar Status Multicausa
            if not status_lista: 
                status_final = "PENDENTE"
            else:
                status_final = " + ".join(status_lista)

            # Consolidar Log
            log_final = " | ".join(log_detalhe) if log_detalhe else f"Entrada regular via {mov_entrada}."

            # FRENTE E RESPONSÁVEL
            frente = "IMPLANTAÇÃO"
            nome_empresa = str(grupo[col_nome1].iloc[0]) if col_nome1 else ''
            termos_manut = ['MNT', 'MTN', 'MANUT', 'REPARO', 'CORRETIVA']
            if any(t in nome_empresa.upper() for t in termos_manut): frente = "MANUTENÇÃO"
            
            # Responsável Atual (Inteligente)
            if id_dono == 'SEM_ID':
                responsavel_atual = f"USR_SAP: {user_entrada}" # Cobra o CPF
            else:
                responsavel_atual = nome_empresa # Cobra o CNPJ

            desc_material = str(grupo[col_desc].iloc[0]) if col_desc else ''

            analise.append({
                'SCORE_RISCO': score_risco,
                'STATUS': status_final,
                'TIPO_AÇÃO': tipo_acao, # <--- Nova coluna para VP/Diretoria
                'CAUSA_RAIZ': causa,
                'AÇÃO_SUGERIDA': acao,
                'RESPONSÁVEL_ATUAL': responsavel_atual, # <--- Nova coluna para Cobrança
                'LOG_AUDITORIA': log_final,
                'FRENTE': frente,
                'ID_RECEBEDOR': id_dono,
                'NOME_PARCEIRO': nome_empresa,
                'MATERIAL': material,
                'DESCRIÇÃO': desc_material,
                'CENTRO': centro,
                'DEPÓSITO': deposito,
                'LOTE': lote,
                'SALDO_RECONSTRUÍDO': saldo_reconstruido,
                'SALDO_MB52_REF': saldo_mb52,
                'VALOR_REAL': valor_real_parado,
                'AGING_DIAS': aging,
                'DT_REF_AGING': dt_fmt,
                'DOC_ORIGEM': f"{mov_entrada}-{doc_entrada}",
                'RESPONSÁVEL_MOV': user_entrada
            })

        return pd.DataFrame(analise)