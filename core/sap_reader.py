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
        self.cols_cfg = config.get('colunas_sap', {})

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

    # ==========================================
    # 🧠 NOVA LÓGICA: ÓCULOS EXTRATOR DE ID
    # ==========================================
    @staticmethod
    def extrair_id_valido(valor):
        """ Extrai blocos numéricos de 5 a 7 dígitos ignorando texto """
        if pd.isna(valor): return None
        texto = str(valor).strip()
        
        # Regex: Procura de 5 a 7 números juntos (que não tenham números colados antes ou depois)
        # Funciona para "549094", "APLICAÇÃO 549094", "ID:549094-ENTREGA"
        matches = re.findall(r'(?<!\d)\d{5,7}(?!\d)', texto)
        
        if matches:
            return matches[0] # Retorna o primeiro ID válido encontrado na frase
        return None

    def _get_col_name(self, df, chave_config):
        padroes = self.cols_cfg.get(chave_config, [])
        if not padroes:
            if chave_config == 'recebedor': padroes = ['RECEBEDOR', 'RECEP.', 'WEMPF']
            elif chave_config == 'texto_cabecalho': padroes = ['TEXTO CABEÇALHO', 'TEXTO', 'SGTXT', 'BKTXT']
            else: return None
        
        for pat in padroes:
            found = next((c for c in df.columns if pat.upper() in str(c).upper()), None)
            if found: return found
        return None

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
    # 🌍 MAPA DE CENTROS POR ID (GEO TRACKING)
    # ---------------------------------------------------------
    def gerar_mapa_centros_por_id(self):
        caminho_mb51 = self.config['arquivos']['mb51']
        if not os.path.exists(caminho_mb51): return {}

        df = pd.read_excel(caminho_mb51, engine='calamine')
        col_rec = self._get_col_name(df, 'recebedor')
        col_texto = self._get_col_name(df, 'texto_cabecalho')
        col_cen = self._get_col_name(df, 'centro')

        if not col_cen: return {}

        # 🧠 APLICAÇÃO DO RESGATE DE ID (Hierarquia H -> J)
        if col_rec: df['ID_H'] = df[col_rec].apply(self.extrair_id_valido)
        else: df['ID_H'] = None
        
        if col_texto: df['ID_J'] = df[col_texto].apply(self.extrair_id_valido)
        else: df['ID_J'] = None

        # combine_first: Se ID_H for nulo, pega do ID_J.
        df['ID_LIMPO'] = df['ID_H'].combine_first(df['ID_J']).fillna('SEM_ID')
        df['CENTRO_LIMPO'] = df[col_cen].astype(str).str.strip()
        
        df = df[df['ID_LIMPO'] != 'SEM_ID']
        mapa = df.groupby('ID_LIMPO')['CENTRO_LIMPO'].apply(lambda x: ' | '.join(sorted(set(x)))).to_dict()
        return mapa

    # ---------------------------------------------------------
    # 🕵️‍♂️ AUDITORIA CONTÍNUA (ENTERPRISE v17.2)
    # ---------------------------------------------------------
    def gerar_raio_x_amed(self, mapa_mb52_referencia):
        caminho_mb51 = self.config['arquivos']['mb51']
        caminho_dim = self.config['arquivos']['dim_movimentos']
        
        if not os.path.exists(caminho_mb51) or not os.path.exists(caminho_dim): return pd.DataFrame()

        print("   ☢️ Iniciando Motor de Auditoria Contínua...")
        
        df_dim = pd.read_csv(caminho_dim, sep=';', dtype=str)
        if df_dim['BWART'].duplicated().any():
            duplicados = df_dim[df_dim['BWART'].duplicated()]['BWART'].tolist()
            raise ValueError(f"❌ ERRO CRÍTICO: Movimentos duplicados no CSV: {duplicados}. Corrija o arquivo!")

        df_dim['BWART'] = df_dim['BWART'].str.strip()
        if 'TIPO_ESPECIAL' not in df_dim.columns: df_dim['TIPO_ESPECIAL'] = 'PADRAO'
        lista_saida = df_dim[df_dim['SENTIDO_AMED'] == 'SAIDA']['BWART'].unique()
        
        df = pd.read_excel(caminho_mb51, engine='calamine')

        col_centro = self._get_col_name(df, 'centro')
        col_dep = self._get_col_name(df, 'deposito')
        col_mat = self._get_col_name(df, 'material')
        col_desc = self._get_col_name(df, 'descricao')
        col_qtd = self._get_col_name(df, 'quantidade')
        col_val = self._get_col_name(df, 'valor')
        col_data = self._get_col_name(df, 'data_lanc')
        col_mov = self._get_col_name(df, 'movimento')
        col_rec = self._get_col_name(df, 'recebedor')
        col_texto = self._get_col_name(df, 'texto_cabecalho') # <--- Lendo Coluna J
        col_doc = self._get_col_name(df, 'documento')
        col_item = self._get_col_name(df, 'item')
        col_user = self._get_col_name(df, 'usuario')
        col_lote = self._get_col_name(df, 'lote')
        col_nome1 = self._get_col_name(df, 'nome1')

        if col_data: df[col_data] = pd.to_datetime(df[col_data], errors='coerce')
        
        df[col_mov] = df[col_mov].astype(str).str.strip()
        df[col_dep] = df[col_dep].astype(str).str.strip().str.upper()
        df[col_mat] = df[col_mat].apply(lambda x: self.normalize_str(x))
        df[col_centro] = df[col_centro].apply(lambda x: self.normalize_str(x))
        
        # 🧠 APLICAÇÃO DO RESGATE DE ID (Hierarquia H -> J)
        if col_rec: df['ID_H'] = df[col_rec].apply(self.extrair_id_valido)
        else: df['ID_H'] = None
        
        if col_texto: df['ID_J'] = df[col_texto].apply(self.extrair_id_valido)
        else: df['ID_J'] = None

        # O robô tenta o H, se falhar, puxa do J. Se os dois falharem, é 'SEM_ID'
        df['ID_LIMPO'] = df['ID_H'].combine_first(df['ID_J']).fillna('SEM_ID')
        
        if col_lote:
            df['LOTE_LIMPO'] = df[col_lote].fillna('SEM_LOTE').astype(str).str.strip()
            df.loc[df['LOTE_LIMPO'] == '', 'LOTE_LIMPO'] = 'SEM_LOTE'
        else:
            df['LOTE_LIMPO'] = 'SEM_LOTE'

        df_merged = df.merge(df_dim[['BWART', 'SENTIDO_AMED', 'TIPO_ESPECIAL']], left_on=col_mov, right_on='BWART', how='left')
        df_merged['SENTIDO_REAL'] = np.where(df_merged[col_dep].str.contains('AMED', na=False), df_merged['SENTIDO_AMED'], 'NEUTRO')

        df_proc = df_merged[df_merged['SENTIDO_REAL'] != 'NEUTRO'].copy()
        
        # Agora o groupby empilha as linhas usando o ID REAL (resgatado)
        grupos = df_proc.groupby(['ID_LIMPO', col_mat, col_centro, col_dep, 'LOTE_LIMPO'])
        analise = []
        hoje = datetime.now()
        
        count_ignored = 0
        count_processed = 0

        print(f"   📊 Processando {len(grupos)} pilhas de estoque consolidadas...")

        for (id_dono, material, centro, deposito, lote), grupo in grupos:
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
                        tem_consumo_previo = any((h['mov'] in lista_saida) and (h['data'] <= data_mov) for h in historico_consumo)
                        if not tem_consumo_previo: status_entrada = "IRREGULAR_SEM_HISTORICO"
                    pilha_estoque.append({'qtd': qtd, 'valor_unit': valor_unit, 'data': data_mov, 'mov': mov, 'tipo_especial': tipo_especial, 'doc': row[col_doc], 'user': row[col_user], 'status_audit': status_entrada})
                elif sentido == 'SAIDA':
                    historico_consumo.append({'mov': mov, 'data': data_mov})
                    qtd_a_baixar = qtd
                    if not pilha_estoque and doc_furo is None: doc_furo = f"{mov}-{row[col_doc]}"
                    while qtd_a_baixar > 0.001 and pilha_estoque:
                        lote_atual = pilha_estoque[-1]
                        if lote_atual['qtd'] <= qtd_a_baixar:
                            qtd_a_baixar -= lote_atual['qtd']
                            pilha_estoque.pop()
                        else:
                            lote_atual['qtd'] -= qtd_a_baixar
                            qtd_a_baixar = 0
                    if qtd_a_baixar > 0.001 and doc_furo is None: doc_furo = f"{mov}-{row[col_doc]}"

            saldo_reconstruido = sum(item['qtd'] for item in pilha_estoque)
            chave_mb52 = (material, centro, deposito)
            saldo_mb52 = mapa_mb52_referencia.get(chave_mb52, {}).get('qtd', 0)
            tem_divergencia = (saldo_reconstruido > 0.1 and saldo_mb52 < 0.01)

            if saldo_reconstruido < 0.001 and id_dono != 'SEM_ID' and not doc_furo and not tem_divergencia:
                count_ignored += 1
                continue
            
            count_processed += 1

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

            status_lista = []
            score_risco = 0
            causa = "INDEFINIDA"
            tipo_acao = "OPERACIONAL"
            acao = "Monitorar"
            log_detalhe = []

            dt_fmt = dt_entrada.strftime('%d/%m/%Y') if pd.notnull(dt_entrada) else "-"

            if tem_divergencia:
                status_lista.append("DIVERGÊNCIA_SISTÊMICA")
                score_risco = max(score_risco, 95)
                tipo_acao = "SISTÊMICA"
                log_detalhe.append(f"[ERRO SISTÊMICO] Saldo Robô: {saldo_reconstruido} vs MB52: ZERO.")

            if doc_furo:
                status_lista.append("FURO_CONTÁBIL")
                causa = "CONSUMO S/ LASTRO"
                score_risco = max(score_risco, 100)
                tipo_acao = "FINANCEIRA"
                log_detalhe.append(f"[FURO] Saída doc {doc_furo} sem entrada histórica suficiente.")

            if id_dono == 'SEM_ID':
                status_lista.append("SEM_ID")
                causa = "MATERIAL ÓRFÃO"
                score_risco = max(score_risco, 100)
                tipo_acao = "OPERACIONAL"
                acao = "Regularizar ID ou Estornar"
                log_detalhe.append(f"[ÓRFÃO] Usuário entrada: {user_entrada}.")

            if status_origem == "IRREGULAR_SEM_HISTORICO":
                status_lista.append("PROCEDIMENTO_INVÁLIDO")
                causa = f"ENTRADA IRREGULAR ({mov_entrada})"
                score_risco = max(score_risco, 90)
                tipo_acao = "OPERACIONAL"
                acao = "Estornar Entrada"
                log_detalhe.append(f"[PROCEDIMENTO] {mov_entrada} sem saída prévia.")

            if aging > 90:
                status_lista.append("ESTAGNADO")
                if score_risco < 80: score_risco = 80
                log_detalhe.append(f"[AGING] {aging} dias parado.")
                if causa == "INDEFINIDA": causa = "MATERIAL PARADO"; tipo_acao = "LOGÍSTICA"
                acao = "Devolver ao CD"

            if score_risco < 90:
                if tipo_entrada == 'ESTORNO':
                    causa = "RETORNO DE OBRA"; acao = "Reaplicar Urgente"; tipo_acao = "LOGÍSTICA"; score_risco = max(score_risco, 60)
                elif tipo_entrada == 'SOBRA_INV':
                    causa = "SOBRA FÍSICA"; acao = "Validar origem"; tipo_acao = "OPERACIONAL"; score_risco = max(score_risco, 40)
                elif tipo_entrada == 'TRANSFORMACAO':
                    causa = "TRANSFORMAÇÃO (309)"; score_risco = max(score_risco, 70)
                elif tipo_entrada == 'COMPRA':
                    causa = "COMPRA NOVA"; acao = "Validar aplicação"; tipo_acao = "LOGÍSTICA"; score_risco = max(score_risco, 20)

            status_final = " + ".join(status_lista) if status_lista else "PENDENTE"
            log_final = " | ".join(log_detalhe) if log_detalhe else f"Entrada regular via {mov_entrada}."
            
            frente = "IMPLANTAÇÃO"
            nome_empresa = str(grupo[col_nome1].iloc[0]) if col_nome1 else ''
            termos_manut = ['MNT', 'MTN', 'MANUT', 'REPARO', 'CORRETIVA']
            if any(t in nome_empresa.upper() for t in termos_manut): frente = "MANUTENÇÃO"
            
            if id_dono == 'SEM_ID': responsavel_atual = f"USR_SAP: {user_entrada}"
            else: responsavel_atual = nome_empresa
            desc_material = str(grupo[col_desc].iloc[0]) if col_desc else ''

            analise.append({'SCORE_RISCO': score_risco, 'STATUS': status_final, 'TIPO_AÇÃO': tipo_acao, 'CAUSA_RAIZ': causa, 'AÇÃO_SUGERIDA': acao, 'RESPONSÁVEL_ATUAL': responsavel_atual, 'LOG_AUDITORIA': log_final, 'FRENTE': frente, 'ID_RECEBEDOR': id_dono, 'NOME_PARCEIRO': nome_empresa, 'MATERIAL': material, 'DESCRIÇÃO': desc_material, 'CENTRO': centro, 'DEPÓSITO': deposito, 'LOTE': lote, 'SALDO_RECONSTRUÍDO': saldo_reconstruido, 'SALDO_MB52_REF': saldo_mb52, 'VALOR_REAL': valor_real_parado, 'AGING_DIAS': aging, 'DT_REF_AGING': dt_fmt, 'DOC_ORIGEM': f"{mov_entrada}-{doc_entrada}", 'RESPONSÁVEL_MOV': user_entrada})

        print(f"   🧹 Limpeza: {count_ignored} pilhas ocultadas.")
        print(f"   📋 Total Reportado: {count_processed} registros.")
        
        return pd.DataFrame(analise)