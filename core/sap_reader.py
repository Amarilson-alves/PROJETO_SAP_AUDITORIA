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
        self.regras = config.get('regras_negocio', {})

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

    @staticmethod
    def extrair_id_valido(valor):
        if pd.isna(valor): return None
        texto = str(valor).strip()
        matches = re.findall(r'(?<!\d)\d{5,7}(?!\d)', texto)
        if matches: return matches[0]
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

    # --- LEITURAS DE CENTROS ---
    def carregar_centro_cidades(self):
        caminho = self.config['arquivos'].get('centro_cidades')
        if not caminho or not os.path.exists(caminho): 
            print(f"   [ERRO] Matriz Cidades não encontrada no caminho: {caminho}")
            return pd.DataFrame()
        
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        
        head_idx = -1
        for i, row in df_raw.head(20).iterrows():
            row_strs = [str(c).upper() for c in row.values]
            if any('CÓDIGO' in c or 'CODIGO' in c for c in row_strs):
                head_idx = i
                break
        
        if head_idx == -1:
            print("   [ERRO] Cabeçalho (CÓDIGO) não detectado na Matriz Cidades.")
            return pd.DataFrame()
            
        df = df_raw.iloc[head_idx+1:].copy()
        df.columns = [str(c).strip().upper() for c in df_raw.iloc[head_idx]]
        
        col_codigo = next((c for c in df.columns if 'CÓDIGO' in c or 'CODIGO' in c), None)
        if col_codigo:
            df[col_codigo] = df[col_codigo].apply(self.normalize_str)
            df.set_index(col_codigo, inplace=True)
            df = df[~df.index.duplicated(keep='first')]
            print(f"   [SUCESSO] Matriz Cidades: {len(df)} siglas mapeadas.")
            
        return df

    def carregar_centro_exec_amed(self):
        caminho = self.config['arquivos'].get('centro_exec_amed')
        if not caminho or not os.path.exists(caminho): 
            print(f"   [ERRO] Planilha EXEC AMED não encontrada no caminho: {caminho}")
            return {}, {}
        
        df = pd.read_excel(caminho, engine='calamine', header=None)
        mapa_cen = {}
        mapa_dep = {}
        
        for idx, r in df.iterrows():
            try:
                id_val = self.normalize_str(r.iloc[12])
                cen_val = self.normalize_str(r.iloc[3])
                dep_val = self.normalize_str(r.iloc[13])
                
                if not id_val or id_val == 'ID' or id_val == 'NAN': continue

                if id_val not in mapa_cen and cen_val and cen_val != 'NAN':
                    mapa_cen[id_val] = cen_val

                if dep_val and dep_val != 'NAN':
                    if id_val not in mapa_dep: mapa_dep[id_val] = set()
                    mapa_dep[id_val].add(dep_val)
            except Exception as e:
                print(f"   [AVISO] EXEC AMED: Linha {idx+1} ignorada. Motivo: {str(e)}")
                continue
            
        mapa_dep_final = {k: ' | '.join(sorted(v)) for k, v in mapa_dep.items()}
        print(f"   [SUCESSO] Matriz Exec/Amed: {len(mapa_cen)} IDs mapeados.")
        return mapa_cen, mapa_dep_final

    def gerar_mapa_centros_por_id(self):
        caminho_mb51 = self.config['arquivos']['mb51']
        if not os.path.exists(caminho_mb51): return {}

        df = pd.read_excel(caminho_mb51, engine='calamine')
        col_rec = self._get_col_name(df, 'recebedor')
        col_texto = self._get_col_name(df, 'texto_cabecalho')
        col_cen = self._get_col_name(df, 'centro')

        if not col_cen: return {}

        if col_rec: df['ID_H'] = df[col_rec].apply(self.extrair_id_valido)
        else: df['ID_H'] = None
        if col_texto: df['ID_J'] = df[col_texto].apply(self.extrair_id_valido)
        else: df['ID_J'] = None

        df['ID_LIMPO'] = df['ID_H'].combine_first(df['ID_J']).fillna('SEM_ID')
        df['CENTRO_LIMPO'] = df[col_cen].astype(str).str.strip()
        
        df = df[df['ID_LIMPO'] != 'SEM_ID']
        
        def get_principal(x):
            modes = x.mode()
            return modes.iloc[0] if not modes.empty else x.iloc[0]
            
        def get_todos(x):
            return " | ".join(sorted(set(x)))
            
        agrupado = df.groupby('ID_LIMPO').agg(
            principal=('CENTRO_LIMPO', get_principal),
            todos=('CENTRO_LIMPO', get_todos)
        )
        
        mapa = agrupado.to_dict('index')
        return mapa

    # --- LEITURAS ANTIGAS E BASES ---
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
            except Exception as e: 
                print(f"   [AVISO] MB52: Linha {idx_original+1} ignorada. Motivo: {str(e)}")
                continue
        return mapa, pd.DataFrame(evidencias)
    
    # 🔥 CORREÇÃO: Função com limpeza de linhas fantasmas
    def carregar_base_auditoria(self):
        caminho = self.config['arquivos'].get('base_auditoria')
        if not caminho or not os.path.exists(caminho): raise FileNotFoundError(f"Base Auditoria não encontrada: {caminho}")
        
        df_raw = pd.read_excel(caminho, engine='calamine', header=None)
        head = next(i for i, row in df_raw.head(20).iterrows() if 'SKU' in [str(c).upper() for c in row.values])
        df = df_raw.iloc[head+1:].reset_index(drop=True)
        df.columns = [str(c).strip() for c in df_raw.iloc[head]]
        
        # 1. Remove colunas vazias
        df = df.loc[:, ~df.columns.str.contains('^nan$|^Unnamed', case=False, na=True)]
        
        # 2. Remove linhas fantasmas (onde o SKU está vazio/NaN)
        if 'SKU' in df.columns:
            df = df.dropna(subset=['SKU']).copy()
            
        return df

    # --- MOTORES CONTÍNUOS ---
    def gerar_raio_x_amed(self, mapa_mb52_referencia):
        caminho_mb51 = self.config['arquivos']['mb51']
        caminho_dim = self.config['arquivos']['dim_movimentos']
        
        if not os.path.exists(caminho_mb51) or not os.path.exists(caminho_dim): return pd.DataFrame()

        print("   ☢️ Iniciando Motor de Auditoria Contínua (Raio-X)...")
        
        df_dim = pd.read_csv(caminho_dim, sep=';', dtype=str)
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
        col_texto = self._get_col_name(df, 'texto_cabecalho') 
        col_doc = self._get_col_name(df, 'documento')
        col_item = self._get_col_name(df, 'item')
        col_user = self._get_col_name(df, 'usuario')
        col_lote = self._get_col_name(df, 'lote')
        col_nome1 = self._get_col_name(df, 'nome1')

        if col_data: df[col_data] = pd.to_datetime(df[col_data], errors='coerce')
        
        df[col_mov] = df[col_mov].astype(str).str.strip()
        df[col_dep] = df[col_dep].astype(str).str.strip().str.upper()
        
        df[col_mat] = df[col_mat].apply(self.normalize_str)
        df[col_centro] = df[col_centro].apply(self.normalize_str)
        
        if col_rec: df['ID_H'] = df[col_rec].apply(self.extrair_id_valido)
        else: df['ID_H'] = None
        if col_texto: df['ID_J'] = df[col_texto].apply(self.extrair_id_valido)
        else: df['ID_J'] = None
        df['ID_LIMPO'] = df['ID_H'].combine_first(df['ID_J']).fillna('SEM_ID')
        
        if col_lote:
            df['LOTE_LIMPO'] = df[col_lote].fillna('SEM_LOTE').astype(str).str.strip()
            df.loc[df['LOTE_LIMPO'] == '', 'LOTE_LIMPO'] = 'SEM_LOTE'
        else:
            df['LOTE_LIMPO'] = 'SEM_LOTE'

        df_merged = df.merge(df_dim[['BWART', 'SENTIDO_AMED', 'TIPO_ESPECIAL']], left_on=col_mov, right_on='BWART', how='left')
        df_merged['SENTIDO_REAL'] = np.where(df_merged[col_dep].str.contains('AMED', na=False), df_merged['SENTIDO_AMED'], 'NEUTRO')

        df_proc = df_merged[df_merged['SENTIDO_REAL'] != 'NEUTRO'].copy()
        grupos = df_proc.groupby(['ID_LIMPO', col_mat, col_centro, col_dep, 'LOTE_LIMPO'])
        analise = []
        hoje = datetime.now()
        
        termos_manut = self.regras.get('termos_manutencao', ['MNT', 'MTN', 'MANUT', 'REPARO', 'CORRETIVA'])

        print(f"   📊 Processando {len(grupos)} pilhas de estoque consolidadas (Raio-X)...")

        for (id_dono, material, centro, deposito, lote), grupo in grupos:
            cols_sort = [col_data, col_doc]
            if col_item: cols_sort.append(col_item)
            grupo = grupo.sort_values(by=cols_sort)

            pilha_estoque = []
            historico_consumo = [] 
            doc_furo = None
            
            col_indices = {col: i for i, col in enumerate(grupo.columns)}
            
            for row_tuple in grupo.itertuples(index=False, name=None):
                qtd = abs(self.converter_sap_br(row_tuple[col_indices[col_qtd]]))
                val_total = abs(self.converter_sap_br(row_tuple[col_indices[col_val]])) if col_val else 0
                sentido = row_tuple[col_indices['SENTIDO_REAL']]
                tipo_especial = str(row_tuple[col_indices['TIPO_ESPECIAL']])
                data_mov = row_tuple[col_indices[col_data]]
                mov = row_tuple[col_indices[col_mov]]
                
                doc_atual = row_tuple[col_indices[col_doc]] if col_doc else "-"
                usr_atual = row_tuple[col_indices[col_user]] if col_user else "-"

                if sentido == 'ENTRADA':
                    valor_unit = (val_total / qtd) if qtd > 0 else 0
                    status_entrada = "OK"
                    if tipo_especial == 'ESTORNO':
                        tem_consumo_previo = any((h['mov'] in lista_saida) and (h['data'] <= data_mov) for h in historico_consumo)
                        if not tem_consumo_previo: status_entrada = "IRREGULAR_SEM_HISTORICO"
                    pilha_estoque.append({'qtd': qtd, 'valor_unit': valor_unit, 'data': data_mov, 'mov': mov, 'tipo_especial': tipo_especial, 'doc': doc_atual, 'user': usr_atual, 'status_audit': status_entrada})
                elif sentido == 'SAIDA':
                    historico_consumo.append({'mov': mov, 'data': data_mov})
                    qtd_a_baixar = qtd
                    if not pilha_estoque and doc_furo is None: doc_furo = f"{mov}-{doc_atual}"
                    while qtd_a_baixar > 0.001 and pilha_estoque:
                        lote_atual = pilha_estoque[-1]
                        if lote_atual['qtd'] <= qtd_a_baixar:
                            qtd_a_baixar -= lote_atual['qtd']
                            pilha_estoque.pop()
                        else:
                            lote_atual['qtd'] -= qtd_a_baixar
                            qtd_a_baixar = 0
                    if qtd_a_baixar > 0.001 and doc_furo is None: doc_furo = f"{mov}-{doc_atual}"

            saldo_reconstruido = sum(item['qtd'] for item in pilha_estoque)
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

            status_lista = []
            score_risco = 0
            causa = "INDEFINIDA"
            tipo_acao = "OPERACIONAL"
            acao = "Monitorar"
            log_detalhe = []
            dt_fmt = dt_entrada.strftime('%d/%m/%Y') if pd.notnull(dt_entrada) else "-"

            if tem_divergencia:
                status_lista.append("DIVERGÊNCIA_SISTÊMICA")
                score_risco = max(score_risco, 95); tipo_acao = "SISTÊMICA"
                log_detalhe.append(f"[ERRO SISTÊMICO] Saldo Robô: {saldo_reconstruido} vs MB52: ZERO.")

            if doc_furo:
                status_lista.append("FURO_CONTÁBIL")
                causa = "CONSUMO S/ LASTRO"; score_risco = max(score_risco, 100); tipo_acao = "FINANCEIRA"
                log_detalhe.append(f"[FURO] Saída doc {doc_furo} sem entrada histórica suficiente.")

            if id_dono == 'SEM_ID':
                status_lista.append("SEM_ID")
                causa = "MATERIAL ÓRFÃO"; score_risco = max(score_risco, 100); tipo_acao = "OPERACIONAL"
                acao = "Regularizar ID ou Estornar"
                log_detalhe.append(f"[ÓRFÃO] Usuário entrada: {user_entrada}.")

            if status_origem == "IRREGULAR_SEM_HISTORICO":
                status_lista.append("PROCEDIMENTO_INVÁLIDO")
                causa = f"ENTRADA IRREGULAR ({mov_entrada})"
                score_risco = max(score_risco, 90); tipo_acao = "OPERACIONAL"; acao = "Estornar Entrada"
                log_detalhe.append(f"[PROCEDIMENTO] {mov_entrada} sem saída prévia.")

            status_final = " + ".join(status_lista) if status_lista else "PENDENTE"
            log_final = " | ".join(log_detalhe) if log_detalhe else f"Entrada regular via {mov_entrada}."
            
            frente = "IMPLANTAÇÃO"
            nome_empresa = str(grupo[col_nome1].iloc[0]) if col_nome1 else ''
            
            if any(t in nome_empresa.upper() for t in termos_manut): frente = "MANUTENÇÃO"
            
            if id_dono == 'SEM_ID': responsavel_atual = f"USR_SAP: {user_entrada}"
            else: responsavel_atual = nome_empresa
            desc_material = str(grupo[col_desc].iloc[0]) if col_desc else ''

            analise.append({'SCORE_RISCO': score_risco, 'STATUS': status_final, 'TIPO_AÇÃO': tipo_acao, 'CAUSA_RAIZ': causa, 'AÇÃO_SUGERIDA': acao, 'RESPONSÁVEL_ATUAL': responsavel_atual, 'LOG_AUDITORIA': log_final, 'FRENTE': frente, 'ID_RECEBEDOR': id_dono, 'NOME_PARCEIRO': nome_empresa, 'MATERIAL': material, 'DESCRIÇÃO': desc_material, 'CENTRO': centro, 'DEPÓSITO': deposito, 'LOTE': lote, 'SALDO_RECONSTRUÍDO': saldo_reconstruido, 'SALDO_MB52_REF': saldo_mb52, 'VALOR_REAL': valor_real_parado, 'AGING_DIAS': aging, 'DT_REF_AGING': dt_fmt, 'DOC_ORIGEM': f"{mov_entrada}-{doc_entrada}", 'RESPONSÁVEL_MOV': user_entrada})

        return pd.DataFrame(analise)

    def gerar_extrato_diario(self, dias_retroativos=180):
        caminho_mb51 = self.config['arquivos']['mb51']
        if not os.path.exists(caminho_mb51): return pd.DataFrame()

        print(f"   📅 Gerando Extrato Diário da Operação (Últimos {dias_retroativos} dias)...")
        df = pd.read_excel(caminho_mb51, engine='calamine')

        col_centro = self._get_col_name(df, 'centro')
        col_dep = self._get_col_name(df, 'deposito')
        col_mat = self._get_col_name(df, 'material')
        col_desc = self._get_col_name(df, 'descricao')
        col_qtd = self._get_col_name(df, 'quantidade')
        col_data = self._get_col_name(df, 'data_lanc')
        col_rec = self._get_col_name(df, 'recebedor')
        col_texto = self._get_col_name(df, 'texto_cabecalho')

        if not col_data: return pd.DataFrame()
        
        df[col_data] = pd.to_datetime(df[col_data], errors='coerce')
        data_limite = datetime.now() - pd.Timedelta(days=dias_retroativos)
        df = df[df[col_data] >= data_limite].copy()

        df[col_dep] = df[col_dep].astype(str).str.strip().str.upper()
        df[col_mat] = df[col_mat].apply(self.normalize_str)
        df[col_centro] = df[col_centro].apply(self.normalize_str)
        df['DESC_LIMPA'] = df[col_desc].astype(str).str.strip()
        
        if col_rec: df['ID_H'] = df[col_rec].apply(self.extrair_id_valido)
        else: df['ID_H'] = None
        if col_texto: df['ID_J'] = df[col_texto].apply(self.extrair_id_valido)
        else: df['ID_J'] = None
        df['ID_LIMPO'] = df['ID_H'].combine_first(df['ID_J']).fillna('SEM_ID')
        
        df['QTD_REAL'] = df[col_qtd].apply(self.converter_sap_br)
        df['QTD_ENTRADA'] = np.where(df['QTD_REAL'] > 0, df['QTD_REAL'], 0.0)
        df['QTD_SAIDA'] = np.where(df['QTD_REAL'] < 0, df['QTD_REAL'].abs(), 0.0)
        df['DATA_DIA'] = df[col_data].dt.strftime('%d/%m/%Y')
        
        agrupado = df.groupby(
            ['DATA_DIA', col_data, col_centro, 'ID_LIMPO', col_mat, 'DESC_LIMPA', col_dep], 
            as_index=False
        ).agg(
            ENTRADAS=('QTD_ENTRADA', 'sum'),
            SAIDAS=('QTD_SAIDA', 'sum')
        )

        agrupado['VARIAÇÃO DO DIA'] = agrupado['ENTRADAS'] - agrupado['SAIDAS']
        agrupado = agrupado[agrupado['VARIAÇÃO DO DIA'].round(3) != 0].copy()

        agrupado.rename(columns={
            'DATA_DIA': 'DATA OPERAÇÃO',
            col_centro: 'CENTRO',
            'ID_LIMPO': 'ID_RECEBEDOR',
            col_mat: 'SKU',
            'DESC_LIMPA': 'DESCRIÇÃO',
            col_dep: 'DEPÓSITO',
            'ENTRADAS': 'ENTRADAS LÍQUIDAS',
            'SAIDAS': 'SAÍDAS LÍQUIDAS'
        }, inplace=True)

        agrupado.sort_values(by=[col_data, 'CENTRO', 'ID_RECEBEDOR', 'SKU'], inplace=True)
        agrupado.drop(columns=[col_data], inplace=True)
        return agrupado

    # --- 🟢 O NOVO MOTOR DE RASTREIO DE APLICAÇÕES ---
    def gerar_rastreio_aplicacoes(self, df_auditoria):
        caminho_mb51 = self.config['arquivos']['mb51']
        if not os.path.exists(caminho_mb51): return pd.DataFrame()

        print("   🔍 Acionando Motor Analítico: Rastreio de Documentos (MB51)...")
        df = pd.read_excel(caminho_mb51, engine='calamine')

        col_centro = self._get_col_name(df, 'centro')
        col_mat = self._get_col_name(df, 'material')
        col_qtd = self._get_col_name(df, 'quantidade')
        col_mov = self._get_col_name(df, 'movimento')
        col_rec = self._get_col_name(df, 'recebedor')
        col_texto = self._get_col_name(df, 'texto_cabecalho')
        col_doc = self._get_col_name(df, 'documento')

        df[col_mov] = df[col_mov].astype(str).str.strip().str.upper()
        
        # Filtro estrito nas aplicações e estornos vitais
        movs_alvo = ['261', '262', 'Z81', 'Z82']
        df_filtro = df[df[col_mov].isin(movs_alvo)].copy()

        if df_filtro.empty:
            print("   ⚠️ Nenhum movimento de aplicação/estorno encontrado na MB51.")
            return pd.DataFrame()

        df_filtro[col_mat] = df_filtro[col_mat].apply(self.normalize_str)
        df_filtro[col_centro] = df_filtro[col_centro].apply(self.normalize_str)
        df_filtro[col_qtd] = df_filtro[col_qtd].apply(self.converter_sap_br).abs()
        df_filtro[col_doc] = df_filtro[col_doc].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

        if col_rec: df_filtro['ID_H'] = df_filtro[col_rec].apply(self.extrair_id_valido)
        else: df_filtro['ID_H'] = None
        if col_texto: df_filtro['ID_J'] = df_filtro[col_texto].apply(self.extrair_id_valido)
        else: df_filtro['ID_J'] = None
        df_filtro['ID_LIMPO'] = df_filtro['ID_H'].combine_first(df_filtro['ID_J']).fillna('SEM_ID')

        df_filtro = df_filtro[df_filtro['ID_LIMPO'] != 'SEM_ID']

        df_filtro['QTD_261'] = np.where(df_filtro[col_mov] == '261', df_filtro[col_qtd], 0.0)
        df_filtro['QTD_Z81'] = np.where(df_filtro[col_mov] == 'Z81', df_filtro[col_qtd], 0.0)
        df_filtro['QTD_262'] = np.where(df_filtro[col_mov] == '262', df_filtro[col_qtd], 0.0)
        df_filtro['QTD_Z82'] = np.where(df_filtro[col_mov] == 'Z82', df_filtro[col_qtd], 0.0)

        df_filtro['DOC_261'] = np.where(df_filtro[col_mov] == '261', df_filtro[col_doc], None)
        df_filtro['DOC_Z81'] = np.where(df_filtro[col_mov] == 'Z81', df_filtro[col_doc], None)
        df_filtro['DOC_262'] = np.where(df_filtro[col_mov] == '262', df_filtro[col_doc], None)
        df_filtro['DOC_Z82'] = np.where(df_filtro[col_mov] == 'Z82', df_filtro[col_doc], None)

        def join_unique(x):
            v = set(str(i) for i in x if pd.notna(i) and str(i).strip() != "")
            return " | ".join(sorted(v)) if v else "-"

        agrupado = df_filtro.groupby(['ID_LIMPO', col_mat]).agg(
            CENTRO_APLICADO=(col_centro, join_unique),
            QTD_261=('QTD_261', 'sum'),
            QTD_Z81=('QTD_Z81', 'sum'),
            QTD_262=('QTD_262', 'sum'),
            QTD_Z82=('QTD_Z82', 'sum'),
            DOCS_261=('DOC_261', join_unique),
            DOCS_Z81=('DOC_Z81', join_unique),
            DOCS_262=('DOC_262', join_unique),
            DOCS_Z82=('DOC_Z82', join_unique)
        ).reset_index()

        agrupado.rename(columns={
            'ID_LIMPO': 'ID_STR', col_mat: 'SKU_STR',
            'CENTRO_APLICADO': 'CENTRO APLICADO SAP',
            'QTD_261': 'QTD APLICADA 261', 'QTD_Z81': 'QTD APLICADA Z81',
            'QTD_262': 'QTD ESTORNADA 262', 'QTD_Z82': 'QTD ESTORNADA Z82',
            'DOCS_261': 'DOCS APLIC. 261', 'DOCS_Z81': 'DOCS APLIC. Z81',
            'DOCS_262': 'DOCS ESTORNO 262', 'DOCS_Z82': 'DOCS ESTORNO Z82'
        }, inplace=True)

        df_base = df_auditoria.copy()
        df_base['ID_STR'] = df_base['ID'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.upper()
        df_base['SKU_STR'] = df_base['SKU'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.upper()

        rastreio = pd.merge(df_base, agrupado, on=['ID_STR', 'SKU_STR'], how='left')

        cols_num = ['QTD APLICADA 261', 'QTD APLICADA Z81', 'QTD ESTORNADA 262', 'QTD ESTORNADA Z82']
        for c in cols_num: rastreio[c] = rastreio[c].fillna(0.0)
            
        cols_txt = ['CENTRO APLICADO SAP', 'DOCS APLIC. 261', 'DOCS APLIC. Z81', 'DOCS ESTORNO 262', 'DOCS ESTORNO Z82']
        for c in cols_txt: rastreio[c] = rastreio[c].fillna("-")

        rastreio['TOTAL APLICADO BRUTO'] = rastreio['QTD APLICADA 261'] + rastreio['QTD APLICADA Z81']
        rastreio['TOTAL ESTORNADO'] = rastreio['QTD ESTORNADA 262'] + rastreio['QTD ESTORNADA Z82']
        rastreio['QTD LÍQUIDA SAP'] = rastreio['TOTAL APLICADO BRUTO'] - rastreio['TOTAL ESTORNADO']
        
        qtd_aplicar_num = pd.to_numeric(rastreio['QTDE APLICAR'], errors='coerce').fillna(0.0)
        rastreio['META DE APLICAÇÃO'] = np.where(qtd_aplicar_num < 0, qtd_aplicar_num.abs(), 0.0)

        rastreio['SALDO PENDENTE'] = rastreio['META DE APLICAÇÃO'] - rastreio['QTD LÍQUIDA SAP']

        c_desc = next((c for c in rastreio.columns if 'DESCRI' in str(c).upper()), 'Descrição')
        c_aliado = next((c for c in rastreio.columns if 'ALIADO' in str(c).upper()), 'Aliado')
        
        rastreio.rename(columns={
            c_desc: 'DESCRIÇÃO',
            c_aliado: 'ALIADO',
            'CENTRO': 'CENTRO PLANEJADO'
        }, inplace=True)
        
        # Layout Final
        cols_to_keep = [
            'ID', 'SKU', 'DESCRIÇÃO', 'ALIADO', 'CENTRO PLANEJADO', 'CENTRO APLICADO SAP',
            'META DE APLICAÇÃO', 'QTD APLICADA 261', 'QTD APLICADA Z81', 'TOTAL APLICADO BRUTO',
            'QTD ESTORNADA 262', 'QTD ESTORNADA Z82', 'TOTAL ESTORNADO',
            'QTD LÍQUIDA SAP', 'SALDO PENDENTE',
            'DOCS APLIC. 261', 'DOCS APLIC. Z81', 'DOCS ESTORNO 262', 'DOCS ESTORNO Z82'
        ]
        
        cols_final = [c for c in cols_to_keep if c in rastreio.columns]
        rastreio_final = rastreio[cols_final]
        
        mask_relevante = (rastreio_final['META DE APLICAÇÃO'] > 0) | (rastreio_final['TOTAL APLICADO BRUTO'] > 0) | (rastreio_final['TOTAL ESTORNADO'] > 0)
        rastreio_final = rastreio_final[mask_relevante].copy()
        
        rastreio_final.sort_values(by=['ALIADO', 'ID'], inplace=True)

        print(f"   🎯 Concluído! Rastreabilidade cruzada: {len(rastreio_final)} materiais encontrados.")
        return rastreio_final