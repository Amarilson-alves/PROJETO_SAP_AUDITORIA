# core/auditoria.py
import pandas as pd
import re
from utils.mapping import FRENTES_PADRAO, MAPEAMENTO_FRENTE
from core.sap_reader import SAPReader

# Dicionário de Aliados (Exatamente como você enviou)
MAPA_ALIADOS_CIDADES = {
    'TLSV ENGENHARIA': 'TLSV',
    'TELEMONT ENGENHARIA': 'TELEMONT',
    'OLITTEL (DENILSON FREIRE DE OLIVEIRA)': 'OLITTEL',
    'CETP TELECOM': 'CETP',
    'TELEPERFORMANCE TELECOM': 'CETP',
    'RICARDO ALEXANDRE': 'RICARDO ALEXANDRE',
    'ABILITY TECNOLOGIA': 'ABILITY',
    'ONDACOM_(COMFICA_SOLUCOES)': 'ONDACOM',
    'NORT SOLUTIONS': 'NORT SOLUTIONS'
}

class AuditoriaAMED:
    def __init__(self, config):
        self.config = config
        self.cols_saida = config['layout_saida']['colunas_finais']

    # Nova função de segurança: Busca a coluna ignorando maiúsculas/minúsculas e espaços
    def _get_val(self, row, target_col):
        target = str(target_col).strip().upper()
        for col in row.index:
            if str(col).strip().upper() == target:
                return row[col]
        return ""

    def _saneamento(self, row):
        obra = str(self._get_val(row, 'OBRA')).upper()
        f_id = str(self._get_val(row, 'FRENTE_ID')).upper()
        t_proj = re.sub(r'\s+', '', str(self._get_val(row, 'Tipo de projeto'))).upper()
        
        if 'SOBREP' in obra: return 'SOBREP'
        if f_id in FRENTES_PADRAO: return f_id
        return MAPEAMENTO_FRENTE.get(t_proj, "")

    def processar_auditoria(self, df_aud, df_cidades, mapa_exec_cen, mapa_exec_dep, mapa_mb52, mapa_centros_mb51={}):
        pd.set_option('future.no_silent_downcasting', True)
        
        # 1. Preparação (Roda a Frente usando a nova lente de segurança)
        df_aud['ID_STR'] = df_aud['ID'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.upper()
        df_aud['SKU_STR'] = df_aud['SKU'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.upper()
        df_aud['UF'] = df_aud['UF'].fillna('').astype(str).str.strip().str.upper()
        df_aud['FRENTE ATUALIZADA'] = df_aud.apply(self._saneamento, axis=1)

        l_centro, l_lvut, l_exec, l_amed, l_valor_amed, l_saldo = [], [], [], [], [], []
        l_unitario_real, l_tipo_dep = [], []

        # Normaliza o dicionário e as colunas de cidades para ignorar espaços nas comparações
        mapa_aliados_norm = {SAPReader.normalize_str(k): v for k, v in MAPA_ALIADOS_CIDADES.items()}
        cols_cidades_limpas_super = [SAPReader.normalize_str(c).replace(" ", "") for c in df_cidades.columns] if not df_cidades.empty else []

        # 2. Motor de Cascata (Geográfico -> Cadastral -> Histórico)
        for _, row in df_aud.iterrows():
            id_b, sku_b = row['ID_STR'], row['SKU_STR']
            
            # Usando o get_val seguro para evitar erro de Case-Sensitive na Sigla/Aliado
            aliado = SAPReader.normalize_str(self._get_val(row, 'Aliado'))
            sigla = SAPReader.normalize_str(self._get_val(row, 'Sigla'))
            frente = str(row.get('FRENTE ATUALIZADA', '')).upper()
            
            centro_final = ""
            
            ignorar_empresas = [SAPReader.normalize_str('SL CONNECT SERVICOS DE TELECOMUNICACOES EIRELI'), 
                                SAPReader.normalize_str('INELCOM BRASIL DE TELECOMUNICA')]
            
            # 🌊 NÍVEL 1: Match Geográfico (Cidades)
            if aliado not in ignorar_empresas and not df_cidades.empty and sigla and sigla in df_cidades.index:
                
                # Roteamento Vivo 
                if 'VIVO' in aliado:
                    col_busca = 'VIVO MANUT' if frente in ['MANUTENÇÃO', 'B2B'] else 'VIVO'
                else:
                    col_busca = mapa_aliados_norm.get(aliado, aliado)

                # Comprime o nome para evitar erro do tipo 'RICARDO ALEXANDRE' vs 'RICARDOALEXANDRE'
                col_busca_limpa_super = SAPReader.normalize_str(col_busca).replace(" ", "")

                if col_busca_limpa_super in cols_cidades_limpas_super:
                    idx_col = cols_cidades_limpas_super.index(col_busca_limpa_super)
                    col_oficial_excel = df_cidades.columns[idx_col]
                    
                    val = df_cidades.loc[sigla, col_oficial_excel]
                    if isinstance(val, pd.Series): val = val.iloc[0] 
                    
                    if pd.notna(val):
                        val_str = SAPReader.normalize_str(val)
                        if val_str and val_str != 'NAN' and val_str != 'N/A':
                            centro_final = val_str

            # 🌊 NÍVEL 2: Resgate Cadastral (Exec Amed)
            if not centro_final:
                centro_final = mapa_exec_cen.get(id_b, "")

            # 🌊 NÍVEL 3: Histórico Operacional (MB51 - Moda)
            if not centro_final:
                centro_final = mapa_centros_mb51.get(id_b, "")
                
            # Tratamento de Falha (Se nada funcionou)
            if not centro_final: centro_final = "N/D"
            l_centro.append(centro_final)

            # 📦 Guarda o Tipo de Depósito na memória para colar no final
            l_tipo_dep.append(mapa_exec_dep.get(id_b, "-"))

            # --- Busca na MB52 ---
            if centro_final == "N/D":
                l_lvut.append(0.0); l_exec.append(0.0); l_amed.append(0.0)
                l_valor_amed.append(0.0); l_saldo.append("NÃO"); l_unitario_real.append(0.0)
            else:
                d_lvut = mapa_mb52.get((sku_b, centro_final, 'LVUT'), {'qtd': 0.0, 'valor': 0.0})
                d_exec = mapa_mb52.get((sku_b, centro_final, 'EXEC'), {'qtd': 0.0, 'valor': 0.0})
                d_amed = mapa_mb52.get((sku_b, centro_final, 'AMED'), {'qtd': 0.0, 'valor': 0.0})

                l_lvut.append(d_lvut['qtd']); l_exec.append(d_exec['qtd']); l_amed.append(d_amed['qtd'])
                l_valor_amed.append(d_amed['valor']) 
                l_saldo.append("SIM" if (d_lvut['qtd']>0 or d_exec['qtd']>0 or d_amed['qtd']>0) else "NÃO")

                qtd_total = d_lvut['qtd'] + d_exec['qtd'] + d_amed['qtd']
                val_total = d_lvut['valor'] + d_exec['valor'] + d_amed['valor']
                if abs(qtd_total) > 0.01: l_unitario_real.append(round(val_total / qtd_total, 2))
                else: l_unitario_real.append(0.0)

        # Atribuição Básica
        df_aud['CENTRO'] = l_centro
        df_aud['QTDE LVUT'], df_aud['QTDE EXEC'] = l_lvut, l_exec
        df_aud['QTDE AMED'], df_aud['$ VALOR - AMED'] = l_amed, l_valor_amed
        df_aud['POSSUI SALDO'] = l_saldo
        df_aud['$ VALOR UNIT'] = l_unitario_real 

        # 3. Financeiro
        for c in ['APL x DRAFT', 'APL x MEDIÇÃO']: df_aud[c] = pd.to_numeric(df_aud[c], errors='coerce').fillna(0)
        
        # Correção segura para a máscara da VIVO usando a lente _get_val
        aliado_series = df_aud.apply(lambda r: self._get_val(r, 'Aliado'), axis=1)
        mask_vivo = aliado_series.astype(str).str.upper().str.contains('VIVO', na=False)
        
        df_aud['SALDO_AUDIT'] = df_aud['APL x MEDIÇÃO'].fillna(0)
        df_aud.loc[mask_vivo, 'SALDO_AUDIT'] = df_aud['APL x DRAFT'].fillna(0)
        df_aud['QTDE APLICAR'] = df_aud['SALDO_AUDIT'].round(4)
        df_aud['$ SALDO X QTDE'] = (df_aud['QTDE APLICAR'] * df_aud['$ VALOR UNIT'] * -1).round(2)

        # 4. Auditoria Lógica (Status)
        livro_faltas = {}
        
        # Correção segura para 'Status ID'
        status_id_series = df_aud.apply(lambda r: self._get_val(r, 'Status ID'), axis=1)
        mask_ativos = status_id_series.astype(str).str.upper() != "CANCELADO"
        
        for _, r in df_aud[(df_aud['SALDO_AUDIT'] < 0) & mask_ativos & (df_aud['UF'] != "")].iterrows():
            aliado_str = str(self._get_val(r, 'Aliado')).upper()
            ch = (r['SKU_STR'], aliado_str, r['UF'])
            if ch not in livro_faltas: livro_faltas[ch] = []
            livro_faltas[ch].append({'ID': str(r['ID']), 'FALTA': abs(r['SALDO_AUDIT'])})

        l_st, l_ac, l_sg = [], [], []
        for _, r in df_aud.iterrows():
            if str(self._get_val(r, 'Status ID')).upper() == "CANCELADO":
                l_st.append("Cancelado"); l_ac.append("Cancelado"); l_sg.append("Cancelado"); continue

            uf, saldo = r['UF'], r['SALDO_AUDIT']
            if saldo > 0 and (uf == "" or uf == "NAN"):
                l_st.append("UF ERRO"); l_ac.append("REVISAR"); l_sg.append("UF vazia"); continue

            if round(saldo, 4) == 0: l_st.append("OK"); l_ac.append("APLICADO"); l_sg.append("Ciclo OK")
            elif saldo < 0: l_st.append("FALTA"); l_ac.append("APLICAR"); l_sg.append("Zerar AMED")
            else:
                sobra, sugs = saldo, []
                aliado_str = str(self._get_val(r, 'Aliado')).upper()
                ch = (r['SKU_STR'], aliado_str, uf)
                if uf != "" and ch in livro_faltas:
                    for item in livro_faltas[ch]:
                        if round(sobra, 4) <= 0: break
                        if item['ID'] == str(r['ID']): continue
                        if round(item['FALTA'], 4) <= 0: continue
                        abat = min(sobra, item['FALTA'])
                        sugs.append(f"APLICAR {round(abat, 4)} EM {item['ID']}")
                        item['FALTA'] -= abat; sobra -= abat
                if sugs: l_st.append("APLICAÇÃO EXTERNA"); l_ac.append("CORREÇÃO"); l_sg.append(" | ".join(sugs))
                else: l_st.append("ESTORNO"); l_ac.append("JUSTIFICATIVA"); l_sg.append("Devolver")

        df_aud['STATUS'], df_aud['AÇÃO'], df_aud['SUGESTÃO'] = l_st, l_ac, l_sg
        df_aud['RESULTADO_OPERACIONAL'] = df_aud['STATUS']

        # 🛡️ DEGRADAÇÃO ELEGANTE
        mask_verif = df_aud['CENTRO'] == "N/D"
        cols_protegidas = ['QTDE LVUT', 'QTDE EXEC', 'QTDE AMED', '$ VALOR - AMED', 'POSSUI SALDO', '$ VALOR UNIT', 'QTDE APLICAR', '$ SALDO X QTDE']
        
        for col in cols_protegidas:
            if col in df_aud.columns:
                df_aud[col] = df_aud[col].astype(object)

        for col in cols_protegidas:
            df_aud.loc[mask_verif, col] = "Verif.Centro"
        
        df_aud.loc[mask_verif, 'CENTRO'] = ""

        # 🔥 AGORA SIM! Adiciona o Tipo de Depósito como a ÚLTIMA coluna do dataframe
        df_aud['TIPO DE DEPÓSITO'] = l_tipo_dep

        for col in self.cols_saida:
            if col not in df_aud.columns and col != "VALIDAÇÃO CONTÁBIL": 
                df_aud[col] = "N/A"

        return df_aud.drop(columns=['ID_STR', 'SKU_STR', 'SALDO_AUDIT'], errors='ignore')