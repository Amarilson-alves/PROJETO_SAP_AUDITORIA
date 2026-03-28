# core/auditoria.py
import pandas as pd
import re

# Importação limpa e direta das regras de negócio do arquivo oficial
from utils.mapping import FRENTES_PADRAO, MAPEAMENTO_FRENTE, MAPA_ALIADOS_CIDADES
from core.sap_reader import SAPReader

class AuditoriaAMED:
    def __init__(self, config):
        self.config = config
        self.cols_saida = config['layout_saida']['colunas_finais']

    # Função de segurança: Busca a coluna ignorando maiúsculas/minúsculas e espaços
    def _get_val(self, row, target_col):
        target = str(target_col).strip().upper()
        for col in row.index:
            if str(col).strip().upper() == target:
                return row[col]
        return ""

    def processar_auditoria(self, df_aud, df_cidades, mapa_exec_cen, mapa_exec_dep, mapa_mb52, mapa_centros_mb51={}):
        pd.set_option('future.no_silent_downcasting', True)
        
        # Mapeamento de colunas em O(1) antes do loop
        col_map = {str(c).strip().upper(): c for c in df_aud.columns}
        col_aliado = col_map.get('ALIADO')
        col_sigla = col_map.get('SIGLA')
        col_obra = col_map.get('OBRA')
        col_frente_id = col_map.get('FRENTE_ID')
        col_tipo_proj = col_map.get('TIPO DE PROJETO')
        col_status_id = col_map.get('STATUS ID')

        # Função de Saneamento otimizada
        def _saneamento_rapido(row):
            obra = str(row[col_obra]).upper() if col_obra and pd.notna(row[col_obra]) else ""
            f_id = str(row[col_frente_id]).strip().upper() if col_frente_id and pd.notna(row[col_frente_id]) else ""
            t_proj_raw = str(row[col_tipo_proj]) if col_tipo_proj and pd.notna(row[col_tipo_proj]) else ""
            t_proj = re.sub(r'\s+', '', t_proj_raw).upper()
            
            if 'SOBREP' in obra: return 'SOBREP'
            if f_id in FRENTES_PADRAO: return f_id
            return MAPEAMENTO_FRENTE.get(t_proj, "")

        # 1. Preparação Rápida
        df_aud['ID_STR'] = df_aud['ID'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.upper()
        df_aud['SKU_STR'] = df_aud['SKU'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.upper()
        df_aud['UF'] = df_aud['UF'].fillna('').astype(str).str.strip().str.upper()
        df_aud['FRENTE ATUALIZADA'] = df_aud.apply(_saneamento_rapido, axis=1)

        l_centro, l_lvut, l_exec, l_amed, l_valor_amed, l_saldo = [], [], [], [], [], []
        l_unitario_real, l_tipo_dep, l_centro_mb51 = [], [], []

        mapa_aliados_norm = {SAPReader.normalize_str(k): v for k, v in MAPA_ALIADOS_CIDADES.items()}
        cols_cidades_limpas_super = [SAPReader.normalize_str(c).replace(" ", "") for c in df_cidades.columns] if not df_cidades.empty else []

        # Lista de empresas instanciada UMA vez fora do loop
        ignorar_empresas = [SAPReader.normalize_str('SL CONNECT SERVICOS DE TELECOMUNICACOES EIRELI'), 
                            SAPReader.normalize_str('INELCOM BRASIL DE TELECOMUNICA')]

        # 2. Motor de Cascata (Geográfico -> Cadastral -> Histórico)
        for _, row in df_aud.iterrows():
            id_b, sku_b = row['ID_STR'], row['SKU_STR']
            
            # Resgate veloz usando as colunas mapeadas no col_map
            val_aliado = row[col_aliado] if col_aliado and pd.notna(row[col_aliado]) else ""
            val_sigla = row[col_sigla] if col_sigla and pd.notna(row[col_sigla]) else ""
            
            aliado = SAPReader.normalize_str(val_aliado)
            sigla = SAPReader.normalize_str(val_sigla)
            frente = str(row.get('FRENTE ATUALIZADA', '')).upper()
            
            centro_final = ""
            
            # 🌊 NÍVEL 1: Match Geográfico (Cidades)
            if aliado not in ignorar_empresas and not df_cidades.empty and sigla and sigla in df_cidades.index:
                
                if 'VIVO' in aliado:
                    col_busca = 'VIVO MANUT' if frente in ['MANUTENÇÃO', 'B2B'] else 'VIVO'
                else:
                    col_busca = mapa_aliados_norm.get(aliado, aliado)

                col_busca_limpa_super = SAPReader.normalize_str(col_busca).replace(" ", "")

                if col_busca_limpa_super in cols_cidades_limpas_super:
                    idx_col = cols_cidades_limpas_super.index(col_busca_limpa_super)
                    col_oficial_excel = df_cidades.columns[idx_col]
                    
                    val = df_cidades.loc[sigla, col_oficial_excel]
                    # Proteção: Caso o drop_duplicates seja removido no leitor (sap_reader), 
                    # o loc pode retornar uma pd.Series. O iloc[0] blinda o código pegando o primeiro.
                    if isinstance(val, pd.Series): val = val.iloc[0] 
                    
                    if pd.notna(val):
                        val_str = SAPReader.normalize_str(val)
                        if val_str and val_str != 'NAN' and val_str != 'N/A':
                            centro_final = val_str

            # 🌊 NÍVEL 2: Resgate Cadastral (Exec Amed)
            if not centro_final:
                centro_final = mapa_exec_cen.get(id_b, "")

            # 🌊 NÍVEL 3: Histórico Operacional (MB51)
            hist_info = mapa_centros_mb51.get(id_b, {})
            if isinstance(hist_info, dict):
                centro_principal_hist = hist_info.get('principal', "")
                centro_todos_hist = hist_info.get('todos', "")
            else:
                centro_principal_hist = str(hist_info)
                centro_todos_hist = str(hist_info)
            
            l_centro_mb51.append(centro_todos_hist)
            
            if not centro_final:
                centro_final = centro_principal_hist
                
            if not centro_final: centro_final = "N/D"
            l_centro.append(centro_final)

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
                
                # 🔥 CORREÇÃO 1 APLICADA AQUI: Ignora LVUT para definir se "Possui Saldo"
                l_saldo.append("SIM" if (d_exec['qtd'] > 0 or d_amed['qtd'] > 0) else "NÃO")

                qtd_total = d_lvut['qtd'] + d_exec['qtd'] + d_amed['qtd']
                val_total = d_lvut['valor'] + d_exec['valor'] + d_amed['valor']
                if abs(qtd_total) > 0.01: l_unitario_real.append(round(val_total / qtd_total, 2))
                else: l_unitario_real.append(0.0)

        # Atribuição Básica
        df_aud['CENTRO'] = l_centro
        df_aud['CENTRO MB51'] = l_centro_mb51 
        df_aud['QTDE LVUT'], df_aud['QTDE EXEC'] = l_lvut, l_exec
        df_aud['QTDE AMED'], df_aud['$ VALOR - AMED'] = l_amed, l_valor_amed
        df_aud['POSSUI SALDO'] = l_saldo
        df_aud['$ VALOR UNIT'] = l_unitario_real 

        # 3. Financeiro
        for c in ['APL x DRAFT', 'APL x MEDIÇÃO']: df_aud[c] = pd.to_numeric(df_aud[c], errors='coerce').fillna(0)
        
        if col_aliado:
            mask_vivo = df_aud[col_aliado].astype(str).str.upper().str.contains('VIVO', na=False)
        else:
            mask_vivo = pd.Series(False, index=df_aud.index)
        
        df_aud['SALDO_AUDIT'] = df_aud['APL x MEDIÇÃO'].fillna(0)
        df_aud.loc[mask_vivo, 'SALDO_AUDIT'] = df_aud['APL x DRAFT'].fillna(0)
        df_aud['QTDE APLICAR'] = df_aud['SALDO_AUDIT'].round(4)
        df_aud['$ SALDO X QTDE'] = (df_aud['QTDE APLICAR'] * df_aud['$ VALOR UNIT'] * -1).round(2)

        # 4. Auditoria Lógica (Status)
        livro_faltas = {}
        
        if col_status_id:
            mask_ativos = df_aud[col_status_id].astype(str).str.upper() != "CANCELADO"
        else:
            mask_ativos = pd.Series(True, index=df_aud.index)
        
        for _, r in df_aud[(df_aud['SALDO_AUDIT'] < 0) & mask_ativos & (df_aud['UF'] != "")].iterrows():
            aliado_str = str(r[col_aliado]).upper() if col_aliado and pd.notna(r[col_aliado]) else ""
            ch = (r['SKU_STR'], aliado_str, r['UF'])
            if ch not in livro_faltas: livro_faltas[ch] = []
            livro_faltas[ch].append({'ID': str(r['ID']), 'FALTA': abs(r['SALDO_AUDIT'])})

        l_st, l_ac, l_sg = [], [], []
        for _, r in df_aud.iterrows():
            status_id_val = str(r[col_status_id]).upper() if col_status_id and pd.notna(r[col_status_id]) else ""
            if status_id_val == "CANCELADO":
                l_st.append("Cancelado"); l_ac.append("Cancelado"); l_sg.append("Cancelado"); continue

            uf, saldo = r['UF'], r['SALDO_AUDIT']
            if saldo > 0 and (uf == "" or uf == "NAN"):
                l_st.append("UF ERRO"); l_ac.append("REVISAR"); l_sg.append("UF vazia"); continue

            if round(saldo, 4) == 0: l_st.append("OK"); l_ac.append("APLICADO"); l_sg.append("Ciclo OK")
            elif saldo < 0: l_st.append("FALTA"); l_ac.append("APLICAR"); l_sg.append("Zerar AMED")
            else:
                sobra, sugs = saldo, []
                aliado_str = str(r[col_aliado]).upper() if col_aliado and pd.notna(r[col_aliado]) else ""
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

        # 🛡️ DEGRADAÇÃO ELEGANTE (OPÇÃO B APLICADA)
        mask_verif = df_aud['CENTRO'] == "N/D"
        
        # Injeta o aviso "Verif.Centro" APENAS nas colunas de texto/dimensão
        df_aud.loc[mask_verif, 'CENTRO'] = "Verif.Centro"
        df_aud.loc[mask_verif, 'STATUS'] = "Verif.Centro"
        df_aud.loc[mask_verif, 'AÇÃO'] = "Verif.Centro"
        df_aud.loc[mask_verif, 'SUGESTÃO'] = "Mapear Cidade/ID"

        # A Coluna extra TIPO DE DEPÓSITO entra no final da linha
        df_aud['TIPO DE DEPÓSITO'] = l_tipo_dep

        for col in self.cols_saida:
            if col not in df_aud.columns and col != "VALIDAÇÃO CONTÁBIL": 
                df_aud[col] = "N/A"

        return df_aud.drop(columns=['ID_STR', 'SKU_STR', 'SALDO_AUDIT'], errors='ignore')