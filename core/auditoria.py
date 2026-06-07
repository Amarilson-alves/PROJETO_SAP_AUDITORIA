# core/auditoria.py
import pandas as pd
import re

pd.set_option('future.no_silent_downcasting', True)

from utils.mapping import FRENTES_PADRAO, MAPEAMENTO_FRENTE, MAPA_ALIADOS_CIDADES
from core.sap_reader import SAPReader

class AuditoriaAMED:
    def __init__(self, config: dict) -> None:
        self.config = config
        self.cols_saida = config['layout_saida']['colunas_finais']

    def processar_auditoria(self, df_aud, df_cidades, mapa_exec_cen, mapa_exec_dep, mapa_mb52, mapa_centros_mb51=None, mapa_docs_aud=None):
        # Trabalha em cópia para não mutar o DataFrame original do chamador
        df_aud = df_aud.copy()

        if mapa_centros_mb51 is None:
            mapa_centros_mb51 = {}

        # Mapeamento de colunas em O(1) antes do loop
        col_map = {str(c).strip().upper(): c for c in df_aud.columns}
        col_aliado    = col_map.get('ALIADO')
        col_sigla     = col_map.get('SIGLA')
        col_obra      = col_map.get('OBRA')
        col_frente_id = col_map.get('FRENTE_ID')
        col_tipo_proj = col_map.get('TIPO DE PROJETO')
        col_status_id = col_map.get('STATUS ID')

        # Função de Saneamento otimizada — usa normalize_str para normalizar acento no lookup
        def _saneamento_rapido(row):
            obra = str(row[col_obra]).upper() if col_obra and pd.notna(row[col_obra]) else ""
            f_id = str(row[col_frente_id]).strip().upper() if col_frente_id and pd.notna(row[col_frente_id]) else ""
            t_proj_raw = str(row[col_tipo_proj]) if col_tipo_proj and pd.notna(row[col_tipo_proj]) else ""
            # Usa normalize_str para remover acentos antes de consultar MAPEAMENTO_FRENTE
            t_proj = re.sub(r'\s+', '', SAPReader.normalize_str(t_proj_raw))

            if 'SOBREP' in obra: return 'SOBREP'
            if f_id in FRENTES_PADRAO: return f_id
            return MAPEAMENTO_FRENTE.get(t_proj, "")

        # 1. Preparação Rápida
        df_aud['ID_STR']  = df_aud['ID'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.upper()
        df_aud['SKU_STR'] = df_aud['SKU'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.upper()
        df_aud['UF'] = df_aud['UF'].fillna('').astype(str).str.strip().str.upper()
        df_aud['FRENTE ATUALIZADA'] = df_aud.apply(_saneamento_rapido, axis=1)

        l_centro, l_lvut, l_exec, l_amed, l_valor_amed, l_saldo = [], [], [], [], [], []
        l_unitario_real, l_tipo_dep, l_centro_mb51 = [], [], []

        mapa_aliados_norm = {SAPReader.normalize_str(k): v for k, v in MAPA_ALIADOS_CIDADES.items()}

        # Pré-compila dict {col_normalizada: idx} para evitar .index() O(n) dentro do loop
        if not df_cidades.empty:
            cols_cidades_limpas_super = [SAPReader.normalize_str(c).replace(" ", "") for c in df_cidades.columns]
            cols_cidades_idx_map = {col: idx for idx, col in enumerate(cols_cidades_limpas_super)}
        else:
            cols_cidades_limpas_super = []
            cols_cidades_idx_map = {}

        # Cache de normalize_str para aliados/siglas repetidos (evita re-normalizar a cada linha)
        _norm_cache: dict[str, str] = {}
        def _norm(v):
            if v not in _norm_cache:
                _norm_cache[v] = SAPReader.normalize_str(v)
            return _norm_cache[v]

        # Lista de empresas instanciada UMA vez fora do loop
        ignorar_empresas = {
            SAPReader.normalize_str('SL CONNECT SERVICOS DE TELECOMUNICACOES EIRELI'),
            SAPReader.normalize_str('INELCOM BRASIL DE TELECOMUNICA')
        }

        # 2. Motor de Cascata (Geográfico -> Cadastral -> Histórico)
        for _, row in df_aud.iterrows():
            id_b, sku_b = row['ID_STR'], row['SKU_STR']

            val_aliado = row[col_aliado] if col_aliado and pd.notna(row[col_aliado]) else ""
            val_sigla  = row[col_sigla]  if col_sigla  and pd.notna(row[col_sigla])  else ""

            aliado = _norm(str(val_aliado))
            sigla  = _norm(str(val_sigla))
            frente = str(row.get('FRENTE ATUALIZADA', '')).upper()

            centro_final = ""

            # 🌊 NÍVEL 1: Match Geográfico (Cidades)
            if aliado not in ignorar_empresas and not df_cidades.empty and sigla and sigla in df_cidades.index:

                if 'VIVO' in aliado:
                    col_busca = 'VIVO MANUT' if frente in ['MANUTENÇÃO', 'B2B'] else 'VIVO'
                else:
                    col_busca = mapa_aliados_norm.get(aliado, aliado)

                col_busca_limpa = SAPReader.normalize_str(col_busca).replace(" ", "")
                idx_col = cols_cidades_idx_map.get(col_busca_limpa)  # O(1) via dict

                if idx_col is not None:
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

            # 🌊 NÍVEL 3: Histórico Operacional (MB51)
            hist_info = mapa_centros_mb51.get(id_b, {})
            if isinstance(hist_info, dict):
                centro_principal_hist = hist_info.get('principal', "")
                centro_todos_hist     = hist_info.get('todos', "")
            else:
                centro_principal_hist = str(hist_info)
                centro_todos_hist     = str(hist_info)

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

                # Ignora LVUT para definir "Possui Saldo"
                l_saldo.append("SIM" if (d_exec['qtd'] > 0 or d_amed['qtd'] > 0) else "NÃO")

                qtd_total = d_lvut['qtd'] + d_exec['qtd'] + d_amed['qtd']
                val_total = d_lvut['valor'] + d_exec['valor'] + d_amed['valor']
                if abs(qtd_total) > 0.01: l_unitario_real.append(round(val_total / qtd_total, 2))
                else: l_unitario_real.append(0.0)

        # Atribuição Básica
        df_aud['CENTRO']       = l_centro
        df_aud['CENTRO MB51']  = l_centro_mb51
        df_aud['QTDE LVUT'], df_aud['QTDE EXEC'] = l_lvut, l_exec
        df_aud['QTDE AMED'], df_aud['$ VALOR - AMED'] = l_amed, l_valor_amed
        df_aud['POSSUI SALDO'] = l_saldo
        df_aud['$ VALOR UNIT'] = l_unitario_real

        # 3. Financeiro
        # Guard: garante que APL x DRAFT / APL x MEDIÇÃO existem (schema as define como optional)
        for c in ['APL x DRAFT', 'APL x MEDIÇÃO']:
            if c not in df_aud.columns: df_aud[c] = 0.0
            df_aud[c] = pd.to_numeric(df_aud[c], errors='coerce').fillna(0)

        if col_aliado:
            mask_vivo = df_aud[col_aliado].astype(str).str.upper().str.contains('VIVO', na=False)
        else:
            mask_vivo = pd.Series(False, index=df_aud.index)

        df_aud['SALDO_AUDIT'] = df_aud['APL x MEDIÇÃO'].fillna(0)
        df_aud.loc[mask_vivo, 'SALDO_AUDIT'] = df_aud['APL x DRAFT'].fillna(0)
        df_aud['QTDE APLICAR']   = df_aud['SALDO_AUDIT'].round(4)
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

        # 5. Enriquecimento Documental (DOC APLICAÇÃO / DOC ESTORNO / ESTORNO 2025 / DE-PARA)
        _colunas_doc = ['DOC - APLI - AUTO', 'DOC - EST - AUTO', 'ESTORNO 2025', 'DOC ESTORNO 2025', 'POSSÍVEL DE-PARA']
        if mapa_docs_aud:
            l_doc_aplic, l_doc_est, l_est_2025, l_doc_est_2025 = [], [], [], []

            for idx, r in df_aud.iterrows():
                id_b   = r['ID_STR']
                sku_b  = r['SKU_STR']
                saldo  = r['SALDO_AUDIT']
                is_vivo = mask_vivo.loc[idx]
                info   = mapa_docs_aud.get((id_b, sku_b), {})

                # DOC APLICAÇÃO — último doc 261/Z81 para o ID+SKU quando há FALTA
                if saldo < 0:
                    chv = 'ultimo_z81' if is_vivo else 'ultimo_261'
                    doc = info.get(chv, '') or ''
                    l_doc_aplic.append(doc if doc else '-')
                else:
                    l_doc_aplic.append('-')

                # DOC ESTORNO — último doc 262/Z82 para o ID+SKU quando há ESTORNO
                if saldo > 0:
                    chv = 'ultimo_z82' if is_vivo else 'ultimo_262'
                    doc = info.get(chv, '') or ''
                    l_doc_est.append(doc if doc else '-')
                else:
                    l_doc_est.append('-')

                # ESTORNO 2025 — saldo > 0 E aplicação original foi em 2025
                if saldo > 0 and info.get('data_aplic_2025', False):
                    docs_501  = info.get('docs_501', '')
                    cents_501 = info.get('centros_501', '')
                    if docs_501:
                        cents = [c.strip() for c in cents_501.split('|')
                                 if c.strip() and c.strip().lower() != 'nan']
                        all_f = bool(cents) and all(c.upper().startswith('F') for c in cents)
                        l_est_2025.append('SIM' if all_f else 'IRREGULAR')
                        l_doc_est_2025.append(docs_501)
                    else:
                        l_est_2025.append('NÃO')
                        l_doc_est_2025.append('-')
                else:
                    l_est_2025.append('-')
                    l_doc_est_2025.append('-')

            df_aud['DOC - APLI - AUTO'] = l_doc_aplic
            df_aud['DOC - EST - AUTO']  = l_doc_est
            df_aud['ESTORNO 2025']     = l_est_2025
            df_aud['DOC ESTORNO 2025'] = l_doc_est_2025

            # POSSÍVEL DE-PARA — cross-row: FALTA sem aplicação, mesmo ID tem outro SKU aplicado com qtd equivalente
            mb51_por_id: dict[str, dict[str, float]] = {}
            for (id_k, sku_k), inf in mapa_docs_aud.items():
                qtd_a = inf.get('qtd_261', 0) + inf.get('qtd_z81', 0)
                if qtd_a > 0:
                    mb51_por_id.setdefault(id_k, {})[sku_k] = round(qtd_a, 4)

            # Usa operação vetorizada (array numpy) em vez de iterrows para montar set de faltas
            mask_falta = df_aud['SALDO_AUDIT'] < 0
            faltas_aud = set(map(tuple, df_aud.loc[mask_falta, ['ID_STR', 'SKU_STR']].values))

            l_de_para = []
            for _, r in df_aud.iterrows():
                id_b  = r['ID_STR']
                sku_b = r['SKU_STR']
                saldo = r['SALDO_AUDIT']
                if saldo < 0:
                    qtd_n  = round(abs(saldo), 4)
                    achado = next(
                        (sku for sku, qtd in mb51_por_id.get(id_b, {}).items()
                         if sku != sku_b
                         and abs(qtd - qtd_n) < 0.01
                         and (id_b, sku) not in faltas_aud),
                        None)
                    l_de_para.append(f'Possível De Para: {achado}' if achado else '-')
                else:
                    l_de_para.append('-')

            df_aud['POSSÍVEL DE-PARA'] = l_de_para
        else:
            for col in _colunas_doc:
                df_aud[col] = 'N/A'

        # 🛡️ DEGRADAÇÃO ELEGANTE (OPÇÃO B APLICADA)
        mask_verif = df_aud['CENTRO'] == "N/D"

        df_aud.loc[mask_verif, 'CENTRO']  = "Verif.Centro"
        df_aud.loc[mask_verif, 'STATUS']  = "Verif.Centro"
        df_aud.loc[mask_verif, 'AÇÃO']    = "Verif.Centro"
        df_aud.loc[mask_verif, 'SUGESTÃO'] = "Mapear Cidade/ID"

        df_aud['TIPO DE DEPÓSITO'] = l_tipo_dep

        for col in self.cols_saida:
            if col not in df_aud.columns and col != "VALIDAÇÃO CONTÁBIL":
                df_aud[col] = "N/A"

        return df_aud.drop(columns=['ID_STR', 'SKU_STR', 'SALDO_AUDIT'], errors='ignore')
