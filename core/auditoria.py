# core/auditoria.py
import pandas as pd
import re
from utils.mapping import FRENTES_PADRAO, MAPEAMENTO_FRENTE, COLUNAS_SAIDA_AUDITORIA

class AuditoriaAMED:
    def saneamento_frente(self, row):
        obra = str(row.get('OBRA', '')).upper()
        frente_id = str(row.get('FRENTE_ID', '')).strip().upper()
        tipo_proj_raw = str(row.get('Tipo de projeto', ''))
        tipo_proj_clean = re.sub(r'\s+', '', tipo_proj_raw).upper()

        if 'SOBREP' in obra: return 'SOBREP'
        if frente_id in FRENTES_PADRAO: return frente_id
        if frente_id in ['', 'NAN', 'VERIFICAR_FRENTE_ID', 'NONE']:
            return MAPEAMENTO_FRENTE.get(tipo_proj_clean, "")
        return ""

    def processar_aba2(self, df_aud):
        # 1. Saneamento e Normalização de Números
        df_aud['FRENTE ATUALIZADA'] = df_aud.apply(self.saneamento_frente, axis=1)
        
        for c in ['APL x DRAFT', 'APL x MEDIÇÃO']:
            if c in df_aud.columns:
                df_aud[c] = pd.to_numeric(df_aud[c], errors='coerce').fillna(0)
        
        df_aud['UF'] = df_aud['UF'].fillna('').astype(str).str.strip().str.upper()

        # 2. Performance: Cálculo de SALDO_AUDIT (Vetorizado)
        mask_vivo = df_aud['Aliado'].astype(str).str.strip().str.upper() == "VIVO INSOURCING"
        df_aud['SALDO_AUDIT'] = 0.0
        df_aud.loc[mask_vivo, 'SALDO_AUDIT'] = df_aud['APL x DRAFT']
        df_aud.loc[~mask_vivo, 'SALDO_AUDIT'] = df_aud['APL x MEDIÇÃO']

        # 3. Lógica de Auditoria (Livro Razão)
        l_st, l_ac, l_sg, l_res = [], [], [], []
        livro_faltas = {}
        mask_ativos = df_aud['Status ID'].astype(str).str.strip().str.upper() != "CANCELADO" if 'Status ID' in df_aud.columns else True
        
        # Preencher livro de faltas
        for _, r in df_aud[(df_aud['SALDO_AUDIT'] < 0) & mask_ativos & (df_aud['UF'] != "")].iterrows():
            ch = (str(r.get('SKU', '')), str(r.get('Aliado', '')).strip().upper(), str(r['UF']))
            if ch not in livro_faltas: livro_faltas[ch] = []
            livro_faltas[ch].append({'ID': str(r.get('ID', '')), 'FALTA': abs(r['SALDO_AUDIT'])})

        for _, r in df_aud.iterrows():
            if 'Status ID' in df_aud.columns and str(r['Status ID']).strip().upper() == "CANCELADO":
                l_st.append("CANCELADO"); l_ac.append("N/A"); l_sg.append("ID Cancelado"); l_res.append("ID CANCELADO"); continue

            uf, s = str(r['UF']).strip(), r['SALDO_AUDIT']
            sku, ali, id_a = str(r.get('SKU', '')), str(r.get('Aliado', '')).strip().upper(), str(r.get('ID', ''))
            ch = (sku, ali, uf)

            if s > 0 and uf == "":
                l_st.append("UF VAZIA"); l_ac.append("REVISAR"); l_sg.append("Informar UF"); l_res.append("VERIFICAR"); continue

            if round(s, 4) == 0:
                l_st.append("OK"); l_ac.append("APLICADO"); l_sg.append("CICLO OK"); l_res.append("OK - CICLO CONCLUÍDO")
            elif s < 0:
                l_st.append("FALTA"); l_ac.append("APLICAR"); l_sg.append("Oportunidade"); l_res.append("PENDENTE - NECESSITA APLICAÇÃO")
            else:
                sob, sugs = s, []
                if uf != "" and ch in livro_faltas:
                    for d in livro_faltas[ch]:
                        if round(sob, 4) <= 0: break
                        if d['ID'] == id_a or round(d['FALTA'], 4) <= 0: continue
                        at = min(sob, d['FALTA']); sugs.append(f"ID {d['ID']} ({round(at,2)})")
                        d['FALTA'] -= at; sob -= at
                
                l_st.append("SOBRA"); l_ac.append("ESTORNO" if not sugs else "AJUSTE")
                l_sg.append("DEVOLVER" if not sugs else " | ".join(sugs))
                l_res.append("DIVERGÊNCIA" if not sugs else "PENDENTE - CORREÇÃO")

        # 4. Garantia de Contrato
        df_aud['STATUS_AUD'], df_aud['AÇÃO_AUD'], df_aud['SUGESTÃO_AUD'], df_aud['RESULTADO_OPERACIONAL'] = l_st, l_ac, l_sg, l_res
        
        for col in COLUNAS_SAIDA_AUDITORIA:
            if col not in df_aud.columns: df_aud[col] = "N/A"

        return df_aud.drop(columns=['SALDO_AUDIT', 'Tipo de projeto'], errors='ignore')