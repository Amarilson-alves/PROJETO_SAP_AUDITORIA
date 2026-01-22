import pandas as pd
import re
from utils.mapping import FRENTES_PADRAO, MAPEAMENTO_FRENTE

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
        # 1. Saneamento e Normalização
        df_aud['FRENTE ATUALIZADA'] = df_aud.apply(self.saneamento_frente, axis=1)
        
        for c in ['APL x DRAFT', 'APL x MEDIÇÃO']:
            if c in df_aud.columns:
                df_aud[c] = pd.to_numeric(df_aud[c], errors='coerce').fillna(0)
        
        df_aud['UF'] = df_aud['UF'].fillna('').astype(str).str.strip().str.upper() if 'UF' in df_aud.columns else ''
        
        # 2. Cálculo de Saldo
        df_aud['SALDO_AUDIT'] = df_aud.apply(
            lambda r: r['APL x DRAFT'] if str(r.get('Aliado', '')).strip().upper() == "VIVO INSOURCING" else r['APL x MEDIÇÃO'], 
            axis=1
        ).astype(float).fillna(0)

        # 3. Livro Razão e Diagnóstico
        l_st, l_ac, l_sg, l_res = [], [], [], []
        
        # Mapa de faltas para sugestões
        livro_faltas = {}
        mask_ativos = df_aud['Status ID'].astype(str).str.strip().str.upper() != "CANCELADO" if 'Status ID' in df_aud.columns else True
        for _, r in df_aud[(df_aud['SALDO_AUDIT'] < 0) & mask_ativos & (df_aud['UF'] != "")].iterrows():
            ch = (str(r.get('SKU', '')), str(r.get('Aliado', '')).strip().upper(), str(r['UF']))
            if ch not in livro_faltas: livro_faltas[ch] = []
            livro_faltas[ch].append({'ID': str(r.get('ID', '')), 'FALTA': abs(r['SALDO_AUDIT'])})

        for _, r in df_aud.iterrows():
            if 'Status ID' in df_aud.columns and str(r['Status ID']).strip().upper() == "CANCELADO":
                l_st.append("CANCELADO"); l_ac.append("CANCELADO"); l_sg.append("ID Cancelado"); l_res.append("ID CANCELADO"); continue

            uf, s = str(r['UF']).strip(), r['SALDO_AUDIT']
            sku, ali, id_a = str(r.get('SKU', '')), str(r.get('Aliado', '')).strip().upper(), str(r.get('ID', ''))
            ch = (sku, ali, uf)

            if s > 0 and uf == "":
                l_st.append("UF NÃO INFORMADA"); l_ac.append("SEM SUGESTÃO"); l_sg.append("UF vazia"); l_res.append("VERIFICAR"); continue

            if round(s, 4) == 0:
                l_st.append("APLICAÇÃO OK"); l_ac.append("APLICADO"); l_sg.append("CICLO CONCLUÍDO"); l_res.append("OK - CICLO CONCLUÍDO")
            elif s < 0:
                l_st.append("FALTA APLICAR"); l_ac.append("APLICAR"); l_sg.append("OPORTUNIDADE"); l_res.append("PENDENTE - NECESSITA APLICAÇÃO")
            else:
                sob, sugs = s, []
                if uf != "" and ch in livro_faltas:
                    for d in livro_faltas[ch]:
                        if round(sob, 4) <= 0: break
                        if d['ID'] == id_a or round(d['FALTA'], 4) <= 0: continue
                        at = min(sob, d['FALTA'])
                        sugs.append(f"APLICAR {round(at, 4)} NO ID {d['ID']}")
                        d['FALTA'] -= at; sob -= at
                
                l_st.append("APLICAÇÃO EXTERNA" if sugs else "ESTORNO")
                l_ac.append("CORREÇÃO" if sugs else "JUSTIFICATIVA")
                l_sg.append(" | ".join(sugs) if sugs else "DEVOLVER MATERIAL")
                l_res.append("PENDENTE - CORREÇÃO ENTRE IDS" if sugs else "DIVERGÊNCIA - SOLICITAR ESTORNO")

        df_aud['STATUS_AUD'], df_aud['AÇÃO_AUD'], df_aud['SUGESTÃO_AUD'], df_aud['RESULTADO_OPERACIONAL'] = l_st, l_ac, l_sg, l_res
        
        # --- GARANTIA DE CONTRATO (Solução 3) ---
        COLUNAS_OBRIGATORIAS = [
            'STATUS_AUD', 'AÇÃO_AUD', 'SUGESTÃO_AUD', 'RESULTADO_OPERACIONAL'
        ]

        for col in COLUNAS_OBRIGATORIAS:
            if col not in df_aud.columns:
                df_aud[col] = 'N/A'
        
        colunas_finais = [c for c in df_aud.columns if c not in ['Tipo de projeto', 'SALDO_AUDIT']]
        return df_aud[colunas_finais]