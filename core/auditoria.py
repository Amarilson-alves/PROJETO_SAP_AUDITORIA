import pandas as pd

class AuditoriaAMED:
    def processar_aba2(self, df_aud):
        # Blindagem numérica
        df_aud['APL x DRAFT'] = pd.to_numeric(df_aud['APL x DRAFT'], errors='coerce').fillna(0)
        df_aud['APL x MEDIÇÃO'] = pd.to_numeric(df_aud['APL x MEDIÇÃO'], errors='coerce').fillna(0)

        # Cálculo de Saldo
        df_aud['SALDO_AUDIT'] = df_aud.apply(
            lambda r: r['APL x DRAFT'] if str(r.get('Aliado', '')).strip().upper() == "VIVO INSOURCING" else r['APL x MEDIÇÃO'], 
            axis=1
        ).astype(float).fillna(0)

        # Mapeamento de Faltas (Livro Razão)
        livro_faltas = {}
        for _, r in df_aud[df_aud['SALDO_AUDIT'] < 0].iterrows():
            ch = (str(r['SKU']), str(r['Aliado']).strip().upper())
            if ch not in livro_faltas: livro_faltas[ch] = []
            livro_faltas[ch].append({'ID': str(r['ID']), 'FALTA': abs(r['SALDO_AUDIT'])})

        l_st, l_ac, l_sg = [], [], []
        for _, r in df_aud.iterrows():
            s, sku, ali, id_a = r['SALDO_AUDIT'], str(r['SKU']), str(r['Aliado']).strip().upper(), str(r['ID'])
            ch = (sku, ali)

            if round(s, 4) == 0:
                l_st.append("APLICAÇÃO OK"); l_ac.append("APLICADO"); l_sg.append("CICLO CONCLUÍDO")
            elif s < 0:
                l_st.append("FALTA APLICAR"); l_ac.append("APLICAR"); l_sg.append("OPORTUNIDADE DE ZERAR AMED")
            else:
                sob, sugs = s, []
                if ch in livro_faltas:
                    for d in livro_faltas[ch]:
                        if round(sob, 4) <= 0: break
                        if d['ID'] == id_a or round(d['FALTA'], 4) <= 0: continue
                        at = min(sob, d['FALTA'])
                        sugs.append(f"APLICAR {round(at, 4)} NO ID {d['ID']}")
                        d['FALTA'] -= at
                        sob -= at
                
                if sugs: 
                    l_st.append("APLICAÇÃO EXTERNA"); l_ac.append("CORREÇÃO"); l_sg.append(" | ".join(sugs))
                else: 
                    l_st.append("ESTORNO"); l_ac.append("JUSTIFICATIVA"); l_sg.append("DEVOLVER MATERIAL")

        df_aud['STATUS'], df_aud['AÇÃO'], df_aud['SUGESTÃO'] = l_st, l_ac, l_sg
        return df_aud.drop(columns=['SALDO_AUDIT'])