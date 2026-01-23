# core/auditoria.py
import pandas as pd
import re
from utils.mapping import FRENTES_PADRAO, MAPEAMENTO_FRENTE, COLUNAS_SAIDA_AUDITORIA

class AuditoriaAMED:
    def _saneamento(self, row):
        obra = str(row.get('OBRA', '')).upper()
        f_id = str(row.get('FRENTE_ID', '')).strip().upper()
        tipo_proj = re.sub(r'\s+', '', str(row.get('Tipo de projeto', ''))).upper()
        if 'SOBREP' in obra: return 'SOBREP'
        if f_id in FRENTES_PADRAO: return f_id
        return MAPEAMENTO_FRENTE.get(tipo_proj, "")

    def processar_auditoria(self, df_aud, mapa_centros, mapa_mb52):
        pd.set_option('future.no_silent_downcasting', True)
        
        # 1. Preparação de Chaves e Saneamento
        df_aud['ID_STR'] = df_aud['ID'].apply(lambda x: str(x).strip().replace('.0', ''))
        df_aud['SKU_STR'] = df_aud['SKU'].apply(lambda x: str(x).strip().replace('.0', ''))
        df_aud['FRENTE ATUALIZADA'] = df_aud.apply(self._saneamento, axis=1)
        df_aud['UF'] = df_aud['UF'].fillna('').astype(str).str.strip().str.upper()

        l_centro, l_lvut, l_exec, l_amed, l_valor, l_saldo = [], [], [], [], [], []

        # 2. Primeira Passada: Cruzamento com MB52 (Saldos Físicos)
        for _, row in df_aud.iterrows():
            sku, id_obra = row['SKU_STR'], row['ID_STR']
            centro = mapa_centros.get(id_obra, "N/D")
            l_centro.append(centro)

            if centro == "N/D":
                for lista in [l_lvut, l_exec, l_amed, l_valor]: lista.append(0.0)
                l_saldo.append("NÃO")
            else:
                d_lvut = mapa_mb52.get((sku, centro, 'LVUT'), {'qtd': 0.0, 'valor': 0.0})
                d_exec = mapa_mb52.get((sku, centro, 'EXEC'), {'qtd': 0.0, 'valor': 0.0})
                d_amed = mapa_mb52.get((sku, centro, 'AMED'), {'qtd': 0.0, 'valor': 0.0})

                l_lvut.append(d_lvut['qtd'])
                l_exec.append(d_exec['qtd'])
                l_amed.append(d_amed['qtd'])
                l_valor.append(d_amed['valor'])
                
                tem = (d_lvut['qtd'] > 0 or d_exec['qtd'] > 0 or d_amed['qtd'] > 0)
                l_saldo.append("SIM" if tem else "NÃO")

        df_aud['CENTRO'], df_aud['QTDE LVUT'], df_aud['QTDE EXEC'] = l_centro, l_lvut, l_exec
        df_aud['QTDE AMED'], df_aud['$ VALOR - AMED'], df_aud['POSSUI SALDO'] = l_amed, l_valor, l_saldo

        # 3. Lógica de Verificação (Livro Razão de Aplicação)
        # Cálculo do Saldo Teórico (Diferença entre o que devia aplicar e o que aplicou)
        mask_vivo = df_aud['Aliado'].astype(str).str.upper().str.contains('VIVO', na=False)
        df_aud['SALDO_AUDIT'] = df_aud['APL x MEDIÇÃO'].fillna(0).infer_objects(copy=False)
        df_aud.loc[mask_vivo, 'SALDO_AUDIT'] = df_aud['APL x DRAFT'].fillna(0).infer_objects(copy=False)

        # Mapeamento de Faltas (Quem precisa de material)
        livro_faltas = {}
        mask_ativos = df_aud['Status ID'].astype(str).str.upper() != "CANCELADO"
        for _, r in df_aud[(df_aud['SALDO_AUDIT'] < 0) & mask_ativos & (df_aud['UF'] != "")].iterrows():
            ch = (str(r['SKU_STR']), str(r['Aliado']).upper(), str(r['UF']))
            if ch not in livro_faltas: livro_faltas[ch] = []
            livro_faltas[ch].append({'ID': str(r['ID']), 'FALTA': abs(r['SALDO_AUDIT'])})

        # 4. Verificação Linha a Linha (Saber se foi aplicado)
        l_st, l_ac, l_sg = [], [], []
        for _, r in df_aud.iterrows():
            if str(r.get('Status ID', '')).upper() == "CANCELADO":
                l_st.append("ID CANCELADO"); l_ac.append("N/A"); l_sg.append("Sem ação: ID Cancelado"); continue

            uf, s = r['UF'], r['SALDO_AUDIT']
            sku, ali, id_a = r['SKU_STR'], str(r['Aliado']).upper(), str(r['ID'])
            ch = (sku, ali, uf)

            if s > 0 and (uf == "" or uf == "NAN"):
                l_st.append("UF VAZIA"); l_ac.append("REVISAR"); l_sg.append("Necessário informar UF para auditar"); continue

            if round(s, 4) == 0:
                l_st.append("OK - CICLO CONCLUÍDO"); l_ac.append("APLICADO"); l_sg.append("Material aplicado corretamente")
            elif s < 0:
                l_st.append("PENDENTE - NECESSITA APLICAÇÃO"); l_ac.append("APLICAR"); l_sg.append("SKU aguardando aplicação no campo")
            else:
                # Sobra de aplicação: Buscar se outro ID da mesma UF/SKU está precisando
                sob, sugs = s, []
                if uf != "" and ch in livro_faltas:
                    for d in livro_faltas[ch]:
                        if round(sob, 4) <= 0: break
                        if d['ID'] == id_a or round(d['FALTA'], 4) <= 0: continue
                        at = min(sob, d['FALTA'])
                        sugs.append(f"APLICAR {round(at, 2)} NO ID {d['ID']}")
                        d['FALTA'] -= at; sob -= at
                
                if sugs:
                    l_st.append("PENDENTE - CORREÇÃO ENTRE IDS")
                    l_ac.append("AJUSTAR"); l_sg.append(" | ".join(sugs))
                else:
                    l_st.append("DIVERGÊNCIA - SOLICITAR ESTORNO")
                    l_ac.append("ESTORNAR"); l_sg.append("Material sobrando sem destino na UF")

        df_aud['STATUS_AUD'], df_aud['AÇÃO_AUD'], df_aud['SUGESTÃO_AUD'] = l_st, l_ac, l_sg
        df_aud['RESULTADO_OPERACIONAL'] = df_aud['STATUS_AUD']

        # CORREÇÃO APLICADA: Garantir que as colunas de quantidade e valor sejam numéricas
        df_aud['$ VALOR - AMED'] = pd.to_numeric(df_aud['$ VALOR - AMED'], errors='coerce').fillna(0.0)
        df_aud['QTDE AMED'] = pd.to_numeric(df_aud['QTDE AMED'], errors='coerce').fillna(0.0)
        
        # Garantia final de contrato
        for col in COLUNAS_SAIDA_AUDITORIA:
            if col not in df_aud.columns: df_aud[col] = "N/A"

        return df_aud.drop(columns=['ID_STR', 'SKU_STR', 'SALDO_AUDIT'], errors='ignore')