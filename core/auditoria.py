# core/auditoria.py
import pandas as pd
import re
from utils.mapping import FRENTES_PADRAO, MAPEAMENTO_FRENTE

class AuditoriaAMED:
    def __init__(self, config):
        self.config = config
        self.cols_saida = config['layout_saida']['colunas_finais']

    def _saneamento(self, row):
        obra = str(row.get('OBRA', '')).upper()
        f_id = str(row.get('FRENTE_ID', '')).strip().upper()
        t_proj = re.sub(r'\s+', '', str(row.get('Tipo de projeto', ''))).upper()
        if 'SOBREP' in obra: return 'SOBREP'
        if f_id in FRENTES_PADRAO: return f_id
        return MAPEAMENTO_FRENTE.get(t_proj, "")

    # REMOVI O ARGUMENTO 'mapa_mb51_baixas'
    def processar_auditoria(self, df_aud, mapa_centros, mapa_mb52, mapa_centros_mb51={}):
        pd.set_option('future.no_silent_downcasting', True)
        
        # 1. Preparação
        df_aud['ID_STR'] = df_aud['ID'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.upper()
        df_aud['SKU_STR'] = df_aud['SKU'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.upper()
        df_aud['UF'] = df_aud['UF'].fillna('').astype(str).str.strip().str.upper()
        df_aud['FRENTE ATUALIZADA'] = df_aud.apply(self._saneamento, axis=1)

        l_centro, l_lvut, l_exec, l_amed, l_valor, l_saldo = [], [], [], [], [], []

        # 2. Dados Físicos (MB52)
        for _, row in df_aud.iterrows():
            id_b, sku_b = row['ID_STR'], row['SKU_STR']
            centro = mapa_centros.get(id_b, "N/D")
            l_centro.append(centro)

            if centro == "N/D":
                l_lvut.append(0.0); l_exec.append(0.0); l_amed.append(0.0)
                l_valor.append(0.0); l_saldo.append("NÃO")
            else:
                d_lvut = mapa_mb52.get((sku_b, centro, 'LVUT'), {'qtd': 0.0, 'valor': 0.0})
                d_exec = mapa_mb52.get((sku_b, centro, 'EXEC'), {'qtd': 0.0, 'valor': 0.0})
                d_amed = mapa_mb52.get((sku_b, centro, 'AMED'), {'qtd': 0.0, 'valor': 0.0})

                l_lvut.append(d_lvut['qtd'])
                l_exec.append(d_exec['qtd'])
                l_amed.append(d_amed['qtd'])
                l_valor.append(d_amed['valor'])
                l_saldo.append("SIM" if (d_lvut['qtd']>0 or d_exec['qtd']>0 or d_amed['qtd']>0) else "NÃO")

        df_aud['CENTRO'] = l_centro
        df_aud['QTDE LVUT'], df_aud['QTDE EXEC'] = l_lvut, l_exec
        df_aud['QTDE AMED'], df_aud['$ VALOR - AMED'] = l_amed, l_valor
        df_aud['POSSUI SALDO'] = l_saldo

        # 3. Financeiro
        for c in ['APL x DRAFT', 'APL x MEDIÇÃO']: df_aud[c] = pd.to_numeric(df_aud[c], errors='coerce').fillna(0)
        
        mask_vivo = df_aud['Aliado'].astype(str).str.upper().str.contains('VIVO', na=False)
        df_aud['SALDO_AUDIT'] = df_aud['APL x MEDIÇÃO'].fillna(0)
        df_aud.loc[mask_vivo, 'SALDO_AUDIT'] = df_aud['APL x DRAFT'].fillna(0)
        df_aud['QTDE APLICAR'] = df_aud['SALDO_AUDIT']

        def calc_unit(r):
            q, v = float(r['QTDE AMED']), float(r['$ VALOR - AMED'])
            return round(v/q, 2) if q > 0 else 0.0
        
        df_aud['$ VALOR UNIT'] = df_aud.apply(calc_unit, axis=1)
        df_aud['$ SALDO X QTDE'] = (df_aud['QTDE APLICAR'] * df_aud['$ VALOR UNIT'] * -1).round(2)

        # 4. Auditoria Lógica (Sem Validação Contábil)
        livro_faltas = {}
        mask_ativos = df_aud['Status ID'].astype(str).str.upper() != "CANCELADO"
        for _, r in df_aud[(df_aud['SALDO_AUDIT'] < 0) & mask_ativos & (df_aud['UF'] != "")].iterrows():
            ch = (r['SKU_STR'], str(r['Aliado']).upper(), r['UF'])
            if ch not in livro_faltas: livro_faltas[ch] = []
            livro_faltas[ch].append({'ID': str(r['ID']), 'FALTA': abs(r['SALDO_AUDIT'])})

        l_st, l_ac, l_sg = [], [], [] 
        # REMOVIDO: l_val_sap = []
        
        for _, r in df_aud.iterrows():
            if str(r.get('Status ID', '')).upper() == "CANCELADO":
                l_st.append("Cancelado"); l_ac.append("Cancelado"); l_sg.append("Cancelado"); continue

            uf, saldo = r['UF'], r['SALDO_AUDIT']
            if saldo > 0 and (uf == "" or uf == "NAN"):
                l_st.append("UF ERRO"); l_ac.append("REVISAR"); l_sg.append("UF vazia"); continue

            if round(saldo, 4) == 0:
                l_st.append("OK"); l_ac.append("APLICADO"); l_sg.append("Ciclo OK")
            elif saldo < 0:
                l_st.append("FALTA"); l_ac.append("APLICAR"); l_sg.append("Zerar AMED")
            else:
                sobra, sugs = saldo, []
                ch = (r['SKU_STR'], str(r['Aliado']).upper(), uf)
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
        # REMOVIDO: df_aud['VALIDAÇÃO CONTÁBIL']

        # --- NOVA COLUNA: CENTRO MB51 ---
        if mapa_centros_mb51:
            df_aud['CENTRO MB51'] = df_aud['ID_STR'].map(mapa_centros_mb51).fillna('-')
        else:
            df_aud['CENTRO MB51'] = '-'

        for col in self.cols_saida:
            if col not in df_aud.columns and col != "VALIDAÇÃO CONTÁBIL": # Ignora se ainda tiver no config
                df_aud[col] = "N/A"

        return df_aud.drop(columns=['ID_STR', 'SKU_STR', 'SALDO_AUDIT'], errors='ignore')