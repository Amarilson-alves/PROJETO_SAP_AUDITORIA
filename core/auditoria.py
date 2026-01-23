# core/auditoria.py
import pandas as pd
import re
from utils.mapping import FRENTES_PADRAO, MAPEAMENTO_FRENTE, COLUNAS_SAIDA_AUDITORIA

class AuditoriaAMED:
    def _saneamento(self, row):
        obra = str(row.get('OBRA', '')).upper()
        f_id = str(row.get('FRENTE_ID', '')).strip().upper()
        t_proj = re.sub(r'\s+', '', str(row.get('Tipo de projeto', ''))).upper()
        if 'SOBREP' in obra: return 'SOBREP'
        if f_id in FRENTES_PADRAO: return f_id
        return MAPEAMENTO_FRENTE.get(t_proj, "")

    def processar_auditoria(self, df_aud, mapa_centros, mapa_mb52):
        pd.set_option('future.no_silent_downcasting', True)
        
        # 1. Normalização Rígida das Chaves (Para o VLOOKUP funcionar)
        # Remove espaços, .0 e joga pra maiúsculo
        df_aud['ID_STR'] = df_aud['ID'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.upper()
        df_aud['SKU_STR'] = df_aud['SKU'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True).str.upper()
        df_aud['UF'] = df_aud['UF'].fillna('').astype(str).str.strip().str.upper()
        df_aud['FRENTE ATUALIZADA'] = df_aud.apply(self._saneamento, axis=1)

        l_centro, l_lvut, l_exec, l_amed, l_valor, l_saldo = [], [], [], [], [], []

        # 2. Busca de Dados (Match)
        for _, row in df_aud.iterrows():
            id_busca = row['ID_STR']
            sku_busca = row['SKU_STR']
            
            # Busca Centro (VLOOKUP)
            centro = mapa_centros.get(id_busca, "N/D")
            l_centro.append(centro)

            # Busca Estoque
            if centro == "N/D":
                l_lvut.append(0.0); l_exec.append(0.0); l_amed.append(0.0)
                l_valor.append(0.0); l_saldo.append("NÃO")
            else:
                # A chave deve bater exatamente com a gerada no sap_reader (SKU, CENTRO, DEPOSITO)
                d_lvut = mapa_mb52.get((sku_busca, centro, 'LVUT'), {'qtd': 0.0, 'valor': 0.0})
                d_exec = mapa_mb52.get((sku_busca, centro, 'EXEC'), {'qtd': 0.0, 'valor': 0.0})
                d_amed = mapa_mb52.get((sku_busca, centro, 'AMED'), {'qtd': 0.0, 'valor': 0.0})

                l_lvut.append(d_lvut['qtd'])
                l_exec.append(d_exec['qtd'])
                l_amed.append(d_amed['qtd'])
                l_valor.append(d_amed['valor']) # Valor exclusivo AMED
                
                tem = (d_lvut['qtd'] > 0 or d_exec['qtd'] > 0 or d_amed['qtd'] > 0)
                l_saldo.append("SIM" if tem else "NÃO")

        # Inserção no DF
        df_aud['CENTRO'] = l_centro
        df_aud['QTDE LVUT'] = l_lvut
        df_aud['QTDE EXEC'] = l_exec
        df_aud['QTDE AMED'] = l_amed
        df_aud['$ VALOR - AMED'] = l_valor
        df_aud['POSSUI SALDO'] = l_saldo

        # 3. Lógica do Livro Razão
        mask_vivo = df_aud['Aliado'].astype(str).str.upper().str.contains('VIVO', na=False)
        df_aud['SALDO_AUDIT'] = df_aud['APL x MEDIÇÃO'].fillna(0)
        df_aud.loc[mask_vivo, 'SALDO_AUDIT'] = df_aud['APL x DRAFT'].fillna(0)
        df_aud['SALDO_AUDIT'] = pd.to_numeric(df_aud['SALDO_AUDIT'], errors='coerce').fillna(0)

        # Mapa de Faltas
        livro_faltas = {}
        mask_ativos = df_aud['Status ID'].astype(str).str.upper() != "CANCELADO"
        # Filtra apenas registros válidos para gerar o livro de faltas
        df_faltas = df_aud[(df_aud['SALDO_AUDIT'] < 0) & mask_ativos & (df_aud['UF'] != "")]
        
        for _, r in df_faltas.iterrows():
            ch = (r['SKU_STR'], str(r['Aliado']).upper(), r['UF'])
            if ch not in livro_faltas: livro_faltas[ch] = []
            livro_faltas[ch].append({'ID': str(r['ID']), 'FALTA': abs(r['SALDO_AUDIT'])})

        # Processamento linha a linha
        l_st, l_ac, l_sg = [], [], []
        for _, r in df_aud.iterrows():
            if str(r.get('Status ID', '')).upper() == "CANCELADO":
                l_st.append("ID CANCELADO"); l_ac.append("N/A"); l_sg.append("ID Cancelado"); continue

            uf = r['UF']
            saldo = r['SALDO_AUDIT']
            
            if saldo > 0 and (uf == "" or uf == "NAN"):
                l_st.append("UF VAZIA"); l_ac.append("REVISAR"); l_sg.append("Necessário UF"); continue

            if round(saldo, 4) == 0:
                l_st.append("OK"); l_ac.append("APLICADO"); l_sg.append("Ciclo Concluído")
            elif saldo < 0:
                l_st.append("PENDENTE"); l_ac.append("APLICAR"); l_sg.append("Aguardando Aplicação")
            else:
                sobra = saldo
                sugs = []
                chave_busca = (r['SKU_STR'], str(r['Aliado']).upper(), uf)
                meu_id = str(r['ID'])

                if uf != "" and chave_busca in livro_faltas:
                    for item in livro_faltas[chave_busca]:
                        if round(sobra, 4) <= 0: break
                        if item['ID'] == meu_id: continue
                        if round(item['FALTA'], 4) <= 0: continue

                        abat = min(sobra, item['FALTA'])
                        sugs.append(f"APLICAR {round(abat, 2)} EM {item['ID']}")
                        item['FALTA'] -= abat
                        sobra -= abat
                
                if sugs:
                    l_st.append("PENDENTE"); l_ac.append("AJUSTAR"); l_sg.append(" | ".join(sugs))
                else:
                    l_st.append("DIVERGÊNCIA"); l_ac.append("ESTORNAR"); l_sg.append("Sobra sem destino")

        df_aud['STATUS_AUD'], df_aud['AÇÃO_AUD'], df_aud['SUGESTÃO_AUD'] = l_st, l_ac, l_sg
        df_aud['RESULTADO_OPERACIONAL'] = df_aud['STATUS_AUD']

        for col in COLUNAS_SAIDA_AUDITORIA:
            if col not in df_aud.columns: df_aud[col] = "N/A"

        return df_aud.drop(columns=['ID_STR', 'SKU_STR', 'SALDO_AUDIT'], errors='ignore')