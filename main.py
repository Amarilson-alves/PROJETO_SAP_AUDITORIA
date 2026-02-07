# main.py
import os
import sys
import time
import pandas as pd
import pandera.pandas as pa
from core.schemas import SchemaAldrei
from utils.settings import carregar_config
from core.sap_reader import SAPReader
from core.auditoria import AuditoriaAMED
from utils.logger import configurar_logger
from utils.formatting import ExcelFormatter

def processar_tudo():
    log = configurar_logger()
    for p in ['data', 'output', 'logs']:
        if not os.path.exists(p): os.makedirs(p)
        
    log.info("🚀 INICIANDO AUDITORIA AMED v13.1 (FIX VISUAL)")

    try:
        config = carregar_config()
        reader = SAPReader(config)
        audit = AuditoriaAMED(config)

        # 1. Carregar Dados
        log.info("📥 Carregando Bases de Dados...")
        try:
            mapa_mb52, df_evidencias = reader.carregar_mapa_mb52()
            mapa_centros = reader.carregar_mapa_centros()
            df_ald = reader.carregar_aldrei()
        except PermissionError:
            log.error("❌ ERRO DE PERMISSÃO: Feche os arquivos de entrada antes de rodar!")
            return

        # Validação
        log.info("🛡️ Validando Schema...")
        try: SchemaAldrei.validate(df_ald)
        except pa.errors.SchemaError as err:
            log.error(f"❌ ERRO VALIDAÇÃO: {err}"); return

        # 2. Processo 1: Auditoria Cruzada
        log.info("⚙️ Processando Auditoria Cruzada...")
        resultado = audit.processar_auditoria(df_ald, mapa_centros, mapa_mb52, {})

        # 3. Processo 2: AUDITORIA CONTÍNUA
        log.info("☢️ Executando Motor de Auditoria Contínua...")
        df_raiox = reader.gerar_raio_x_amed(mapa_mb52) 

        # 4. Exportação
        saida_res = config['saidas']['dashboard']
        log.info(f"📊 Gerando Relatório Final: {saida_res}")
        
        sucesso = False
        tentativas = 0
        while not sucesso and tentativas < 1:
            try:
                with pd.ExcelWriter(saida_res, engine='xlsxwriter') as writer:
                    
                    # Aba 1: Auditoria Padrão
                    resultado.to_excel(writer, sheet_name='analise auditoria', index=False)
                    try: ExcelFormatter.aplicar_formato(writer, resultado)
                    except: pass
                    
                    # Aba 2: RAIO-X AMED
                    if not df_raiox.empty:
                        # Ordena por Score
                        df_raiox.sort_values(by=['SCORE_RISCO', 'VALOR_REAL'], ascending=[False, False], inplace=True)
                        
                        sheet_raiox = 'RAIO_X_AMED'
                        df_raiox.to_excel(writer, sheet_name=sheet_raiox, index=False)
                        
                        # --- FORMATAÇÃO VISUAL (MAPA CORRIGIDO) ---
                        wb = writer.book
                        ws = writer.sheets[sheet_raiox]
                        
                        fmt_money = wb.add_format({'num_format': 'R$ #,##0.00'})
                        fmt_num   = wb.add_format({'num_format': '0'})
                        fmt_wrap  = wb.add_format({'text_wrap': True})
                        
                        # Coluna A (Score)
                        ws.conditional_format('A2:A500000', {
                            'type': 'data_bar', 'bar_color': '#FF6347',
                            'min_value': 0, 'max_value': 100
                        })
                        
                        # Largura das Colunas de Texto
                        ws.set_column('A:A', 12)  # Score
                        ws.set_column('B:B', 30)  # Status
                        ws.set_column('G:G', 60, fmt_wrap) # Log Auditoria (Agora está na G)
                        
                        # === O MAPEAMENTO DO DINHEIRO ===
                        # Na versão final, a coluna VALOR_REAL caiu na letra R
                        # A coluna AGING_DIAS caiu na letra S
                        
                        ws.set_column('P:P', 12, fmt_num)   # Saldo Reconstruído (Qtd)
                        ws.set_column('Q:Q', 12, fmt_num)   # Saldo MB52 Ref (Qtd)
                        
                        ws.set_column('R:R', 18, fmt_money) # VALOR_REAL -> AGORA SIM EM R$
                        ws.set_column('S:S', 10, fmt_num)   # AGING -> AGORA SIM NÚMERO

                sucesso = True
            
            except PermissionError:
                log.error("🚫 ARQUIVO ABERTO! Feche o Excel e aguarde...")
                time.sleep(5)
                tentativas += 1
        
        if not sucesso: return

        # 5. Salva Evidências
        saida_evi = config['saidas']['evidencias']
        try: df_evidencias.to_csv(saida_evi, index=False, sep=';', decimal=',')
        except: pass

        log.info("✅ RELATÓRIO FORMATADO CORRETAMENTE!")

    except Exception as e:
        log.error(f"❌ ERRO CRÍTICO: {str(e)}", exc_info=True)

if __name__ == "__main__":
    processar_tudo()