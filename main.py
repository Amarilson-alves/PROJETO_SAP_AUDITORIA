# main.py
import os
import sys
import time
import pandas as pd
import pandera.pandas as pa
from datetime import datetime
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
        
    log.info("🚀 INICIANDO AUDITORIA AMED v18.1 (CLEAN ARCHITECTURE)")

    try:
        config = carregar_config()
        reader = SAPReader(config)
        audit = AuditoriaAMED(config)

        log.info("📥 Carregando Bases de Dados...")
        try:
            mapa_mb52, df_evidencias = reader.carregar_mapa_mb52()
            df_ald = reader.carregar_aldrei()
            
            df_cidades = reader.carregar_centro_cidades()
            mapa_exec_cen, mapa_exec_dep = reader.carregar_centro_exec_amed()
            
        except PermissionError:
            log.error("❌ ERRO DE PERMISSÃO: Feche os arquivos de entrada!"); return

        log.info("🛡️ Validando Schema...")
        try: SchemaAldrei.validate(df_ald)
        except pa.errors.SchemaError as err: log.error(f"❌ ERRO VALIDAÇÃO: {err}"); return

        log.info("🌍 Mapeando Frequência Histórica de Centros (MB51)...")
        mapa_geo_mb51 = reader.gerar_mapa_centros_por_id()

        log.info("⚙️ Processando Auditoria Cruzada (Cascata de Centros)...")
        resultado = audit.processar_auditoria(df_ald, df_cidades, mapa_exec_cen, mapa_exec_dep, mapa_mb52, mapa_geo_mb51)

        log.info("☢️ Executando Motor de Auditoria Contínua...")
        df_raiox = reader.gerar_raio_x_amed(mapa_mb52) 
        
        log.info("📈 Construindo Extrato Diário (Últimos 6 Meses)...")
        df_extrato = reader.gerar_extrato_diario(dias_retroativos=180)

        log.info(f"📊 Gerando Relatório Final: {config['saidas']['dashboard']}")
        sucesso = False; tentativas = 0
        while not sucesso and tentativas < 3:
            try:
                with pd.ExcelWriter(config['saidas']['dashboard'], engine='xlsxwriter') as writer:
                    
                    # 1. ABA: Auditoria Cruzada (Balanço Patrimonial)
                    resultado.to_excel(writer, sheet_name='analise auditoria', index=False)
                    try: ExcelFormatter.aplicar_formato(writer, resultado)
                    except: pass
                    
                    # 2. ABA: Raio-X AMED (Estoque Parado)
                    if not df_raiox.empty:
                        df_raiox.sort_values(by=['SCORE_RISCO', 'VALOR_REAL'], ascending=[False, False], inplace=True)
                        df_raiox.to_excel(writer, sheet_name='RAIO_X_AMED', index=False)
                        wb = writer.book; ws = writer.sheets['RAIO_X_AMED']
                        fmt_money = wb.add_format({'num_format': 'R$ #,##0.00'})
                        fmt_num = wb.add_format({'num_format': '0'})
                        ws.set_column('A:A', 12); ws.set_column('P:Q', 12, fmt_num)
                        ws.set_column('R:R', 18, fmt_money); ws.set_column('S:S', 10, fmt_num)

                    # 3. ABA: Extrato Diário (Fluxo Logístico)
                    if not df_extrato.empty:
                        df_extrato.to_excel(writer, sheet_name='EXTRATO_DIARIO', index=False)
                        ws_ext = writer.sheets['EXTRATO_DIARIO']
                        fmt_num_ext = wb.add_format({'num_format': '#,##0.00'})
                        ws_ext.set_column('A:A', 15); ws_ext.set_column('B:C', 12)
                        ws_ext.set_column('D:D', 18); ws_ext.set_column('E:E', 40)
                        ws_ext.set_column('F:F', 12); ws_ext.set_column('G:I', 18, fmt_num_ext)

                sucesso = True
            except PermissionError:
                tentativas += 1
                log.warning(f"⚠️ Arquivo Excel aberto. Tentativa {tentativas}/3. Pausando 5s...")
                time.sleep(5)
        
        if not sucesso:
            log.error("🚫 Falha ao salvar: O arquivo permaneceu aberto após 3 tentativas.")
            return

        try: df_evidencias.to_csv(config['saidas']['evidencias'], index=False, sep=';', decimal=',')
        except: pass
        log.info("✅ RELATÓRIO FINALIZADO COM SUCESSO!")

    except Exception as e:
        log.error(f"❌ ERRO CRÍTICO: {str(e)}", exc_info=True)

if __name__ == "__main__":
    processar_tudo()