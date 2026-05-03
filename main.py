# main.py
import os
import sys
import time
import pandas as pd
import pandera.pandas as pa
from datetime import datetime

from core.schemas import SchemaAuditoria
from utils.settings import carregar_config
from core.sap_reader import SAPReader
from core.auditoria import AuditoriaAMED
from utils.logger import configurar_logger
from utils.formatting import ExcelFormatter

def processar_tudo():
    log = configurar_logger()
    for p in ['data', 'output', 'logs']:
        if not os.path.exists(p): os.makedirs(p)
        
    log.info("🚀 INICIANDO AUDITORIA AMED v18.4 (COM RADAR 311 SEMPRE VISÍVEL)")

    try:
        config = carregar_config()
        reader = SAPReader(config)
        audit = AuditoriaAMED(config)

        log.info("📥 Carregando Bases de Dados...")
        try:
            mapa_mb52, df_evidencias = reader.carregar_mapa_mb52()
            df_base = reader.carregar_base_auditoria()
            df_cidades = reader.carregar_centro_cidades()
            mapa_exec_cen, mapa_exec_dep = reader.carregar_centro_exec_amed()
            
        except PermissionError:
            log.error("❌ ERRO DE PERMISSÃO: Feche os arquivos de entrada!"); return

        log.info("🛡️ Validando Schema...")
        try: 
            SchemaAuditoria.validate(df_base)
        except pa.errors.SchemaError as err: 
            log.error(f"❌ ERRO VALIDAÇÃO: {err}"); return

        log.info("🌍 Mapeando Frequência Histórica de Centros (MB51)...")
        mapa_geo_mb51 = reader.gerar_mapa_centros_por_id()

        log.info("⚙️ Processando Auditoria Cruzada (Cascata de Centros)...")
        resultado = audit.processar_auditoria(df_base, df_cidades, mapa_exec_cen, mapa_exec_dep, mapa_mb52, mapa_geo_mb51)

        log.info("🕵️‍♂️ Cruzando Documentos de Aplicação/Estorno...")
        df_rastreio = reader.gerar_rastreio_aplicacoes(resultado)

        log.info("🚨 Verificando Radar de Prevenção de Perdas (Entradas 311)...")
        df_monitor = reader.gerar_monitor_entradas_311(resultado)

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
                    try:
                        ExcelFormatter.aplicar_formato(writer, resultado)
                    except Exception as e:
                        log.warning(f"⚠️ Formatação Excel não aplicada: {e}")
                    
                    # 2. ABA: Rastreio Operacional
                    if not df_rastreio.empty:
                        df_rastreio.to_excel(writer, sheet_name='RASTREIO_APLICACOES', index=False)
                        wb = writer.book; ws_rast = writer.sheets['RASTREIO_APLICACOES']
                        ws_rast.freeze_panes(1, 0)
                        
                        fmt_num = wb.add_format({'num_format': '#,##0.00'})
                        fmt_red = wb.add_format({'num_format': '#,##0.00', 'font_color': '#9C0006'})
                        fmt_green = wb.add_format({'num_format': '#,##0.00', 'font_color': '#006100'})
                        
                        ws_rast.set_column('A:B', 12); ws_rast.set_column('C:D', 35)
                        ws_rast.set_column('E:F', 18); ws_rast.set_column('G:G', 16, fmt_num)
                        ws_rast.set_column('H:J', 18, fmt_green); ws_rast.set_column('K:M', 18, fmt_red)
                        ws_rast.set_column('N:O', 18, fmt_num); ws_rast.set_column('P:S', 22)

                    # 3. ABA: Radar de Entradas 311 (A Tabela Fato) - 🔴 AGORA SEMPRE NASCE!
                    df_monitor.to_excel(writer, sheet_name='MONITOR_ENTRADAS_311', index=False)
                    wb = writer.book
                    ws_mon = writer.sheets['MONITOR_ENTRADAS_311']
                    ws_mon.freeze_panes(1, 0)
                    
                    fmt_qtd_mon = wb.add_format({'num_format': '#,##0.00'})
                    
                    ws_mon.set_column('A:A', 15); ws_mon.set_column('B:C', 12)
                    ws_mon.set_column('D:D', 15); ws_mon.set_column('E:E', 30) # O Alerta
                    ws_mon.set_column('F:H', 25); ws_mon.set_column('I:I', 15)
                    ws_mon.set_column('J:J', 40); ws_mon.set_column('K:K', 18, fmt_qtd_mon)
                    ws_mon.set_column('L:L', 15)
                    
                    # 4. ABA: Raio-X AMED (Estoque Parado)
                    if not df_raiox.empty:
                        df_raiox.sort_values(by=['SCORE_RISCO', 'VALOR_REAL'], ascending=[False, False], inplace=True)
                        df_raiox.to_excel(writer, sheet_name='RAIO_X_AMED', index=False)
                        wb = writer.book; ws_raiox = writer.sheets['RAIO_X_AMED']
                        fmt_money = wb.add_format({'num_format': 'R$ #,##0.00'})
                        fmt_num_int = wb.add_format({'num_format': '0'})
                        ws_raiox.set_column('A:A', 12); ws_raiox.set_column('P:Q', 12, fmt_num_int)
                        ws_raiox.set_column('R:R', 18, fmt_money); ws_raiox.set_column('S:S', 10, fmt_num_int)

                    # 5. ABA: Extrato Diário (Fluxo Logístico)
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

        try:
            df_evidencias.to_csv(config['saidas']['evidencias'], index=False, sep=';', decimal=',')
        except Exception as e:
            log.warning(f"⚠️ CSV de evidências não salvo: {e}")
        log.info("✅ RELATÓRIO FINALIZADO COM SUCESSO!")

    except Exception as e:
        log.error(f"❌ ERRO CRÍTICO: {str(e)}", exc_info=True)

if __name__ == "__main__":
    processar_tudo()