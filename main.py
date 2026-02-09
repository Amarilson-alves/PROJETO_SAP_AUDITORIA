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
        
    log.info("🚀 INICIANDO AUDITORIA AMED v17.0 (ENTERPRISE HARDENED)")

    try:
        config = carregar_config()
        reader = SAPReader(config)
        audit = AuditoriaAMED(config)

        log.info("📥 Carregando Bases de Dados...")
        try:
            mapa_mb52, df_evidencias = reader.carregar_mapa_mb52()
            mapa_centros = reader.carregar_mapa_centros()
            df_ald = reader.carregar_aldrei()
        except PermissionError:
            log.error("❌ ERRO DE PERMISSÃO: Feche os arquivos de entrada!"); return

        log.info("🛡️ Validando Schema...")
        try: SchemaAldrei.validate(df_ald)
        except pa.errors.SchemaError as err: log.error(f"❌ ERRO VALIDAÇÃO: {err}"); return

        log.info("🌍 Mapeando Centros por ID (MB51)...")
        mapa_geo = reader.gerar_mapa_centros_por_id()

        log.info("⚙️ Processando Auditoria Cruzada...")
        resultado = audit.processar_auditoria(df_ald, mapa_centros, mapa_mb52, mapa_geo)

        log.info("☢️ Executando Motor de Auditoria Contínua...")
        df_raiox = reader.gerar_raio_x_amed(mapa_mb52) 

        # --- GERAÇÃO DE METADADOS (RASTREABILIDADE) ---
        df_meta = pd.DataFrame({
            'CHAVE': ['DATA_EXECUCAO', 'VERSAO_ROBO', 'ARQUIVO_CONFIG', 'ARQUIVO_REGRAS'],
            'VALOR': [
                datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
                'v17.0 (Enterprise)',
                'config.yaml',
                'dim_movimentos.csv'
            ]
        })

        log.info(f"📊 Gerando Relatório Final: {config['saidas']['dashboard']}")
        sucesso = False; tentativas = 0
        while not sucesso and tentativas < 1:
            try:
                with pd.ExcelWriter(config['saidas']['dashboard'], engine='xlsxwriter') as writer:
                    
                    # 1. Metadados (Aba Técnica)
                    df_meta.to_excel(writer, sheet_name='INFO_EXECUCAO', index=False)
                    
                    # 2. Auditoria Padrão
                    resultado.to_excel(writer, sheet_name='analise auditoria', index=False)
                    # Mantém formatação básica de dados (cabeçalho, largura), sem ícones
                    try: ExcelFormatter.aplicar_formato(writer, resultado)
                    except: pass
                    
                    # 3. Raio-X
                    if not df_raiox.empty:
                        df_raiox.sort_values(by=['SCORE_RISCO', 'VALOR_REAL'], ascending=[False, False], inplace=True)
                        sheet_raiox = 'RAIO_X_AMED'
                        df_raiox.to_excel(writer, sheet_name=sheet_raiox, index=False)
                        
                        # Formatação Apenas de Tipos de Dados (Para Power Query)
                        wb = writer.book; ws = writer.sheets[sheet_raiox]
                        fmt_money = wb.add_format({'num_format': 'R$ #,##0.00'})
                        fmt_num = wb.add_format({'num_format': '0'})
                        
                        # Colunas de Valores e Números
                        ws.set_column('A:A', 12) # Score
                        ws.set_column('P:P', 12, fmt_num)
                        ws.set_column('Q:Q', 12, fmt_num)
                        ws.set_column('R:R', 18, fmt_money)
                        ws.set_column('S:S', 10, fmt_num)

                sucesso = True
            except PermissionError:
                log.error("🚫 ARQUIVO ABERTO! Feche o Excel..."); time.sleep(5); tentativas += 1
        
        if not sucesso: return
        try: df_evidencias.to_csv(config['saidas']['evidencias'], index=False, sep=';', decimal=',')
        except: pass
        log.info("✅ RELATÓRIO FINALIZADO COM SUCESSO!")

    except Exception as e:
        log.error(f"❌ ERRO CRÍTICO: {str(e)}", exc_info=True)

if __name__ == "__main__":
    processar_tudo()