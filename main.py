# main.py
import os
import pandas as pd
from core.sap_reader import SAPReader
from core.processor import MMProcessor
from core.auditoria import AuditoriaAMED
from core.validator import DataValidator
from utils.logger import configurar_logger
from utils.formatting import ExcelFormatter
from utils.mapping import COLUNAS_PADRAO_MB51, COLUNAS_PADRAO_MB52, COLUNAS_PADRAO_ALDREI, OFFSET_DASHBOARD

def inicializar_ambiente():
    for pasta in ['data', 'output', 'logs']:
        if not os.path.exists(pasta):
            os.makedirs(pasta)
    
    if not os.listdir('data'):
        return False
    return True

def processar_tudo():
    pd.set_option('mode.chained_assignment', None)
    log = configurar_logger()
    
    if not inicializar_ambiente():
        log.error("❌ Pasta 'data' vazia ou inexistente. Abortando.")
        return

    log.info("🚀 Iniciando Motor de Auditoria SAP PRO")

    try:
        reader = SAPReader()
        validator = DataValidator(log)
        proc = MMProcessor()
        audit = AuditoriaAMED()

        # 1. LEITURA GRANULAR
        try:
            df51 = reader.carregar_mb51('data/MB51.xlsx')
            df52 = reader.carregar_mb52('data/MB52.xlsx')
            df_ald = reader.carregar_aldrei('data/Aldrei.xlsx')
        except Exception as e:
            log.error(f"❌ Erro na leitura dos arquivos: {str(e)}")
            return

        # 2. VALIDAÇÃO COMPLETA
        validacao = [
            validator.validar_colunas(df51, "MB51", COLUNAS_PADRAO_MB51),
            validator.validar_colunas(df52, "MB52", COLUNAS_PADRAO_MB52),
            validator.validar_colunas(df_ald, "Aldrei", COLUNAS_PADRAO_ALDREI)
        ]
        
        if not all(validacao):
            log.error("❌ Falha na validação de colunas. Verifique os arquivos.")
            return

        # 3. PROCESSAMENTO
        log.info("⚙️ Processando inteligência MM e Auditoria...")
        aba1 = proc.processar_aba1(df51, df52)
        aba2 = audit.processar_aba2(df_ald)

        # 4. EXPORTAÇÃO
        saida = 'output/Resultado_Auditoria_PRO.xlsx'
        with pd.ExcelWriter(saida, engine='xlsxwriter') as writer:
            aba1.to_excel(writer, sheet_name='analise MB', index=False)
            aba2.to_excel(writer, sheet_name='analise auditoria', index=False, startrow=OFFSET_DASHBOARD)
            ExcelFormatter.aplicar_formato(writer, aba1, aba2)

        log.info(f"✅ PROCESSO CONCLUÍDO! Resultado: {saida}")

    except Exception as e:
        log.error(f"❌ ERRO CRÍTICO NO SISTEMA: {str(e)}", exc_info=True)

if __name__ == "__main__":
    processar_tudo()