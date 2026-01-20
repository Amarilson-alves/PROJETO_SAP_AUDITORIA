import os
import pandas as pd
from core.sap_reader import SAPReader
from core.processor import MMProcessor
from core.auditoria import AuditoriaAMED
from core.validator import DataValidator
from utils.logger import configurar_logger
from utils.formatting import ExcelFormatter

def processar_tudo():
    # --- AUTO-SETUP ---
    # Garante que as pastas existam antes de iniciar
    for pasta in ['data', 'output', 'logs']:
        if not os.path.exists(pasta):
            os.makedirs(pasta)

    log = configurar_logger()
    log.info("🚀 Iniciando Motor de Auditoria SAP PRO")

    try:
        reader = SAPReader()
        validator = DataValidator(log)
        proc = MMProcessor()
        audit = AuditoriaAMED()

        # 1. LEITURA
        log.info("📥 Carregando arquivos da pasta /data...")
        df51 = reader.carregar_mb51('data/MB51.xlsx')
        df52 = reader.carregar_mb52('data/MB52.xlsx')
        df_ald = reader.carregar_aldrei('data/Aldrei.xlsx')

        # 2. VALIDAÇÃO
        colunas_mb51 = ['Centro', 'Material', 'Quantidade', 'Tipo de movimento']
        if not validator.validar_colunas(df51, "MB51", colunas_mb51):
            return

        # 3. PROCESSAMENTO
        log.info("⚙️ Processando inteligência MM e Auditoria...")
        aba1 = proc.processar_aba1(df51, df52)
        aba2 = audit.processar_aba2(df_ald)

        # 4. EXPORTAÇÃO E FORMATAÇÃO
        saida = 'output/Resultado_Pia_do_Sul_PRO.xlsx'
        with pd.ExcelWriter(saida, engine='xlsxwriter') as writer:
            aba1.to_excel(writer, sheet_name='analise MB', index=False)
            aba2.to_excel(writer, sheet_name='analise auditoria', index=False)
            ExcelFormatter.aplicar_formato(writer, aba1, aba2)

        log.info(f"✅ PROCESSO CONCLUÍDO! Resultado em: {saida}")

    except Exception as e:
        log.error(f"❌ ERRO CRÍTICO: {str(e)}", exc_info=True)

if __name__ == "__main__":
    processar_tudo()