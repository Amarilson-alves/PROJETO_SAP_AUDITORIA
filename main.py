import os
from core.sap_reader import SAPReader
from core.processor import MMProcessor
from core.auditoria import AuditoriaAMED
from utils.logger import configurar_logger
from utils.formatting import ExcelFormatter
import pandas as pd

def processar_tudo():
    log = configurar_logger()
    log.info("🚀 Iniciando Processamento...")

    try:
        reader = SAPReader()
        proc = MMProcessor()
        audit = AuditoriaAMED()

        # Caminhos (ajuste conforme sua pasta data/)
        df51 = reader.carregar_mb51('data/MB51.xlsx')
        df52 = reader.carregar_mb52('data/MB52.xlsx')
        df_ald = reader.carregar_aldrei('data/Aldrei.xlsx')

        aba1 = proc.processar_aba1(df51, df52)
        aba2 = audit.processar_aba2(df_ald)

        saida = 'output/Resultado_Pia_do_Sul_PRO.xlsx'
        with pd.ExcelWriter(saida, engine='xlsxwriter') as writer:
            aba1.to_excel(writer, sheet_name='analise MB', index=False)
            aba2.to_excel(writer, sheet_name='analise auditoria', index=False)
            ExcelFormatter.aplicar_formato(writer, aba1, aba2)

        log.info(f"✅ Sucesso! Arquivo gerado em {saida}")

    except Exception as e:
        log.error(f"Erro crítico: {e}", exc_info=True)

if __name__ == "__main__":
    processar_tudo()