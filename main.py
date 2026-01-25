# main.py
import os
import pandas as pd
from core.sap_reader import SAPReader
from core.auditoria import AuditoriaAMED
from utils.logger import configurar_logger
from utils.formatting import ExcelFormatter

def processar_tudo():
    log = configurar_logger()
    for p in ['data', 'output', 'logs']:
        if not os.path.exists(p): os.makedirs(p)
        
    log.info("🚀 INICIANDO AUDITORIA AMED (Lógica Consolidada)")

    try:
        reader = SAPReader()
        audit = AuditoriaAMED()

        # 1. Leitura
        log.info("📥 Carregando bases...")
        mapa_mb52 = reader.carregar_mapa_mb52('data/MB52.xlsx')
        mapa_centros = reader.carregar_mapa_centros('data/Centro.xlsx') # Singular
        df_ald = reader.carregar_aldrei('data/Aldrei.xlsx')

        # 2. Processamento
        log.info("⚙️ Cruzando dados e calculando financeiro...")
        resultado = audit.processar_auditoria(df_ald, mapa_centros, mapa_mb52)

        # 3. Exportação
        saida = 'output/Resultado_Auditoria_PRO.xlsx'
        with pd.ExcelWriter(saida, engine='xlsxwriter') as writer:
            resultado.to_excel(writer, sheet_name='analise auditoria', index=False)
            ExcelFormatter.aplicar_formato(writer, resultado)

        log.info(f"✅ SUCESSO! Arquivo: {saida}")

    except Exception as e:
        log.error(f"❌ ERRO CRÍTICO: {str(e)}", exc_info=True)

if __name__ == "__main__":
    processar_tudo()