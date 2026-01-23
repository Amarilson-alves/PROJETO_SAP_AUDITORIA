# main.py
import os
import pandas as pd
from core.sap_reader import SAPReader
from core.auditoria import AuditoriaAMED
from utils.logger import configurar_logger
from utils.formatting import ExcelFormatter

def processar_tudo():
    log = configurar_logger()
    log.info("🚀 Iniciando Auditoria v3.1 (Acumulada)")

    try:
        reader = SAPReader()
        audit = AuditoriaAMED()

        # 1. LEITURA E MAPEAMENTO (Acumulando valores aqui)
        log.info("📥 Carregando e somando saldos da MB52...")
        mapa_mb52 = reader.carregar_mapa_mb52('data/MB52.xlsx')
        
        log.info("📥 Carregando mapa de Centros...")
        mapa_centros = reader.carregar_mapa_centros('data/Centro.xlsx')
        
        log.info("📥 Lendo planilha Aldrei...")
        df_ald = reader.carregar_aldrei('data/Aldrei.xlsx')

        # 2. PROCESSAMENTO
        log.info("⚙️ Executando Auditoria com saldos somados...")
        resultado = audit.processar_auditoria(df_ald, mapa_centros, mapa_mb52)

        # 3. EXPORTAÇÃO
        saida = 'output/Resultado_Auditoria_PRO.xlsx'
        with pd.ExcelWriter(saida, engine='xlsxwriter') as writer:
            # Dados agora começam do topo na sua aba
            resultado.to_excel(writer, sheet_name='analise auditoria', index=False)
            # O formatador cria a aba "Painel" separada
            ExcelFormatter.aplicar_formato(writer, resultado)

        log.info(f"✅ SUCESSO! Resultado em {saida}")

    except Exception as e:
        log.error(f"❌ ERRO CRÍTICO: {str(e)}", exc_info=True)

if __name__ == "__main__":
    processar_tudo()