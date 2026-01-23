# main.py
import os
import pandas as pd
from core.sap_reader import SAPReader
from core.auditoria import AuditoriaAMED
from utils.logger import configurar_logger
from utils.formatting import ExcelFormatter

def inicializar_ambiente():
    for p in ['data', 'output', 'logs']:
        if not os.path.exists(p): os.makedirs(p)

def processar_tudo():
    log = configurar_logger()
    inicializar_ambiente()
    log.info("🚀 INICIANDO AUDITORIA AMED v4.0 (CRITICAL EDITION)")

    try:
        reader = SAPReader()
        audit = AuditoriaAMED()

        # 1. Carregamento Inteligente
        log.info("📥 Carregando e consolidando saldos MB52...")
        # Nota: Ajuste o nome do arquivo se necessário (ex: MB52.xlsx)
        mapa_mb52 = reader.carregar_mapa_mb52('data/MB52.xlsx')
        
        log.info("📥 Mapeando Centros...")
        # Nota: O código agora busca colunas dinamicamente, evitando erro de índice
        mapa_centros = reader.carregar_mapa_centros('data/Centro.xlsx')
        
        log.info("📥 Lendo Auditoria de Campo (Aldrei)...")
        df_ald = reader.carregar_aldrei('data/Aldrei.xlsx')

        # 2. Processamento (Vetorizado + Livro Razão)
        log.info(f"⚙️ Processando {len(df_ald)} registros...")
        resultado = audit.processar_auditoria(df_ald, mapa_centros, mapa_mb52)

        # 3. Exportação e Dashboards
        saida = 'output/Resultado_Auditoria_PRO.xlsx'
        log.info("📊 Gerando Painel Executivo e Relatórios...")
        
        with pd.ExcelWriter(saida, engine='xlsxwriter') as writer:
            resultado.to_excel(writer, sheet_name='analise auditoria', index=False)
            ExcelFormatter.aplicar_formato(writer, resultado)

        log.info(f"✅ SUCESSO ABSOLUTO! Arquivo gerado: {saida}")

    except Exception as e:
        log.error(f"❌ FALHA CRÍTICA: {str(e)}", exc_info=True)

if __name__ == "__main__":
    processar_tudo()