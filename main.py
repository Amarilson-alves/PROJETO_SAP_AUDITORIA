# main.py
import os
import pandas as pd
import pandera as pa
from utils.settings import carregar_config
from core.sap_reader import SAPReader
from core.auditoria import AuditoriaAMED
from utils.logger import configurar_logger
from utils.formatting import ExcelFormatter
from core.schemas import SchemaAldrei

def processar_tudo():
    log = configurar_logger()
    log.info("🚀 AUDITORIA AMED v5.0 (RASTREABILIDADE TOTAL)")

    try:
        # 1. Carregar Config
        config = carregar_config()
        reader = SAPReader(config)
        audit = AuditoriaAMED(config)

        # 2. Leitura
        log.info("📥 Carregando Saldos (MB52)...")
        mapa_mb52 = reader.carregar_mapa_mb52()
        
        log.info("📥 Carregando Histórico de Baixas (MB51)...")
        # Se a MB51 não existir, o sistema avisa mas não para
        mapa_mb51 = reader.carregar_historico_movimentos()
        
        log.info("📥 Mapeando Centros...")
        mapa_centros = reader.carregar_mapa_centros()
        
        log.info("📥 Lendo Aldrei...")
        df_ald = reader.carregar_aldrei()

        # Validação do schema Aldrei
        try:
            log.info("🛡️ Validando estrutura do arquivo Aldrei...")
            SchemaAldrei.validate(df_ald) # Se faltar coluna 'ID', ele explode aqui com erro claro
            log.info("✅ Estrutura do Aldrei validada com sucesso!")
        except pa.errors.SchemaError as e:
            log.error(f"❌ O arquivo Aldrei está fora do padrão! Detalhe: {e}")
            return  # Interrompe a execução se a validação falhar

        # 3. Processamento
        log.info("⚙️ Cruzando Físico x Contábil x Campo...")
        resultado = audit.processar_auditoria(df_ald, mapa_centros, mapa_mb52, mapa_mb51)

        # 4. Exportação
        saida = 'output/Resultado_Auditoria_PRO.xlsx'
        with pd.ExcelWriter(saida, engine='xlsxwriter') as writer:
            resultado.to_excel(writer, sheet_name='analise auditoria', index=False)
            ExcelFormatter.aplicar_formato(writer, resultado)

        log.info(f"✅ SUCESSO! Análise Completa: {saida}")

    except Exception as e:
        log.error(f"❌ ERRO: {str(e)}", exc_info=True)

if __name__ == "__main__":
    processar_tudo()