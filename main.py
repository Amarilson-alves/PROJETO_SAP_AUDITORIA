# main.py
import os
import pandas as pd
import pandera as pa
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
        
    log.info("🚀 INICIANDO AUDITORIA AMED v6.0 (DATA LINEAGE)")

    try:
        config = carregar_config()
        reader = SAPReader(config)
        audit = AuditoriaAMED(config)

        # 1. Leitura
        log.info("📥 Carregando Saldos MB52 + Evidências...")
        # AGORA RECEBEMOS DOIS OBJETOS
        mapa_mb52, df_evidencias = reader.carregar_mapa_mb52()
        
        log.info(f"   📝 {len(df_evidencias)} linhas de rastreabilidade geradas.")
        
        log.info("📥 Carregando Histórico MB51...")
        mapa_mb51 = reader.carregar_historico_movimentos()
        
        log.info("📥 Mapeando Centros...")
        mapa_centros = reader.carregar_mapa_centros()
        
        log.info("📥 Lendo Aldrei...")
        df_ald = reader.carregar_aldrei()

        # Validação
        log.info("🛡️ Validando Schema...")
        try: SchemaAldrei.validate(df_ald)
        except pa.errors.SchemaError as err:
            log.error(f"❌ ARQUIVO ALDREI INVÁLIDO: {err}"); return

        # 2. Processamento Principal
        log.info("⚙️ Cruzando dados...")
        resultado = audit.processar_auditoria(df_ald, mapa_centros, mapa_mb52, mapa_mb51)

        # 3. Exportação Principal (Resultado)
        saida_res = 'output/Resultado_Auditoria_PRO.xlsx'
        with pd.ExcelWriter(saida_res, engine='xlsxwriter') as writer:
            resultado.to_excel(writer, sheet_name='analise auditoria', index=False)
            ExcelFormatter.aplicar_formato(writer, resultado)

        # 4. Exportação Secundária (Evidências / Lineage)
        saida_evi = 'output/EVIDENCIAS_DETALHADAS_MB52.csv'
        log.info(f"💾 Salvando Rastreabilidade em CSV...")
        # Salvamos em CSV para ser leve e abrir rápido
        df_evidencias.to_csv(saida_evi, index=False, sep=';', decimal=',')

        log.info("✅ PROCESSO CONCLUÍDO COM SUCESSO!")
        log.info(f"   👉 Resultado Final: {saida_res}")
        log.info(f"   👉 Prova Real (CSV): {saida_evi}")

    except Exception as e:
        log.error(f"❌ ERRO CRÍTICO: {str(e)}", exc_info=True)

if __name__ == "__main__":
    processar_tudo()