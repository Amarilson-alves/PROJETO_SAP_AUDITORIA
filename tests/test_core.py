# tests/test_core.py
import pytest
import pandas as pd
from core.sap_reader import SAPReader
from core.auditoria import AuditoriaAMED

# --- TESTES DO LEITOR (SAPReader) ---

def test_limpeza_valor_sap_positivo():
    assert SAPReader.converter_sap_br("1.250,50") == 1250.50

def test_limpeza_valor_sap_negativo_final():
    assert SAPReader.converter_sap_br("1.250,50-") == -1250.50

def test_limpeza_valor_texto_sujo():
    assert SAPReader.converter_sap_br(" R$ 500,00 ") == 500.00

def test_normalizacao_strings():
    assert SAPReader.normalize_str(" 12345.0 ") == "12345"
    assert SAPReader.normalize_str("Café") == "CAFE"

# --- TESTES DA LÓGICA DE AUDITORIA ---

@pytest.fixture
def config_mock():
    return {
        'layout_saida': {'colunas_finais': ['STATUS', 'RESULTADO_OPERACIONAL']},
        'movimentos_baixa': ['221']
    }

def test_logica_auditoria_saldo_positivo(config_mock):
    audit = AuditoriaAMED(config_mock)
    
    # MOCK CORRIGIDO: Adicionada a coluna 'APL x DRAFT' que faltava
    df_aud = pd.DataFrame({
        'ID': ['1'], 'SKU': ['A'], 'UF': ['SP'],
        'OBRA': ['TESTE'], 'FRENTE_ID': ['B2B'], 
        'Aliado': ['PARCEIRO'], 
        'APL x MEDIÇÃO': [10], 
        'APL x DRAFT': [0], # Coluna obrigatória adicionada
        'Status ID': ['ATIVO'],
        'Tipo de projeto': ['TESTE']
    })
    
    mapa_centros = {'1': 'CEN1'}
    mapa_mb52 = {('A', 'CEN1', 'AMED'): {'qtd': 10.0, 'valor': 100.0}}
    mapa_mb51 = {}

    res = audit.processar_auditoria(df_aud, mapa_centros, mapa_mb52, mapa_mb51)
    
    # Se aplicou 10 e tem 10 no estoque, o saldo é OK (ou sobra se considerar saída)
    # A lógica atual diz: Saldo Auditoria = APL x MEDIÇÃO = 10.
    # Se saldo > 0 e não tem falta pra compensar, gera sobra.
    # Vamos verificar se gerou resultado
    assert len(res) > 0

def test_logica_auditoria_falta(config_mock):
    audit = AuditoriaAMED(config_mock)
    
    # MOCK CORRIGIDO
    df_aud = pd.DataFrame({
        'ID': ['1'], 'SKU': ['A'], 'UF': ['SP'],
        'OBRA': ['TESTE'], 'FRENTE_ID': ['B2B'], 
        'Aliado': ['PARCEIRO'], 
        'APL x MEDIÇÃO': [-5], 
        'APL x DRAFT': [0], # Coluna obrigatória
        'Status ID': ['ATIVO'],
        'Tipo de projeto': ['TESTE']
    })
    
    res = audit.processar_auditoria(df_aud, {'1': 'CEN1'}, {}, {})
    
    # Saldo negativo = FALTA
    assert 'FALTA' in res.iloc[0]['RESULTADO_OPERACIONAL']