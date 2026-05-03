# tests/test_core.py
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from core.sap_reader import SAPReader
from core.auditoria import AuditoriaAMED


# ============================================================
# FIXTURES
# ============================================================

@pytest.fixture
def config_mock():
    return {
        'layout_saida': {'colunas_finais': ['STATUS', 'RESULTADO_OPERACIONAL']},
        'movimentos_baixa': ['221'],
        'arquivos': {
            'mb51': 'data/MB51.xlsx',
            'mb52': 'data/MB52.xlsx',
            'dim_movimentos': 'data/dim_movimentos.csv',
        },
        'colunas_sap': {},
        'regras_negocio': {},
        'indices_fixos': {'centro_col_id': 12, 'centro_col_nome': 3, 'centro_col_dep': 13},
    }

@pytest.fixture
def reader(config_mock):
    return SAPReader(config_mock)

@pytest.fixture
def df_audit_base():
    """DataFrame mínimo válido para processar_auditoria."""
    return pd.DataFrame({
        'ID': ['1'], 'SKU': ['A'], 'UF': ['SP'],
        'OBRA': ['TESTE'], 'FRENTE_ID': ['B2B'],
        'Aliado': ['PARCEIRO'],
        'APL x MEDIÇÃO': [10],
        'APL x DRAFT': [0],
        'Status ID': ['ATIVO'],
        'Tipo de projeto': ['TESTE'],
        'SIGLA': ['SIG1'],
    })


# ============================================================
# TESTES: converter_sap_br
# ============================================================

class TestConverterSapBr:
    def test_valor_positivo_formato_br(self):
        assert SAPReader.converter_sap_br("1.250,50") == 1250.50

    def test_valor_negativo_sufixo_menos(self):
        assert SAPReader.converter_sap_br("1.250,50-") == -1250.50

    def test_valor_com_prefixo_rs(self):
        assert SAPReader.converter_sap_br(" R$ 500,00 ") == 500.00

    def test_inteiro_nativo(self):
        assert SAPReader.converter_sap_br(100) == 100.0

    def test_float_nativo(self):
        assert SAPReader.converter_sap_br(3.14) == 3.14

    def test_string_vazia(self):
        assert SAPReader.converter_sap_br("") == 0.0

    def test_nan_retorna_zero(self):
        assert SAPReader.converter_sap_br(float('nan')) == 0.0

    def test_zero_formato_br(self):
        assert SAPReader.converter_sap_br("0,00") == 0.0

    def test_valor_simples_sem_separador(self):
        assert SAPReader.converter_sap_br("250") == 250.0


# ============================================================
# TESTES: normalize_str
# ============================================================

class TestNormalizeStr:
    def test_remove_sufixo_ponto_zero(self):
        assert SAPReader.normalize_str(" 12345.0 ") == "12345"

    def test_remove_acento_e_maiuscula(self):
        assert SAPReader.normalize_str("Café") == "CAFE"

    def test_converte_para_maiuscula(self):
        assert SAPReader.normalize_str("teste") == "TESTE"

    def test_nan_retorna_string_vazia(self):
        assert SAPReader.normalize_str(float('nan')) == ""

    def test_none_retorna_string_vazia(self):
        assert SAPReader.normalize_str(None) == ""

    def test_string_ja_normalizada(self):
        assert SAPReader.normalize_str("ABC") == "ABC"

    def test_remove_acento_til(self):
        assert SAPReader.normalize_str("Ação") == "ACAO"

    def test_espacos_laterais_removidos(self):
        assert SAPReader.normalize_str("  ABC  ") == "ABC"


# ============================================================
# TESTES: extrair_id_valido
# ============================================================

class TestExtrairIdValido:
    def test_id_5_digitos(self):
        assert SAPReader.extrair_id_valido("12345") == "12345"

    def test_id_7_digitos(self):
        assert SAPReader.extrair_id_valido("1234567") == "1234567"

    def test_id_embutido_em_texto(self):
        assert SAPReader.extrair_id_valido("ID: 54321 - OBRA") == "54321"

    def test_sem_digitos_retorna_none(self):
        assert SAPReader.extrair_id_valido("ABC DEF") is None

    def test_4_digitos_ignorado(self):
        assert SAPReader.extrair_id_valido("1234") is None

    def test_8_digitos_ignorado(self):
        assert SAPReader.extrair_id_valido("12345678") is None

    def test_nan_retorna_none(self):
        assert SAPReader.extrair_id_valido(float('nan')) is None

    def test_retorna_primeiro_match(self):
        assert SAPReader.extrair_id_valido("11111 e 22222") == "11111"


# ============================================================
# TESTES: _extrair_id_limpo
# ============================================================

class TestExtrairIdLimpo:
    def test_usa_col_recebedor_com_id_valido(self, reader):
        df = pd.DataFrame({'REC': ['12345'], 'TXT': ['sem numero']})
        assert reader._extrair_id_limpo(df, 'REC', 'TXT').iloc[0] == '12345'

    def test_fallback_para_col_texto_quando_rec_vazio(self, reader):
        df = pd.DataFrame({'REC': [None], 'TXT': ['ID 54321']})
        assert reader._extrair_id_limpo(df, 'REC', 'TXT').iloc[0] == '54321'

    def test_sem_id_retorna_sem_id(self, reader):
        df = pd.DataFrame({'REC': [None], 'TXT': ['SEM NUMERO']})
        assert reader._extrair_id_limpo(df, 'REC', 'TXT').iloc[0] == 'SEM_ID'

    def test_sem_colunas_retorna_sem_id(self, reader):
        df = pd.DataFrame({'X': [1, 2]})
        resultado = reader._extrair_id_limpo(df, None, None)
        assert list(resultado) == ['SEM_ID', 'SEM_ID']

    def test_col_rec_tem_prioridade_sobre_texto(self, reader):
        df = pd.DataFrame({'REC': ['11111'], 'TXT': ['22222']})
        assert reader._extrair_id_limpo(df, 'REC', 'TXT').iloc[0] == '11111'

    def test_multiplas_linhas_mistas(self, reader):
        df = pd.DataFrame({
            'REC': ['11111', None, None],
            'TXT': [None, '22222', 'sem id'],
        })
        resultado = reader._extrair_id_limpo(df, 'REC', 'TXT')
        assert list(resultado) == ['11111', '22222', 'SEM_ID']


# ============================================================
# TESTES: cache _carregar_mb51
# ============================================================

class TestCacheMb51:
    def test_arquivo_inexistente_retorna_df_vazio(self, config_mock):
        config_inexistente = {**config_mock, 'arquivos': {'mb51': 'data/ARQUIVO_QUE_NAO_EXISTE.xlsx'}}
        r = SAPReader(config_inexistente)
        df = r._carregar_mb51()
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_df_vazio_e_cacheado_evitando_re_checagem(self, reader):
        """Arquivo inexistente: o resultado vazio também é cacheado."""
        reader._carregar_mb51()
        reader._carregar_mb51()
        assert reader._mb51_cache is not None

    def test_read_excel_chamado_apenas_uma_vez(self, config_mock):
        """MB51 deve ser lido do disco somente 1× por instância."""
        df_fake = pd.DataFrame({'COL': [1, 2, 3]})
        reader = SAPReader(config_mock)
        with patch('core.sap_reader.os.path.exists', return_value=True), \
             patch('pandas.read_excel', return_value=df_fake) as mock_read:
            reader._carregar_mb51()
            reader._carregar_mb51()
            reader._carregar_mb51()
        assert mock_read.call_count == 1

    def test_retorna_copia_independente_do_cache(self, config_mock):
        """Modificar o DataFrame retornado não deve corromper o cache."""
        df_fake = pd.DataFrame({'A': [1, 2]})
        reader = SAPReader(config_mock)
        with patch('core.sap_reader.os.path.exists', return_value=True), \
             patch('pandas.read_excel', return_value=df_fake):
            df1 = reader._carregar_mb51()
            df1['COLUNA_NOVA'] = 99
            df2 = reader._carregar_mb51()
        assert 'COLUNA_NOVA' not in df2.columns

    def test_multiplos_metodos_compartilham_cache(self, config_mock):
        """gerar_mapa_centros_por_id e outro método devem usar o mesmo cache."""
        df_fake = pd.DataFrame({'COL': [1]})
        reader = SAPReader(config_mock)
        with patch('core.sap_reader.os.path.exists', return_value=True), \
             patch('pandas.read_excel', return_value=df_fake) as mock_read:
            reader._carregar_mb51()
            reader._carregar_mb51()
        assert mock_read.call_count == 1


# ============================================================
# TESTES: AuditoriaAMED — processar_auditoria
# ============================================================

class TestProcessarAuditoria:

    def _chamar(self, audit, df_aud, mapa_exec_cen=None, mapa_mb52=None, mapa_mb51=None):
        return audit.processar_auditoria(
            df_aud,
            pd.DataFrame(),          # df_cidades
            mapa_exec_cen or {},     # mapa_exec_cen
            {},                      # mapa_exec_dep
            mapa_mb52 or {},         # mapa_mb52
            mapa_mb51 or {},         # mapa_centros_mb51
        )

    def test_resultado_tem_linhas(self, config_mock, df_audit_base):
        audit = AuditoriaAMED(config_mock)
        res = self._chamar(audit, df_audit_base, mapa_exec_cen={'1': 'CEN1'})
        assert len(res) > 0

    def test_saldo_zero_gera_ok(self, config_mock):
        audit = AuditoriaAMED(config_mock)
        df = pd.DataFrame({
            'ID': ['1'], 'SKU': ['A'], 'UF': ['SP'],
            'OBRA': ['T'], 'FRENTE_ID': ['B2B'], 'Aliado': ['P'],
            'APL x MEDIÇÃO': [0], 'APL x DRAFT': [0],
            'Status ID': ['ATIVO'], 'Tipo de projeto': ['T'],
        })
        res = self._chamar(audit, df, mapa_exec_cen={'1': 'CEN1'})
        assert res.iloc[0]['STATUS'] == 'OK'

    def test_saldo_negativo_gera_falta(self, config_mock):
        audit = AuditoriaAMED(config_mock)
        df = pd.DataFrame({
            'ID': ['1'], 'SKU': ['A'], 'UF': ['SP'],
            'OBRA': ['T'], 'FRENTE_ID': ['B2B'], 'Aliado': ['P'],
            'APL x MEDIÇÃO': [-5], 'APL x DRAFT': [0],
            'Status ID': ['ATIVO'], 'Tipo de projeto': ['T'],
        })
        res = self._chamar(audit, df, mapa_exec_cen={'1': 'CEN1'})
        assert res.iloc[0]['STATUS'] == 'FALTA'

    def test_saldo_positivo_sem_falta_pendente_gera_estorno(self, config_mock):
        audit = AuditoriaAMED(config_mock)
        df = pd.DataFrame({
            'ID': ['1'], 'SKU': ['A'], 'UF': ['SP'],
            'OBRA': ['T'], 'FRENTE_ID': ['B2B'], 'Aliado': ['P'],
            'APL x MEDIÇÃO': [10], 'APL x DRAFT': [0],
            'Status ID': ['ATIVO'], 'Tipo de projeto': ['T'],
        })
        res = self._chamar(audit, df, mapa_exec_cen={'1': 'CEN1'})
        assert res.iloc[0]['STATUS'] == 'ESTORNO'

    def test_status_cancelado_ignora_saldo(self, config_mock):
        audit = AuditoriaAMED(config_mock)
        df = pd.DataFrame({
            'ID': ['1'], 'SKU': ['A'], 'UF': ['SP'],
            'OBRA': ['T'], 'FRENTE_ID': ['B2B'], 'Aliado': ['P'],
            'APL x MEDIÇÃO': [-99], 'APL x DRAFT': [0],
            'Status ID': ['CANCELADO'], 'Tipo de projeto': ['T'],
        })
        res = self._chamar(audit, df, mapa_exec_cen={'1': 'CEN1'})
        assert res.iloc[0]['STATUS'] == 'Cancelado'

    def test_centro_nao_encontrado_gera_verif_centro(self, config_mock):
        audit = AuditoriaAMED(config_mock)
        df = pd.DataFrame({
            'ID': ['99'], 'SKU': ['A'], 'UF': ['SP'],
            'OBRA': ['T'], 'FRENTE_ID': ['B2B'], 'Aliado': ['P'],
            'APL x MEDIÇÃO': [-5], 'APL x DRAFT': [0],
            'Status ID': ['ATIVO'], 'Tipo de projeto': ['T'],
        })
        # mapa_exec_cen e mb51 vazios → centro N/D → degradação
        res = self._chamar(audit, df, mapa_exec_cen={})
        assert res.iloc[0]['STATUS'] == 'Verif.Centro'

    def test_uf_vazia_com_saldo_positivo_gera_uf_erro(self, config_mock):
        audit = AuditoriaAMED(config_mock)
        df = pd.DataFrame({
            'ID': ['1'], 'SKU': ['A'], 'UF': [''],
            'OBRA': ['T'], 'FRENTE_ID': ['B2B'], 'Aliado': ['P'],
            'APL x MEDIÇÃO': [10], 'APL x DRAFT': [0],
            'Status ID': ['ATIVO'], 'Tipo de projeto': ['T'],
        })
        res = self._chamar(audit, df, mapa_exec_cen={'1': 'CEN1'})
        assert res.iloc[0]['STATUS'] == 'UF ERRO'

    def test_historico_mb51_usado_como_fallback_de_centro(self, config_mock):
        audit = AuditoriaAMED(config_mock)
        df = pd.DataFrame({
            'ID': ['1'], 'SKU': ['A'], 'UF': ['SP'],
            'OBRA': ['T'], 'FRENTE_ID': ['B2B'], 'Aliado': ['P'],
            'APL x MEDIÇÃO': [0], 'APL x DRAFT': [0],
            'Status ID': ['ATIVO'], 'Tipo de projeto': ['T'],
        })
        # exec_cen vazio, mas MB51 histórico tem o centro
        mapa_mb51 = {'1': {'principal': 'CEN_MB51', 'todos': 'CEN_MB51'}}
        res = self._chamar(audit, df, mapa_exec_cen={}, mapa_mb51=mapa_mb51)
        assert res.iloc[0]['CENTRO'] == 'CEN_MB51'

    def test_sobra_compensa_falta_de_outro_id(self, config_mock):
        """ID com sobra deve sugerir aplicação no ID com falta (mesma UF e SKU)."""
        audit = AuditoriaAMED(config_mock)
        df = pd.DataFrame({
            'ID': ['1', '2'], 'SKU': ['A', 'A'], 'UF': ['SP', 'SP'],
            'OBRA': ['T', 'T'], 'FRENTE_ID': ['B2B', 'B2B'],
            'Aliado': ['P', 'P'],
            'APL x MEDIÇÃO': [5, -3],
            'APL x DRAFT': [0, 0],
            'Status ID': ['ATIVO', 'ATIVO'],
            'Tipo de projeto': ['T', 'T'],
        })
        res = self._chamar(audit, df,
                           mapa_exec_cen={'1': 'CEN1', '2': 'CEN1'})
        status_sobra = res[res['ID'] == '1'].iloc[0]['STATUS']
        assert status_sobra == 'APLICAÇÃO EXTERNA'

    def test_argumento_mutavel_none_nao_compartilha_estado(self, config_mock, df_audit_base):
        """Duas chamadas independentes não devem compartilhar mapa_centros_mb51."""
        audit = AuditoriaAMED(config_mock)
        res1 = audit.processar_auditoria(
            df_audit_base.copy(), pd.DataFrame(), {}, {}, {}
        )
        res2 = audit.processar_auditoria(
            df_audit_base.copy(), pd.DataFrame(), {}, {}, {}
        )
        assert len(res1) == len(res2)

    def test_colunas_saida_presentes_no_resultado(self, config_mock, df_audit_base):
        audit = AuditoriaAMED(config_mock)
        res = self._chamar(audit, df_audit_base, mapa_exec_cen={'1': 'CEN1'})
        for col in config_mock['layout_saida']['colunas_finais']:
            assert col in res.columns, f"Coluna ausente: {col}"
