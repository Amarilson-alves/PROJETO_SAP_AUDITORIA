# utils/formatting.py
import pandas as pd

class ExcelFormatter:
    @staticmethod
    def aplicar_formato(writer, df):
        workbook = writer.book
        
        # Garante que a aba existe antes de tentar formatar
        if 'analise auditoria' in writer.sheets:
            sheet_data = writer.sheets['analise auditoria']

            # --- FORMATAÇÃO DOS DADOS ---
            # Congela a primeira linha (cabeçalho) para facilitar a rolagem
            sheet_data.freeze_panes(1, 0)
            
            # Colunas financeiras: Formatação PT-BR (Padrão SAP: 1.000,00)
            money_fmt_br = workbook.add_format({'num_format': '#,##0.00'})
            cols_financeiras = ['$ VALOR - AMED', '$ VALOR UNIT', '$ SALDO X QTDE']
            
            for col_name in cols_financeiras:
                if col_name in df.columns:
                    idx = df.columns.get_loc(col_name)
                    # Aplica a largura 18 e o formato contábil na coluna inteira
                    sheet_data.set_column(idx, idx, 18, money_fmt_br)