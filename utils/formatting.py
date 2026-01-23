# utils/formatting.py
import pandas as pd

class ExcelFormatter:
    @staticmethod
    def aplicar_formato(writer, df):
        workbook = writer.book
        sheet_dash = workbook.add_worksheet('Painel')
        sheet_data = writer.sheets['analise auditoria']
        sheet_dash.activate()

        # Estilos
        header_fmt = workbook.add_format({'bold': True, 'font_size': 16, 'font_color': '#FFFFFF', 'bg_color': '#1F4E78', 'align': 'center'})
        card_label_fmt = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#D9E1F2', 'border': 1, 'align': 'center'})
        card_val_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'border': 1, 'align': 'center', 'font_color': '#1F4E78'})
        card_money_fmt = workbook.add_format({'bold': True, 'font_size': 12, 'border': 1, 'align': 'center', 'font_color': '#006100', 'num_format': 'R$ #,##0.00'})
        aliado_header_fmt = workbook.add_format({'bold': True, 'bg_color': '#EAEAEA', 'border': 1, 'align': 'center'})

        # 1. TÍTULO
        sheet_dash.merge_range('B2:F2', 'PAINEL DE INDICADORES - AUDITORIA AMED', header_fmt)

        # 2. CÁLCULOS
        total_regs = len(df)
        ok = len(df[df['RESULTADO_OPERACIONAL'].str.contains('OK', na=False)])
        pend = len(df[df['RESULTADO_OPERACIONAL'].str.contains('PENDENTE', na=False)])
        div = len(df[df['RESULTADO_OPERACIONAL'].str.contains('DIVERGÊNCIA', na=False)])
        saldo_financeiro = df['$ VALOR - AMED'].sum()

        # 3. CARDS
        metrics = [
            ("TOTAL REGISTROS", total_regs, 1, card_val_fmt),
            ("CONFORMADOS (OK)", ok, 2, card_val_fmt),
            ("PENDENTES", pend, 3, card_val_fmt),
            ("DIVERGÊNCIAS", div, 4, card_val_fmt),
            ("SALDO AMED", saldo_financeiro, 5, card_money_fmt)
        ]

        for label, val, col, fmt in metrics:
            sheet_dash.write(3, col, label, card_label_fmt)
            sheet_dash.write(4, col, val, fmt)

        # 4. TABELA ALIADOS
        sheet_dash.write('B7', 'SALDO AMED POR ALIADO', workbook.add_format({'bold': True}))
        resumo = df.groupby('Aliado')['$ VALOR - AMED'].sum().reset_index().sort_values('$ VALOR - AMED', ascending=False)
        
        row_idx = 8
        sheet_dash.write(row_idx, 1, 'ALIADO', aliado_header_fmt)
        sheet_dash.write(row_idx, 2, 'TOTAL VALOR', aliado_header_fmt)
        
        for i, row in resumo.iterrows():
            sheet_dash.write(row_idx + 1 + i, 1, row['Aliado'], workbook.add_format({'border': 1}))
            sheet_dash.write(row_idx + 1 + i, 2, row['$ VALOR - AMED'], workbook.add_format({'border': 1, 'num_format': 'R$ #,##0.00'}))

        # 5. AJUSTES
        sheet_dash.set_column('B:B', 40)
        sheet_dash.set_column('C:F', 20)
        sheet_data.freeze_panes(1, 0)
        sheet_dash.hide_gridlines(2)