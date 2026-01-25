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
        header_fmt = workbook.add_format({'bold': True, 'font_size': 16, 'font_color': 'white', 'bg_color': '#1F4E78', 'align': 'center'})
        card_label = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1, 'align': 'center'})
        card_val = workbook.add_format({'bold': True, 'font_size': 14, 'border': 1, 'align': 'center', 'font_color': '#1F4E78'})
        card_money = workbook.add_format({'bold': True, 'font_size': 12, 'border': 1, 'align': 'center', 'font_color': '#006100', 'num_format': 'R$ #,##0.00'})
        
        # --- PAINEL ---
        sheet_dash.merge_range('B2:F2', 'PAINEL DE INDICADORES AMED', header_fmt)
        
        # Soma usando a coluna correta
        val_amed = df['$ VALOR - AMED'].sum()
        total_regs = len(df)
        ok = len(df[df['RESULTADO_OPERACIONAL'].str.contains('OK', na=False)])
        pend = len(df[df['RESULTADO_OPERACIONAL'].str.contains('APLICAÇÃO EXTERNA|FALTA', na=False)])
        div = len(df[df['RESULTADO_OPERACIONAL'].str.contains('ESTORNO', na=False)])

        kpis = [("TOTAL", total_regs, 1, card_val), ("OK", ok, 2, card_val),
                ("PENDENTES", pend, 3, card_val), ("DIVERGÊNCIAS", div, 4, card_val),
                ("SALDO AMED", val_amed, 5, card_money)]

        for lab, val, col, fmt in kpis:
            sheet_dash.write(3, col, lab, card_label)
            sheet_dash.write(4, col, val, fmt)

        # Resumo por Aliado
        sheet_dash.write('B7', 'SALDO FINANCEIRO POR ALIADO', workbook.add_format({'bold': True}))
        resumo = df.groupby('Aliado')['$ VALOR - AMED'].sum().reset_index().sort_values('$ VALOR - AMED', ascending=False)
        
        sheet_dash.write(8, 1, 'ALIADO', workbook.add_format({'bold':True, 'border':1}))
        sheet_dash.write(8, 2, 'TOTAL (R$)', workbook.add_format({'bold':True, 'border':1}))
        
        for i, r in resumo.iterrows():
            sheet_dash.write(9+i, 1, r['Aliado'], workbook.add_format({'border':1}))
            sheet_dash.write(9+i, 2, r['$ VALOR - AMED'], workbook.add_format({'border':1, 'num_format': 'R$ #,##0.00'}))

        # --- FORMATAÇÃO DOS DADOS ---
        sheet_data.freeze_panes(1, 0)
        
        # Colunas financeiras: Formatação PT-BR
        money_fmt_br = workbook.add_format({'num_format': '#,##0.00'})
        cols_financeiras = ['$ VALOR - AMED', '$ VALOR UNIT', '$ SALDO X QTDE']
        
        for col_name in cols_financeiras:
            if col_name in df.columns:
                idx = df.columns.get_loc(col_name)
                sheet_data.set_column(idx, idx, 18, money_fmt_br)

        # Auto-ajuste e Cores
        sheet_dash.set_column('B:B', 40); sheet_dash.set_column('C:F', 20)
        sheet_dash.hide_gridlines(2)