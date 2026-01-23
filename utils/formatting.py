# utils/formatting.py
import pandas as pd

class ExcelFormatter:
    @staticmethod
    def aplicar_formato(writer, df):
        workbook = writer.book
        sheet_dash = workbook.add_worksheet('Painel')
        sheet_data = writer.sheets['analise auditoria']
        sheet_dash.activate()

        # --- ESTILOS PROFISSIONAIS ---
        # Cores padrão corporativo (Azul Petróleo e Cinza)
        header_style = workbook.add_format({'bold': True, 'font_size': 16, 'font_color': 'white', 'bg_color': '#1F4E78', 'align': 'center', 'valign': 'vcenter'})
        card_title_style = workbook.add_format({'bold': True, 'font_size': 10, 'font_color': '#1F4E78', 'bg_color': '#E7E6E6', 'border': 1, 'align': 'center'})
        card_value_style = workbook.add_format({'bold': True, 'font_size': 14, 'font_color': '#333333', 'bg_color': 'white', 'border': 1, 'align': 'center'})
        money_style = workbook.add_format({'bold': True, 'font_size': 14, 'font_color': '#006100', 'bg_color': '#C6EFCE', 'border': 1, 'align': 'center', 'num_format': 'R$ #,##0.00'})
        
        table_header = workbook.add_format({'bold': True, 'bg_color': '#1F4E78', 'font_color': 'white', 'border': 1})
        table_row = workbook.add_format({'border': 1})
        table_money = workbook.add_format({'border': 1, 'num_format': 'R$ #,##0.00'})

        # --- 1. CABEÇALHO GERAL ---
        sheet_dash.merge_range('B2:F3', 'PAINEL EXECUTIVO - AUDITORIA AMED', header_style)

        # --- 2. CÁLCULO DE KPIS ---
        # Garante que estamos somando números e não strings
        val_amed = pd.to_numeric(df['$ VALOR - AMED'], errors='coerce').sum()
        total_regs = len(df)
        
        res = df['RESULTADO_OPERACIONAL'].astype(str)
        ok = len(df[res.str.contains('OK', na=False)])
        pend = len(df[res.str.contains('PENDENTE', na=False)])
        div = len(df[res.str.contains('DIVERGÊNCIA', na=False)])

        # --- 3. CARDS DE INDICADORES ---
        # Layout: Label na linha 5, Valor na linha 6
        kpis = [
            ("TOTAL REGISTROS", total_regs, 1, card_value_style),
            ("CONFORMIDADE (OK)", ok, 2, card_value_style),
            ("PENDÊNCIAS", pend, 3, card_value_style),
            ("DIVERGÊNCIAS", div, 4, card_value_style),
            ("SALDO FINANCEIRO", val_amed, 5, money_style)
        ]

        for label, val, col, fmt in kpis:
            sheet_dash.write(5, col, label, card_title_style)
            sheet_dash.write(6, col, val, fmt)

        # --- 4. TABELA: SALDO POR ALIADO ---
        sheet_dash.write('B9', 'VISÃO FINANCEIRA POR ALIADO', workbook.add_format({'bold': True, 'font_size': 12}))
        
        # Agrupamento seguro
        df['$ VALOR - AMED'] = pd.to_numeric(df['$ VALOR - AMED'], errors='coerce').fillna(0)
        resumo = df.groupby('Aliado')['$ VALOR - AMED'].sum().reset_index()
        resumo = resumo.sort_values(by='$ VALOR - AMED', ascending=False)

        # Cabeçalho da tabela
        start_row = 10
        sheet_dash.write(start_row, 1, 'ALIADO / PARCEIRO', table_header)
        sheet_dash.write(start_row, 2, 'SALDO ACUMULADO (R$)', table_header)

        # Corpo da tabela
        for i, r in resumo.iterrows():
            curr = start_row + 1 + i
            sheet_dash.write(curr, 1, r['Aliado'], table_row)
            sheet_dash.write(curr, 2, r['$ VALOR - AMED'], table_money)

        # Ajuste de colunas
        sheet_dash.set_column('B:B', 40)
        sheet_dash.set_column('C:F', 20)
        sheet_dash.hide_gridlines(2)

        # --- 5. FORMATAÇÃO DA ABA DE DADOS ---
        # Congela cabeçalho
        sheet_data.freeze_panes(1, 0)
        
        # Formatação Condicional
        col_res_idx = df.columns.get_loc('RESULTADO_OPERACIONAL')
        letra_res = chr(65 + col_res_idx) if col_res_idx < 26 else f"A{chr(65 + (col_res_idx - 26))}"
        
        bg_green = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        bg_yellow = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})
        bg_red = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})

        rng = (1, 0, 100000, len(df.columns)-1)
        
        sheet_data.conditional_format(*rng, {'type': 'formula', 'criteria': f'=SEARCH("OK", ${letra_res}2)', 'format': bg_green})
        sheet_data.conditional_format(*rng, {'type': 'formula', 'criteria': f'=SEARCH("PENDENTE", ${letra_res}2)', 'format': bg_yellow})
        sheet_data.conditional_format(*rng, {'type': 'formula', 'criteria': f'=SEARCH("DIVERGÊNCIA", ${letra_res}2)', 'format': bg_red})