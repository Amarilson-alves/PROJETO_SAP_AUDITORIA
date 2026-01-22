# utils/formatting.py
from utils.mapping import OFFSET_DASHBOARD

class ExcelFormatter:
    @staticmethod
    def aplicar_formato(writer, aba1, aba2):
        workbook = writer.book
        sheet2 = writer.sheets['analise auditoria']

        if 'RESULTADO_OPERACIONAL' not in aba2.columns:
            aba2['RESULTADO_OPERACIONAL'] = "N/A"

        # Estilos
        title_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'font_color': '#1F4E78'})
        metric_name_fmt = workbook.add_format({'bg_color': '#F2F2F2', 'border': 1})
        metric_val_fmt = workbook.add_format({'bold': True, 'align': 'center', 'border': 1})
        red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        yellow_fmt = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})

        # Dashboard
        sheet2.write('A1', 'RESUMO EXECUTIVO DE AUDITORIA AMED', title_fmt)
        
        series_res = aba2['RESULTADO_OPERACIONAL'].astype(str)
        metrics = [
            ('Total Auditado', len(aba2)),
            ('Conformados (OK)', len(aba2[series_res.str.contains('OK', na=False)])),
            ('Pendentes', len(aba2[series_res.str.contains('PENDENTE', na=False)])),
            ('Divergências', len(aba2[series_res.str.contains('DIVERGÊNCIA|ESTORNO', na=False)]))
        ]

        for i, (name, val) in enumerate(metrics):
            sheet2.write(i + 2, 0, name, metric_name_fmt)
            sheet2.write(i + 2, 1, val, metric_val_fmt)

        # Formatação Condicional Dinâmica
        col_idx = aba2.columns.get_loc('RESULTADO_OPERACIONAL')
        col_letter = chr(65 + col_idx) if col_idx < 26 else f"A{chr(65 + (col_idx - 26))}"
        range_dados = (OFFSET_DASHBOARD, 0, 100000, len(aba2.columns)-1)

        sheet2.conditional_format(*range_dados, {
            'type': 'formula',
            'criteria': f'=SEARCH("PENDENTE", ${col_letter}{OFFSET_DASHBOARD+1})',
            'format': yellow_fmt
        })
        sheet2.conditional_format(*range_dados, {
            'type': 'formula',
            'criteria': f'=SEARCH("DIVERGÊNCIA", ${col_letter}{OFFSET_DASHBOARD+1})',
            'format': red_fmt
        })
        sheet2.conditional_format(*range_dados, {
            'type': 'formula',
            'criteria': f'=SEARCH("OK", ${col_letter}{OFFSET_DASHBOARD+1})',
            'format': green_fmt
        })