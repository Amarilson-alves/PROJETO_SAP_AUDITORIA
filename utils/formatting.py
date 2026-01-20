class ExcelFormatter:
    @staticmethod
    def aplicar_formato(writer, aba1, aba2):
        workbook = writer.book
        sheet1 = writer.sheets['analise MB']
        sheet2 = writer.sheets['analise auditoria']

        # Formatos
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
        red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})

        # Aplicar Filtros
        sheet1.autofilter(0, 0, len(aba1), len(aba1.columns) - 1)
        sheet2.autofilter(0, 0, len(aba2), len(aba2.columns) - 1)

        # Formatação Condicional na Aba Auditoria (Exemplo)
        sheet2.conditional_format(1, 0, len(aba2), 20, {
            'type': 'formula',
            'criteria': '=$Q2="FALTA APLICAR"', # Supondo que Status é coluna Q
            'format': red_fmt
        })