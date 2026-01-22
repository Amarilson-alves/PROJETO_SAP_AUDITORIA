class ExcelFormatter:
    @staticmethod
    def aplicar_formato(writer, aba1, aba2):
        workbook = writer.book
        sheet1 = writer.sheets['analise MB']
        sheet2 = writer.sheets['analise auditoria']

        # --- AJUSTE DE SEGURANÇA (PONTO 1) ---
        # Garante que a coluna exista para evitar o erro de KeyError no Dashboard
        if 'RESULTADO_OPERACIONAL' not in aba2.columns:
            aba2['RESULTADO_OPERACIONAL'] = "N/A"

        # --- DEFINIÇÃO DE ESTILOS ---
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#1F4E78', 'font_color': 'white', 'border': 1})
        title_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'font_color': '#1F4E78'})
        metric_name_fmt = workbook.add_format({'bg_color': '#F2F2F2', 'border': 1})
        metric_val_fmt = workbook.add_format({'bold': True, 'align': 'center', 'border': 1})
        
        # Cores para o Resultado Operacional
        red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        yellow_fmt = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})

        # --- DASHBOARD DE RESUMO ---
        sheet2.write('A1', 'RESUMO EXECUTIVO DE AUDITORIA AMED', title_fmt)
        
        # Cálculos de métricas (Usando str.contains com proteção na=False)
        total_skus = len(aba2)
        ok_count = len(aba2[aba2['RESULTADO_OPERACIONAL'].str.contains('OK', na=False)])
        pendente_count = len(aba2[aba2['RESULTADO_OPERACIONAL'].str.contains('PENDENTE', na=False)])
        divergente_count = len(aba2[aba2['RESULTADO_OPERACIONAL'].str.contains('DIVERGÊNCIA', na=False)])

        metrics = [
            ('Total de Registros Auditados', total_skus),
            ('Aplicações Conformadas (OK)', ok_count),
            ('Aplicações Pendentes (Material em Stock)', pendente_count),
            ('Divergências de Sobra/Erro', divergente_count)
        ]

        row = 2
        for name, val in metrics:
            sheet2.write(row, 0, name, metric_name_fmt)
            sheet2.write(row, 1, val, metric_val_fmt)
            row += 1

        # --- FORMATAÇÃO DINÂMICA (A partir da linha 9) ---
        # Identificar dinamicamente qual a letra da coluna 'RESULTADO_OPERACIONAL'
        # Isso evita erro se você adicionar ou remover colunas no futuro
        col_idx = aba2.columns.get_loc('RESULTADO_OPERACIONAL')
        col_letter = chr(65 + col_idx) # Transforma índice em letra (ex: 18 -> S)

        # Filtro automático para os dados (Linha 9 em diante)
        sheet2.autofilter(8, 0, 8 + len(aba2), len(aba2.columns) - 1)

        # Aplicar cores baseadas no diagnóstico (Ponto 18/S no seu modelo)
        sheet2.conditional_format(8, 0, 50000, len(aba2.columns) - 1, {
            'type': 'formula',
            'criteria': f'=SEARCH("PENDENTE", ${col_letter}9)',
            'format': yellow_fmt
        })
        sheet2.conditional_format(8, 0, 50000, len(aba2.columns) - 1, {
            'type': 'formula',
            'criteria': f'=SEARCH("DIVERGÊNCIA", ${col_letter}9)',
            'format': red_fmt
        })
        sheet2.conditional_format(8, 0, 50000, len(aba2.columns) - 1, {
            'type': 'formula',
            'criteria': f'=SEARCH("OK", ${col_letter}9)',
            'format': green_fmt
        })