class ExcelFormatter:
    @staticmethod
    def aplicar_formato(writer, aba1, aba2):
        OFFSET_DASHBOARD = 8 
        workbook = writer.book
        sheet2 = writer.sheets['analise auditoria']

        # --- SOLUÇÃO 2 & 3: GARANTIA DE CONTRATO ---
        # Se por algum motivo a coluna não foi criada, garantimos aqui para não quebrar o código
        if 'RESULTADO_OPERACIONAL' not in aba2.columns:
            aba2['RESULTADO_OPERACIONAL'] = "N/A"

        # --- ESTILOS ---
        title_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'font_color': '#1F4E78'})
        metric_name_fmt = workbook.add_format({'bg_color': '#F2F2F2', 'border': 1})
        metric_val_fmt = workbook.add_format({'bold': True, 'align': 'center', 'border': 1})
        
        red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        yellow_fmt = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})

        # --- DASHBOARD COM ACESSO SEGURO (SOLUÇÃO 1) ---
        sheet2.write('A1', 'RESUMO EXECUTIVO DE AUDITORIA AMED', title_fmt)
        
        total_skus = len(aba2)
        
        # Uso do astype(str) evita erros se houver valores inesperados na coluna
        series_res = aba2['RESULTADO_OPERACIONAL'].astype(str)
        
        ok_count = len(aba2[series_res.str.contains('OK', na=False)])
        pendente_count = len(aba2[series_res.str.contains('PENDENTE', na=False)])
        divergente_count = len(aba2[series_res.str.contains('DIVERGÊNCIA|ESTORNO', na=False)])

        metrics = [
            ('Total de Registros Auditados', total_skus),
            ('Aplicações Conformadas (OK)', ok_count),
            ('Aplicações Pendentes (Ação Necessária)', pendente_count),
            ('Divergências / Estornos Identificados', divergente_count)
        ]

        for i, (name, val) in enumerate(metrics):
            sheet2.write(i + 2, 0, name, metric_name_fmt)
            sheet2.write(i + 2, 1, val, metric_val_fmt)

        # --- LOCALIZAÇÃO DINÂMICA DA COLUNA ---
        col_idx = aba2.columns.get_loc('RESULTADO_OPERACIONAL')
        # Lógica para converter índice em letra (Suporta A-Z e AA-AZ)
        if col_idx < 26:
            col_letter = chr(65 + col_idx)
        else:
            col_letter = f"{chr(64 + col_idx // 26)}{chr(65 + col_idx % 26)}"

        # --- FORMATAÇÃO CONDICIONAL ---
        range_dados = (OFFSET_DASHBOARD, 0, 100000, len(aba2.columns)-1)
        
        # Aplicamos as cores baseadas na letra da coluna encontrada dinamicamente
        formatos = [
            ("PENDENTE", yellow_fmt),
            ("DIVERGÊNCIA", red_fmt),
            ("ESTORNO", red_fmt),
            ("OK", green_fmt)
        ]

        for crit, fmt in formatos:
            sheet2.conditional_format(*range_dados, {
                'type': 'formula',
                'criteria': f'=SEARCH("{crit}", ${col_letter}{OFFSET_DASHBOARD+1})',
                'format': fmt
            })
        
        sheet2.autofilter(OFFSET_DASHBOARD, 0, OFFSET_DASHBOARD + len(aba2), len(aba2.columns) - 1)