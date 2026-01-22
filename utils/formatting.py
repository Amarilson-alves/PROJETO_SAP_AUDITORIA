import pandas as pd
from utils.mapping import OFFSET_DASHBOARD

class ExcelFormatter:
    @staticmethod
    def aplicar_formato(writer, aba1, aba2):
        workbook = writer.book
        # Acessando as abas
        sheet1 = writer.sheets['analise MB']
        sheet2 = writer.sheets['analise auditoria']

        # 1. GARANTIA DE CONTRATO
        if 'RESULTADO_OPERACIONAL' not in aba2.columns:
            aba2['RESULTADO_OPERACIONAL'] = "N/A"

        # 2. DEFINIÇÃO DE ESTILOS PROFISSIONAIS
        title_fmt = workbook.add_format({'bold': True, 'font_size': 14, 'font_color': '#1F4E78', 'valign': 'vcenter'})
        metric_name_fmt = workbook.add_format({'bg_color': '#F2F2F2', 'border': 1, 'bold': True})
        metric_val_fmt = workbook.add_format({'bold': True, 'align': 'center', 'border': 1, 'font_color': '#1F4E78'})
        
        # Estilos para Formatação Condicional
        red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        yellow_fmt = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})
        header_table_fmt = workbook.add_format({'bold': True, 'bg_color': '#1F4E78', 'font_color': 'white', 'border': 1})

        # 3. CONSTRUÇÃO DO DASHBOARD (Preenchendo as linhas vazias)
        sheet2.write('A1', 'RESUMO EXECUTIVO DE AUDITORIA AMED', title_fmt)
        
        # Cálculos Defensivos
        series_res = aba2['RESULTADO_OPERACIONAL'].astype(str)
        total_skus = len(aba2)
        ok_count = len(aba2[series_res.str.contains('OK', na=False)])
        pendente_count = len(aba2[series_res.str.contains('PENDENTE', na=False)])
        divergente_count = len(aba2[series_res.str.contains('DIVERGÊNCIA|ESTORNO', na=False)])

        metrics = [
            ('Total de Registros Auditados', total_skus),
            ('Aplicações Conformadas (OK)', ok_count),
            ('Pendências / Ação de Campo', pendente_count),
            ('Divergências / Erros de Saldo', divergente_count)
        ]

        # Escrevendo as métricas no topo (Linhas 3 a 6)
        for i, (name, val) in enumerate(metrics):
            sheet2.write(i + 2, 0, name, metric_name_fmt)
            sheet2.write(i + 2, 1, val, metric_val_fmt)

        # 4. FORMATAÇÃO DINÂMICA DA TABELA DE DADOS
        # Localiza a letra da coluna RESULTADO_OPERACIONAL
        col_idx = aba2.columns.get_loc('RESULTADO_OPERACIONAL')
        col_letter = chr(65 + col_idx) if col_idx < 26 else f"A{chr(65 + (col_idx - 26))}"
        
        # Range que começa após o OFFSET
        range_dados = (OFFSET_DASHBOARD, 0, 100000, len(aba2.columns) - 1)

        # Aplicando Cores nas Linhas conforme o Status
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

        # 5. PERFUMARIA E UX (O toque final)
        
        # Congelar Dashboard e Cabeçalho (Fica fixo ao rolar)
        sheet2.freeze_panes(OFFSET_DASHBOARD + 1, 0)
        
        # Ajuste de largura das colunas para leitura clara
        sheet2.set_column('A:A', 25)  # SKU / Primeiro campo
        sheet2.set_column('B:H', 18)  # Campos intermediários
        sheet2.set_column(col_idx, col_idx, 35)  # Coluna de Resultado (Mais larga)
        
        # Ativa o filtro automático na linha do cabeçalho
        sheet2.autofilter(OFFSET_DASHBOARD, 0, OFFSET_DASHBOARD + len(aba2), len(aba2.columns) - 1)
        
        # Limpa as linhas de grade para um visual "Clean"
        sheet2.hide_gridlines(2)

        # Formatação de cabeçalho da tabela de dados (Linha do OFFSET)
        for col_num, value in enumerate(aba2.columns.values):
            sheet2.write(OFFSET_DASHBOARD, col_num, value, header_table_fmt)