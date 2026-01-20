import pandas as pd
from .sap_reader import SAPReader

class MMProcessor:
    def __init__(self):
        self.reader = SAPReader()

    def classificar_status(self, row):
        mov = str(row['Tipo de movimento'])
        qtd = row['Qtd_MB51']
        if mov == '261': return 'Aplicação'
        if mov == '311':
            return 'Entrada' if qtd > 0 else 'Estorno' if qtd < 0 else 'Mov. 311'
        return f'Mov. {mov}'

    def processar_aba1(self, df51, df52):
        # Limpeza e Normalização
        df51['Qtd_MB51'] = df51['Quantidade'].apply(self.reader.limpar_sap_qtd)
        df51 = df51.rename(columns={'Material': 'Material (SKU)'})
        df51['Status'] = df51.apply(self.classificar_status, axis=1)

        # Agrupamento
        agrupado = df51.groupby([
            'Centro', 'Material (SKU)', 'Texto breve material', 'Tipo de movimento', 'Status'
        ], as_index=False)['Qtd_MB51'].sum()

        # Merge com MB52
        df52['Quantidade MB52'] = df52['Utilização livre'].apply(self.reader.limpar_sap_qtd)
        df52['Utilização livre_val'] = df52['Val.utiliz.livre'].apply(self.reader.limpar_sap_qtd)
        df52 = df52.rename(columns={'Material': 'Material (SKU)'})

        return pd.merge(
            agrupado, 
            df52[['Centro', 'Material (SKU)', 'Quantidade MB52', 'Utilização livre_val']], 
            on=['Centro', 'Material (SKU)'], 
            how='left'
        ).rename(columns={'Qtd_MB51': 'Quantidade MB51', 'Utilização livre_val': 'Utilização livre'})