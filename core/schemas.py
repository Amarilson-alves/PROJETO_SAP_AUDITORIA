# core/schemas.py
import pandera as pa
from pandera import Column, Check

# Contrato: O arquivo Aldrei OBRIGATORIAMENTE tem que ter essas colunas
SchemaAldrei = pa.DataFrameSchema({
    "SKU": Column(str, coerce=True), # Tem que ser texto
    "ID": Column(str, coerce=True),
    "Aliado": Column(str),
    "APL x DRAFT": Column(float, required=False, coerce=True), # Pode não existir, mas se existir é número
    "APL x MEDIÇÃO": Column(float, required=False, coerce=True),
})

# Contrato: O arquivo MB52 tem que ter valores positivos ou zero nas qtds (exemplo)
SchemaMB52 = pa.DataFrameSchema({
    "Utilização livre": Column(float, Check.greater_than_or_equal_to(0), coerce=True),
})