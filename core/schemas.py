# core/schemas.py
import pandera.pandas as pa
from pandera import Column

# Contrato da Planilha AUDITORIA
SchemaAuditoria = pa.DataFrameSchema({
    "SKU": Column(str, coerce=True), 
    "ID": Column(str, coerce=True),
    "FILA": Column(str, required=False, nullable=True),   # Nova coluna mapeada
    "Aliado": Column(str, required=False, nullable=True), # Pode vir vazio
    "UF": Column(str, required=False, nullable=True),     # Pode vir vazio
    
    # Adicionado nullable=True
    # Isso diz: "Se a coluna existir, tem que ser número. Mas aceito células vazias."
    "APL x DRAFT": Column(float, required=False, coerce=True, nullable=True), 
    "APL x MEDIÇÃO": Column(float, required=False, coerce=True, nullable=True),
})

# Contrato da Planilha MB52
SchemaMB52 = pa.DataFrameSchema({
    "Material": Column(str, required=False),
    "Centro": Column(str, required=False),
})