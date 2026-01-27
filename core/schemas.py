# core/schemas.py
import pandera as pa
from pandera import Column

# Contrato da Planilha ALDREI
SchemaAldrei = pa.DataFrameSchema({
    "SKU": Column(str, coerce=True), 
    "ID": Column(str, coerce=True),
    "Aliado": Column(str, required=False, nullable=True), # Pode vir vazio
    "UF": Column(str, required=False, nullable=True),     # Pode vir vazio
    
    # AQUI ESTAVA O BLOQUEIO: Adicionei nullable=True
    # Isso diz: "Se a coluna existir, tem que ser número. Mas aceito células vazias."
    "APL x DRAFT": Column(float, required=False, coerce=True, nullable=True), 
    "APL x MEDIÇÃO": Column(float, required=False, coerce=True, nullable=True),
})

# Contrato da Planilha MB52
SchemaMB52 = pa.DataFrameSchema({
    "Material": Column(str, required=False),
    "Centro": Column(str, required=False),
})