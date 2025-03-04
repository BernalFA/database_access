"""
Short test of the search_compounds function.

@author: Dr. Freddy A. Bernal
"""

import os
import pandas as pd
from database_access import search_compounds

# SQL expression
sql = """SELECT CM.CM_UNITS.CMU_REGID,QUATTRO_CR.BATCH.BATCH_REGID,
QUATTRO_CR.MOLECULE.MOL_REGID,QUATTRO_CR.MOLECULE.MOL_CTFILE
FROM CM.CM_UNITS,QUATTRO_CR.BATCH,QUATTRO_CR.MOLECULE
WHERE CM.CM_UNITS.CMA_ID = QUATTRO_CR.BATCH.BATCH_ID
AND QUATTRO_CR.BATCH.MOL_COMP_ID = QUATTRO_CR.MOLECULE.MOL_COMP_ID
AND QUATTRO_CR.BATCH.BATCH_REGID = :mybv"""

path = os.path.abspath(os.getcwd())
# get list of compounds
df = pd.read_excel(os.path.join(path, "test", "test_data.xlsx"))
print(f"Source dataframe shape: {df.shape}")

# query database
result = search_compounds(df["Batch ID"], sql=sql)
print(f"Resulting dataframe shape: {result.shape}")
print(result.head(3))
