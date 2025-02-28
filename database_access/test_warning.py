"""
Short test of the search_compounds function.

@author: Dr. Freddy A. Bernal
"""

import os
import pandas as pd
from connect import search_compounds

# SQL expression
sql = """SELECT CM.CM_UNITS.CMU_REGID,QUATTRO_CR.BATCH.BATCH_REGID,
QUATTRO_CR.MOLECULE.MOL_REGID,QUATTRO_CR.MOLECULE.MOL_CTFILE
FROM CM.CM_UNITS,QUATTRO_CR.BATCH,QUATTRO_CR.MOLECULE
WHERE CM.CM_UNITS.CMA_ID = QUATTRO_CR.BATCH.BATCH_ID
AND QUATTRO_CR.BATCH.MOL_COMP_ID = QUATTRO_CR.MOLECULE.MOL_COMP_ID
AND QUATTRO_CR.MOLECULE.MOL_REGID = :mybv"""

path = os.path.abspath(os.getcwd())
# get list of compounds
df = pd.read_excel(os.path.join(path, "test", "test_data_medium.xlsx"))
print(f"Source dataframe shape: {df.shape}")

# query database
result = search_compounds(df["Compound_Id"], sql=sql)
print(f"Query results: {len(result)}")
print(result.iloc[[10, 500, 927]])

# # Transform CT files
# ct_strings = [x[-1] for x in result]
# mols, messages = transform_ct(ct_strings)
# issues = 0
# for i, msn in enumerate(messages):
#     if msn != "":
#         issues += 1
# print(f"Number of warnings: {issues} / {len(messages)}")
