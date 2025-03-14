"""
Implementation of a general function to connect to DB and extract structural
information, using a custom SQL expression.

@author: Dr. Freddy A. Bernal
"""

import logging
import os
import re
from functools import wraps
from io import StringIO
from typing import Iterable

import oracledb
import pandas as pd
from dotenv import load_dotenv
from rdkit import Chem, rdBase
from tqdm import tqdm

# load keys
load_dotenv()

# get access credentials
DB_URL = os.getenv("DB_URL")
USER = os.getenv("USERNAME")
DB_KEY = os.getenv("DB_KEY")


def connect(func):
    """Decorator to open a secure connection to the COMAS database allowing subsequent
    query. The connection is effectively closed after function execution.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        connection = oracledb.connect(user=USER, password=DB_KEY, dsn=DB_URL)
        cursor = connection.cursor()
        try:
            result = func(cursor, *args, **kwargs)
            return result
        finally:
            connection.close()

    return wrapper


@connect
def search_compounds(cursor, identifiers: Iterable[str], sql: str) -> pd.DataFrame:
    """Utility for compound search on the COMAS DB.

    Args:
        cursor (oracledb.cursor): oracle connection inherited from @connect.
        identifiers (Iterable[str]): any compound identifier (e.g. Compound ID)
        sql (str): SQL expression to effectively query DB.

    Returns:
        pd.DataFrame: results from query.
    """
    result = []
    for id in tqdm(identifiers, desc="Processed"):
        # Execute SQL
        cursor.execute(sql, mybv=id)
        # Fetch result from search
        res = cursor.fetchall()
        # Transform structural info into str (otherwise are kept as Oracle objects)
        converted = first_output_to_str(res)  # before closing connection
        result.append(converted)

    result = organize_results(result, sql)
    return result


def first_output_to_str(data: tuple) -> tuple:
    """Convert CT file from an embedded orable object into a string.

    Args:
        data (tuple): collection of properties retrieved from DB.

    Returns:
        tuple: collection of properties retrieved from DB as str.
    """
    # we take only the first entry (one container ID)
    first = data[0]
    # Transform structural info into str
    converted = first[:-1] + tuple([str(first[-1])])

    return converted


def organize_results(data: list, sql: str) -> pd.DataFrame:
    """Transforms query results from list to dataframe. It automatically assigns column
    names using the corresponding DB fields and adds RDKit.Mol objects and warnings
    catched during ct to mol transformation.

    Args:
        data (list): information from query.
        sql (str): SQL expression used for query.

    Returns:
        pd.DataFrame: results including rdkit mol object and possible warnings.
    """
    cols = get_field_names(sql)
    result = pd.DataFrame(data, columns=cols)
    if "MOL_CTFILE" in cols:
        # result["ROMol"] = result["MOL_CTFILE"].apply(Chem.MolFromMolBlock)
        mols, messages = transform_ct(result["MOL_CTFILE"])
        result["ROMol"] = mols
        result["Warnings"] = messages
        result.drop(columns="MOL_CTFILE", inplace=True)
    return result


def get_field_names(sql: str) -> list:
    """Extract DB field names from SQL expression.

    Args:
        sql (str): SQL expression for query

    Returns:
        list: field names
    """
    # remove line spaces for re to work
    sql_no_blank = sql.replace("\n", "")
    # search fields
    match = re.findall("SELECT (.*?)FROM", sql_no_blank)
    # split fields
    fields = match[0].split(",")
    # retrieve actual field name
    fields = [x.split(".")[-1] for x in fields]
    return fields


def transform_ct(ct_strs: Iterable[str]) -> tuple[list]:
    """Convert ct file content into RDKit mol objects while catching warning messages.
    First transfer logger information from C++ to python as shown in:
    https://greglandrum.github.io/rdkit-blog/posts/2024-02-23-custom-transformations-and-logging.html

    Args:
        ct_strs (Iterable[str]): group of chemical table files as strings.

    Returns:
        tuple: RDKit Mol objects and RDKit warnings as lists.
    """
    # Tell the RDKit's C++ backend to log to use the python logger:
    rdBase.LogToPythonLogger()
    logger = logging.getLogger("rdkit")
    # set the log level for the default log handler (the one which sense output
    # to the console/notebook):
    logger.handlers[0].setLevel(logging.WARN)

    # create a handler that uses the StringIO and set its log level:
    logger_sio = StringIO()
    handler = logging.StreamHandler(logger_sio)
    handler.setLevel(logging.INFO)
    # add the handler to the Python logger:
    logger.addHandler(handler)

    # Iterate over files
    res_mols = []
    messages = []
    for ct in ct_strs:
        mol = Chem.MolFromMolBlock(ct)
        res_mols.append(mol)

        text = logger_sio.getvalue()
        messages.append(text)

        # reset StringIO object
        logger_sio.truncate(0)
        logger_sio.seek(0)

    return res_mols, messages
