"""
Implementation of a general function to connect to DB and extract structural
information, using a custom SQL expression.

@author: Dr. Freddy A. Bernal
"""

import os
import re
from functools import wraps
from typing import Iterable

import oracledb
import pandas as pd
from dotenv import load_dotenv
from rdkit import Chem
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
    if len(data) != 1:
        first = data[0]
    else:
        first = data
    # Transform structural info into str
    converted = first[:-1] + tuple([str(first[-1])])

    return converted


def organize_results(data: list, sql: str) -> pd.DataFrame:
    """Transforms query results from list to dataframe. It automatically assigns column
    names using the corresponding DB fields and adds RDKit.Mol objects.

    Args:
        data (list): information from query.
        sql (str): SQL expression used for query.

    Returns:
        pd.DataFrame: results including rdkit mol object.
    """
    cols = get_field_names(sql)
    result = pd.DataFrame(data, columns=cols)
    result["ROMol"] = result["MOL_CTFILE"].apply(Chem.MolFromMolBlock)
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
