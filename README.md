# database_access


This module facilitates querying an oracle database.

&nbsp; 
>[!IMPORTANT]
>For the module to work, it is necessary to have a `.env` file, containing the corresponding URL and personal credentials for access. 

&nbsp; 
&nbsp; 
Here is a minimal example for the .env file.
&nbsp; 
```
DB_URL=<URL_DATABASE>

USERNAME=<USERNAME>

DB_KEY=<PASSWORD>
```
&nbsp; 

## Usage

To query the database for specific compounds, simply use the `search_compounds` function, using a list of compound identifiers (e.g. Compound ID) and a valid SQL expression.

```python
from database_connect import search_compounds

result_query = search_compounds(compounds_ids, sql) 
```
