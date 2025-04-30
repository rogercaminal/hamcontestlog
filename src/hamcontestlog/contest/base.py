"""Base class for contests"""
from abc import ABC
from abc import abstractmethod
from typing import Dict, List, Optional
import os
import functools
from datetime import date

import duckdb
from duckdb import DuckDBPyConnection
from duckdb import IOException
import pandas as pd

from hamcontestlog.log.base import LogBase
from hamcontestlog.log.online import LogOnline
from hamcontestlog.rbn.rbn import ReverseBeaconReader



def with_write_access(method):
    """Decorator to temporarily switch to write mode for a method"""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        # Close the current read-only connection
        if self.con:
            self.con.close()
        # Open a new connection in write mode
        self.con = duckdb.connect(self.storage_path, read_only=False)
        try:
            # Execute the method
            result = method(self, *args, **kwargs)
        finally:
            # Close write connection
            self.con.close()
            # Reopen in read-only mode
            self.con = duckdb.connect(self.storage_path, read_only=True)
        return result
    return wrapper


class ContestBase(ABC):
    url_contest_participants: str
    url_contest_log: str

    def __init__(self, storage_path: str):
        self.storage_path = storage_path
        try:
            self.con: DuckDBPyConnection = duckdb.connect(self.storage_path, read_only=True)
        except IOException:
            self.con: DuckDBPyConnection = duckdb.connect(self.storage_path, read_only=False)

    def __del__(self):
        self.con.close()

    def add_log(self, schema: str, log: LogBase):
        self.con.register("log", log.log)
        # Create the target table if it doesn’t exist
        self.con.execute(f"""
            -- Create the table if it doesn't exist
            CREATE TABLE IF NOT EXISTS {schema}.raw_logs AS 
            SELECT * FROM log WHERE FALSE;

            -- Insert only new rows by avoiding duplicates
            INSERT INTO {schema}.raw_logs
            SELECT * FROM log
            WHERE id NOT IN (SELECT id FROM {schema}.raw_logs);
        """)

    def add_rbn(self, schema: str, rbn: ReverseBeaconReader, calls: Optional[List[str]] = None):
        data = rbn.data if not calls else rbn.data.query(f"dx.isin({calls})")
        self.con.register("rbn", data)
        # Create the target table if it doesn’t exist
        self.con.execute(f"""
            -- Create the table if it doesn't exist
            CREATE TABLE IF NOT EXISTS {schema}.raw_rbn AS 
            SELECT * FROM rbn WHERE FALSE;

            -- Insert only new rows by avoiding duplicates
            INSERT INTO {schema}.raw_rbn
            SELECT * FROM rbn
            WHERE id NOT IN (SELECT id FROM {schema}.raw_rbn);
        """)

    @with_write_access
    def add_online_logs(self, year: int, mode: str, calls: Optional[List[str]] = None):
        # Create schema for mode and year
        schema = f"{mode.lower()}{year}"
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        log_files_online = self.list_cabrillo_files(year=year, mode=mode)
        if calls:
            log_files_online = {call: log_files_online[call.upper()] for call in calls}

        # Loop over logs
        for i, log_file_url in enumerate(log_files_online.values()):
            log_url = os.path.join(self.url_contest_log.format(year=year, mode=mode.lower(), call_hash=log_file_url))
            print(log_url)
            log = LogOnline(path=log_url)
            self.add_log(schema=schema, log=log)
            if i > 10:
                break


    @with_write_access
    def add_online_rbn(self):
        years = [int(schema[0].replace("cw", "")) for schema in self.con.execute("SELECT schema_name FROM information_schema.schemata").fetchall() if schema[0].startswith("cw")]
        for year in years:
            dates = [d[0] for d in self.con.execute(f"select distinct cast (datetime as date) as dates from cw{year}.raw_logs").fetchall()]
            calls = [c[0] for c in self.con.execute(f"select distinct mycall from cw{year}.raw_logs").fetchall()]
            for d in dates:
                schema = f"cw{d.year}"
                self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                rbn = ReverseBeaconReader(date=d)
                self.add_rbn(schema=schema, rbn=rbn, calls=calls)


    @classmethod
    @abstractmethod
    def list_cabrillo_files(cls, year: int, mode: str) -> Dict[str, str]:
        ...

    def query(self, query: str) -> pd.DataFrame:
        return self.con.query(query).fetchdf()

    def list_tables(self) -> List[str]:
        return [t[0] for t in self.con.query("select table_schema || '.' || table_name from information_schema.tables").fetchall()]



