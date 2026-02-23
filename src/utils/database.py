"""
database utility module for sqlite operations.
"""

import sqlite3
import logging
import os
from typing import List, Tuple, Any, Optional

logger = logging.getLogger('load')

DATABASE_PATH = os.path.join('data', 'job_market_analytics.db')
SCHEMA_PATH = os.path.join('sql', '01_schema.sql')

class DatabaseManager:
    """manages sqlite database connections and operations."""

    def __init__(self, db_path: str = DATABASE_PATH):
        self.db_path = db_path
        self._connection = None

    def _get_connection(self) -> sqlite3.Connection:
        """establishes and returns a database connection."""
        if self._connection is None:
            if self.db_path != ":memory:": # skip creating directory for in-memory db
                os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self._connection = sqlite3.connect(self.db_path)
            self._connection.row_factory = sqlite3.Row  # access columns by name
            logger.info(f"connected to database: {self.db_path}")
        return self._connection

    def close_connection(self) -> None:
        """closes the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info(f"closed database connection: {self.db_path}")

    def execute_query(self, query: str, params: Optional[Tuple] = None, fetch_one: bool = False) -> Optional[List[sqlite3.Row]]:
        """executes a select query and returns results."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if fetch_one:
                return cursor.fetchone()
            return cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"database query error: {e} - query: {query}")
            return None
        finally:
            cursor.close()

    def execute_insert(self, query: str, params: Optional[Tuple] = None, return_id: bool = False) -> Any:
        """executes an insert, update, or delete query and commits changes. returns rowid if return_id is True, else True on success, None on error."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            if return_id:
                return cursor.lastrowid
            return True # indicate success
        except sqlite3.IntegrityError as e:
            logger.warning(f"database integrity error (likely duplicate): {e} - query: {query}")
            conn.rollback()
            return None
        except sqlite3.Error as e:
            logger.error(f"database modification error: {e} - query: {query}")
            conn.rollback()
            return None
        finally:
            cursor.close()

    def setup_database(self) -> None:
        """reads and executes the schema sql file to create tables and indexes."""
        cursor = None  # initialize cursor to none
        try:
            with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.executescript(schema_sql)
            conn.commit()
            logger.info("database schema applied successfully.")
        except FileNotFoundError:
            logger.error(f"schema file not found: {SCHEMA_PATH}")
        except sqlite3.Error as e:
            logger.error(f"error applying database schema: {e}")
        finally:
            if cursor:
                cursor.close()

# global instance to manage the database
_db_manager = None

def get_db_manager() -> DatabaseManager:
    """returns the singleton databasemanager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager
