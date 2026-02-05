"""Snowpipe Streaming client for real-time data ingestion."""

import json
import time
from datetime import datetime
from typing import Any, Iterator
from uuid import uuid4

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from .config import SnowflakeConfig


class SnowpipeStreamingClient:
    """Client for streaming data to Snowflake via Snowpipe Streaming."""
    
    def __init__(
        self,
        config: SnowflakeConfig,
        table_name: str,
        database: str | None = None,
        schema: str | None = None,
    ):
        """Initialize streaming client.
        
        Args:
            config: Snowflake configuration.
            table_name: Target table name.
            database: Override database from config.
            schema: Override schema from config.
        """
        self.config = config
        self.table_name = table_name
        self.database = database or config.database
        self.schema = schema or config.schema
        self._connection: SnowflakeConnection | None = None
        
    @property
    def fully_qualified_table(self) -> str:
        """Get fully qualified table name."""
        return f"{self.database}.{self.schema}.{self.table_name}"
    
    def connect(self) -> SnowflakeConnection:
        """Establish connection to Snowflake."""
        if self._connection is None or self._connection.is_closed():
            self._connection = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                password=self.config.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.config.warehouse,
                role=self.config.role,
            )
        return self._connection
    
    def close(self) -> None:
        """Close the connection."""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
            
    def __enter__(self) -> "SnowpipeStreamingClient":
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
    
    def insert_rows(
        self,
        rows: list[dict[str, Any]],
        batch_size: int = 100,
    ) -> int:
        """Insert rows into the target table.
        
        Args:
            rows: List of row dictionaries.
            batch_size: Number of rows per batch.
            
        Returns:
            Number of rows inserted.
        """
        if not rows:
            return 0
            
        conn = self.connect()
        cursor = conn.cursor()
        
        total_inserted = 0
        for batch in self._batch_iterator(rows, batch_size):
            columns = list(batch[0].keys())
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join(columns)
            
            sql = f"""
                INSERT INTO {self.fully_qualified_table} ({column_names})
                VALUES ({placeholders})
            """
            
            values = [tuple(row[col] for col in columns) for row in batch]
            cursor.executemany(sql, values)
            total_inserted += len(batch)
            
        cursor.close()
        return total_inserted
    
    def insert_json(
        self,
        records: list[dict[str, Any]],
        json_column: str = "RAW_JSON",
        include_metadata: bool = True,
    ) -> int:
        """Insert records as JSON into a variant column.
        
        Args:
            records: List of records to insert.
            json_column: Name of the VARIANT column.
            include_metadata: Add UUID and timestamp metadata.
            
        Returns:
            Number of records inserted.
        """
        if not records:
            return 0
            
        rows = []
        for record in records:
            row = {json_column: json.dumps(record)}
            if include_metadata:
                row["UUID"] = str(uuid4())
                row["TS"] = int(time.time() * 1000)
            rows.append(row)
            
        return self.insert_rows(rows)
    
    def merge_rows(
        self,
        rows: list[dict[str, Any]],
        key_columns: list[str],
        update_columns: list[str] | None = None,
    ) -> tuple[int, int]:
        """Merge rows using MERGE statement.
        
        Args:
            rows: List of row dictionaries.
            key_columns: Columns to match on.
            update_columns: Columns to update (default: all non-key columns).
            
        Returns:
            Tuple of (rows_inserted, rows_updated).
        """
        if not rows:
            return 0, 0
            
        conn = self.connect()
        cursor = conn.cursor()
        
        all_columns = list(rows[0].keys())
        if update_columns is None:
            update_columns = [c for c in all_columns if c not in key_columns]
        
        match_condition = " AND ".join(
            f"target.{col} = source.{col}" for col in key_columns
        )
        update_set = ", ".join(
            f"target.{col} = source.{col}" for col in update_columns
        )
        insert_columns = ", ".join(all_columns)
        insert_values = ", ".join(f"source.{col}" for col in all_columns)
        
        source_values = []
        for row in rows:
            values = ", ".join(
                f"'{v}'" if isinstance(v, str) else str(v) if v is not None else "NULL"
                for v in [row[col] for col in all_columns]
            )
            source_values.append(f"SELECT {values}")
        
        source_query = " UNION ALL ".join(source_values)
        
        sql = f"""
            MERGE INTO {self.fully_qualified_table} AS target
            USING ({source_query}) AS source({', '.join(all_columns)})
            ON {match_condition}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
        
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        
        return result[0] if result else 0, 0
    
    @staticmethod
    def _batch_iterator(
        items: list[Any],
        batch_size: int,
    ) -> Iterator[list[Any]]:
        """Yield items in batches."""
        for i in range(0, len(items), batch_size):
            yield items[i:i + batch_size]
