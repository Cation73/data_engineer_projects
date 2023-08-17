import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect



class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def cdm_insert(self,
                    id_first: uuid,
                    id_second: uuid,
                    name_second: str,
                    order_cnt: int,
                    name_table: str,
                    list_columns: list) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f""" 
                    INSERT INTO cdm.{name_table} ({list_columns[0]}, {list_columns[1]}, {list_columns[2]}, {list_columns[3]}) VALUES 
                        (%s, %s, %s, %s)
                    ON CONFLICT ({list_columns[0]}, {list_columns[1]}) DO UPDATE
                    SET {list_columns[3]} = {name_table}.{list_columns[3]} + EXCLUDED.{list_columns[3]};
                    """, (id_first, id_second, name_second, order_cnt))