import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect



class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_insert(self,
                    h_pk: uuid,
                    id: str,
                    load_dt: datetime,
                    load_src: str,
                    name_table: str,
                    list_columns: list) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f""" 
                    INSERT INTO dds.{name_table} ({list_columns[0]}, {list_columns[1]}, {list_columns[2]}, {list_columns[3]}) VALUES 
                        (%s, %s, %s, %s)
                    ON CONFLICT ({list_columns[0]}) DO UPDATE
                    SET  {list_columns[1]} = EXCLUDED.{list_columns[1]}, {list_columns[2]} = EXCLUDED.{list_columns[2]},
                           {list_columns[3]} = EXCLUDED.{list_columns[3]};
                    """, (h_pk, id, load_dt, load_src)) 

    def h_order_insert(self,
                    h_order_pk: uuid,
                    order_id: str,
                    order_dt: datetime,
                    load_dt: datetime,
                    load_src: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """ 
                    INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src) VALUES 
                        (%s, %s, %s, %s, %s)
                    ON CONFLICT (h_order_pk) DO UPDATE
                    SET order_id = EXCLUDED.order_id, order_dt = EXCLUDED.order_dt,
                            load_dt = EXCLUDED.load_dt;
                    """,
                    (h_order_pk, order_id, order_dt, load_dt, load_src))

    def l_insert(self,
                    h_t1_t2_pk: uuid,
                    h_t1_pk: uuid,
                    h_t2_pk: uuid,
                    load_dt: datetime,
                    load_src: str,
                    name_table: str,
                    list_columns: list) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f""" 
                    INSERT INTO dds.{name_table} ({list_columns[0]}, {list_columns[1]}, {list_columns[2]}, {list_columns[3]}, {list_columns[4]}) VALUES 
                        (%s, %s, %s, %s, %s)
                    ON CONFLICT ({list_columns[0]}) DO UPDATE
                    SET {list_columns[1]} = EXCLUDED.{list_columns[1]}, {list_columns[2]} = EXCLUDED.{list_columns[2]},
                            {list_columns[3]} = EXCLUDED.{list_columns[3]}, {list_columns[4]} = EXCLUDED.{list_columns[4]};
                    """, (h_t1_t2_pk, h_t1_pk, h_t2_pk, load_dt, load_src))

    def s_res_product_status_insert(self,
                            h_t1_t2_pk: uuid,
                            h_t1_pk: uuid,
                            name: str,
                            load_dt: datetime,
                            load_src: str,
                            name_table: str,
                            list_columns: list) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f""" 
                    INSERT INTO dds.{name_table} ({list_columns[0]}, {list_columns[1]}, {list_columns[2]}, {list_columns[3]}, {list_columns[4]}) VALUES 
                        (%s, %s, %s, %s, %s)
                    ON CONFLICT ({list_columns[0]}) DO UPDATE
                    SET {list_columns[1]} = EXCLUDED.{list_columns[1]}, {list_columns[2]} = EXCLUDED.{list_columns[2]},
                            {list_columns[3]} = EXCLUDED.{list_columns[3]}, {list_columns[4]} = EXCLUDED.{list_columns[4]};
                    """, (h_t1_t2_pk, h_t1_pk, name, load_dt, load_src))
    
    def s_user_insert(self,
                            h_t1_t2_pk: uuid,
                            h_t1_pk: uuid,
                            name: str,
                            login: str,
                            load_dt: datetime,
                            load_src: str,
                            name_table: str,
                            list_columns: list) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f""" 
                    INSERT INTO dds.{name_table} ({list_columns[0]}, {list_columns[1]}, {list_columns[2]}, {list_columns[3]}, {list_columns[4]}, {list_columns[5]}) VALUES 
                        (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT ({list_columns[0]}) DO UPDATE
                    SET {list_columns[1]} = EXCLUDED.{list_columns[1]}, {list_columns[2]} = EXCLUDED.{list_columns[2]},
                            {list_columns[3]} = EXCLUDED.{list_columns[3]}, {list_columns[4]} = EXCLUDED.{list_columns[4]},
                            {list_columns[5]} = EXCLUDED.{list_columns[5]};
                    """, (h_t1_t2_pk, h_t1_pk, name, login, load_dt, load_src))

    def s_cost_insert(self,
                            h_t1_t2_pk: uuid,
                            h_t1_pk: uuid,
                            cost: float,
                            payment: float,
                            load_dt: datetime,
                            load_src: str,
                            name_table: str,
                            list_columns: list) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f""" 
                    INSERT INTO dds.{name_table} ({list_columns[0]}, {list_columns[1]}, {list_columns[2]}, {list_columns[3]}, {list_columns[4]}, {list_columns[5]}) VALUES 
                        (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT ({list_columns[0]}) DO UPDATE
                    SET {list_columns[1]} = EXCLUDED.{list_columns[1]}, {list_columns[2]} = EXCLUDED.{list_columns[2]},
                            {list_columns[3]} = EXCLUDED.{list_columns[3]}, {list_columns[4]} = EXCLUDED.{list_columns[4]},
                            {list_columns[5]} = EXCLUDED.{list_columns[5]};
                    """, (h_t1_t2_pk, h_t1_pk, cost, payment, load_dt, load_src))
