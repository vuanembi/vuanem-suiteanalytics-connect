from typing import Any
import os

import jaydebeapi

ACCOUNT_ID = 4975572

ROWS_PER_FETCH = 50000


def _get_connection(data_source: str, role_id: int, user: str, pwd: str):
    def _get() -> jaydebeapi.Connection:
        return jaydebeapi.connect(
            "com.netsuite.jdbc.openaccess.OpenAccessDriver",
            (
                f"jdbc:ns://{ACCOUNT_ID}.connect.api.netsuite.com:1708;"
                f"ServerDataSource={data_source}.com;"
                "Encrypted=1;"
                f"CustomProperties=(AccountID={ACCOUNT_ID};RoleID={role_id})"
            ),
            {
                "user": user,
                "password": pwd,
            },
            "NQjc.jar",
        )

    return _get


netsuite_connection = _get_connection(
    "NetSuite",
    3,
    os.getenv("NS_UID", ""),
    os.getenv("NS_PWD", ""),
)
netsuite2_connection = _get_connection(
    "NetSuite2",
    1022,
    os.getenv("NS_UID2", ""),
    os.getenv("NS_PWD2", ""),
)


def get(connection: jaydebeapi.Connection, query: str) -> list[dict[str, Any]]:
    def _fetch(cursor: jaydebeapi.Cursor) -> list[tuple]:
        results = cursor.fetchmany(ROWS_PER_FETCH)
        return [] if not results else results + _fetch(cursor)

    with connection.cursor() as cursor:
        cursor.execute(query)
        keys = [key[0] for key in cursor.description]
        values = _fetch(cursor)
        return [dict(zip(keys, value)) for value in values]
