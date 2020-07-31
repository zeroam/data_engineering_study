import os
import dotenv
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2


def get_redshift_connection(user: str, pw: str):
    host = "grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(
        f"dbname={dbname} user={user} host={host} password={pw} port={port}"
    )
    conn.set_session(readonly=True, autocommit=True)
    return conn


if __name__ == "__main__":
    dotenv.load_dotenv()

    user = os.getenv("REDSHIFT_ID")
    pw = os.getenv("REDSHIFT_PW")
    conn = get_redshift_connection(user, pw)

    sql = \
"""
SELECT
  TO_CHAR(ts, 'yyyy-MM') AS Dateformat,
  COUNT(DISTINCT userid)
FROM adhoc.jayone_test
GROUP BY Dateformat
ORDER BY Dateformat;
"""

    df = sqlio.read_sql_query(sql, conn)
    print(df)
