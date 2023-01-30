import sys
import snowflake.connector as sf
from awsglue.utils import getResolvedOptions


class tech_func:
    def __init__(self):
        pass

    @staticmethod
    def sf_connection(connection, query):
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            cursor.close()
        except Exception as e:
            print(e)


def main_load_dtm():
    args = getResolvedOptions(sys.argv, ['ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])

    conn = sf.connect(
        user=args['USERNAME'],
        password=args['PASSWORD'],
        account=args['ACCOUNT'],
        warehouse=args['WAREHOUSE'],
        database=args['DB'],
        schema=args['SCHEMA']
    )

    # Logged stage
    job_id = [ '2.1.WYM_PLEC', '2.2.WYM_KATEGORIA_DROGI', '2.3.WYM_ODCINEK_DROGI', '2.4.WYM_KLIENT', '2.5.WYM_TYP_POJAZDU', '2.6.WYM_TYP_PRZEJAZDU','2.8.FAKT_ZAREJESTROWANY_PRZEJAZD'   ]
    jobs_name = ['WYM_PLEC', 'WYM_KATEGORIA_DROGI', 'WYM_ODCINEK_DROGI', 'WYM_KLIENT', 'WYM_TYP_POJAZDU', 'WYM_TYP_PRZEJAZDU','FAKT_ZAREJESTROWANY_PRZEJAZD']

    sql = "call DWH_TRIP_DATA.META_DATA.META_DATA_START_PROCESS('{}','{}')".format(job_id[0], jobs_name[0])
    tech_func.sf_connection(conn, sql)
    
    sql = "call DWH_TRIP_DATA.DATA_MART.WYM_PLEC('{}')".format(job_id[0])
    tech_func.sf_connection(conn, sql)

    sql = "call DWH_TRIP_DATA.META_DATA.META_DATA_END_PROCESS('{}','{}')".format(job_id[0], jobs_name[0])
    tech_func.sf_connection(conn, sql)

    sql = "call DWH_TRIP_DATA.META_DATA.META_DATA_START_PROCESS('{}','{}')".format(job_id[1], jobs_name[1])
    tech_func.sf_connection(conn, sql)

    sql = "call DWH_TRIP_DATA.DATA_MART.WYM_KATEGORIA_DROGI('{}')".format(job_id[1])
    tech_func.sf_connection(conn, sql)

    sql = "call DWH_TRIP_DATA.META_DATA.META_DATA_END_PROCESS('{}','{}')".format(job_id[1], jobs_name[1])
    tech_func.sf_connection(conn, sql)

main_load_dtm()