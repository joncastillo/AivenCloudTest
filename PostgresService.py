import psycopg2

class PostgresService:

    def __init__(self, aiven_service):
        self.m_aiven_service = aiven_service
        self.m_conn = None

    def connect(self):
        host = self.m_aiven_service.m_service_config_postgres.m_hostname
        port = self.m_aiven_service.m_service_config_postgres.m_port
        dbname = self.m_aiven_service.m_service_config_postgres.m_databaseName
        username = self.m_aiven_service.m_service_config_postgres.m_username
        password = self.m_aiven_service.m_service_config_postgres.m_password
        try:
            self.m_conn = psycopg2.connect(f"dbname={dbname} user={username} password={password} host={host} port={str(port)}")
        except Exception as e:
            print("I am unable to connect to the database")
            print(str(e))
            return 1
        return 0

    def disconnect(self):
        self.m_conn.close()
        self.m_conn = None

    # too complicated - not required for this assignment
    #def run_stored_procedure(self, filename, *args):
    #    with self.m_conn.cursor() as cursor:
    #        with open(filename, "r") as stored_proc:
    #            cursor.execute(stored_proc.read(), args)
    #    return 0

    def create_table_if_not_exist(self):
        with self.m_conn.cursor() as cursor:
            cursor.execute("CREATE TABLE IF NOT EXISTS logs "
                           "("
                           "    id SERIAL PRIMARY KEY,"
                           "    datetime VARCHAR,"
                           "    url VARCHAR,"
                           "    status_code VARCHAR,"
                           "    json_filtered_text VARCHAR"
                           ");")
            self.m_conn.commit()
        return 0

    def store_to_database(self, datetime, url, status_code, json_filtered_text):
        with self.m_conn.cursor() as cursor:

            cursor.execute("CREATE TABLE IF NOT EXISTS logs "
                           "("
                           "    id SERIAL PRIMARY KEY,"
                           "    datetime VARCHAR,"
                           "    url VARCHAR,"
                           "    status_code VARCHAR,"
                           "    json_filtered_text VARCHAR"
                           ");")

            cursor.execute(f"INSERT INTO logs(datetime, url, status_code, json_filtered_text) "
                           f"VALUES ('{datetime}', '{url}', '{status_code}', '{json_filtered_text}')")
            self.m_conn.commit()

            print(f"inserting: {datetime} {url} {status_code} {json_filtered_text}")

        return 0
