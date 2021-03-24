import aiven.client.client as ac


class AivenService:
    """
    Class for Initializing Aiven's Services

    Attributes
    ----------
    m_aiven_client : AivenClient
        class provided by the Aiven Client Python library. Used for configuring Aiven services.
    m_aiven_project_name : str
        The Aiven project name. For demo accounts, this is predetermined by Aiven during sign-up. Seen at the top left corner of https://console.aiven.io.
    m_aiven_email : str
        credentials used for logging into the Aiven service.
    m_aiven_password : KafkaService
        credentials used for logging into the Aiven service.
    m_service_config_kafka : ServiceConfigKafka
        user defined configurations for initialising Aiven's Kafka service.
    m_service_config_postgres : ServiceConfigPostGres
        user defined configurations for initialising Aiven's Postgres service.
    m_token : str
        login token
    m_aiven_handle_kafka : dict
        metadata returned by Aiven when creating the Kafka service. Used for changing Kafka configurations.
    m_aiven_handle_postgres : dict
        metadata returned by Aiven when creating the Postgres service. Used for changing Postgres configurations.

    """


    def __init__(self, aiven_project_name, aiven_email, aiven_password, service_config_kafka, service_config_postgres):
        self.m_aiven_client = ac.AivenClient("https://api.aiven.io")

        self.m_aiven_project_name = aiven_project_name
        self.m_aiven_email = aiven_email
        self.m_aiven_password = aiven_password

        self.m_service_config_kafka = service_config_kafka
        self.m_service_config_kafka.set_hostname(self.m_aiven_project_name)
        self.m_service_config_postgres = service_config_postgres
        self.m_service_config_postgres.set_hostname(self.m_aiven_project_name)

        self.m_token = None
        self.m_aiven_handle_kafka = None
        self.m_aiven_handle_postgres = None
        self.m_kafka_producer = None
        self.m_kafka_consumer = None

    def login(self):
        """
        login routine to Aiven's services.
        """
        self.m_token = self.m_aiven_client.authenticate_user(self.m_aiven_email,self.m_aiven_password)["token"]
        self.m_aiven_client.set_auth_token(self.m_token)

    def kafka_init(self):
        """
        Used for creating a Kafka service in Aiven.
        """
        try:
            self.m_aiven_handle_kafka = self.m_aiven_client.get_service(self.m_aiven_project_name, self.m_service_config_kafka.m_serviceName)
            if not self.m_aiven_handle_kafka["user_config"]["kafka_connect"]:
                self.m_aiven_handle_kafka = self.m_aiven_client.update_service(self.m_aiven_project_name, self.m_service_config_kafka.m_serviceName, user_config={"kafka_connect": True})
        except ac.Error:
            self.m_aiven_handle_kafka = self.m_aiven_client.create_service(self.m_aiven_project_name, self.m_service_config_kafka.m_serviceName, "kafka", "business-4", "google-australia-southeast1", {"kafka_connect": True})

        self.create_kafka_cert_files()

        return self.m_aiven_handle_kafka

    def create_kafka_cert_files(self):
        """
        Download and save keys from the server. Used in KafkaService when creating a producer or a consumer.
        """
        ca = self.m_aiven_client.get_project_ca(self.m_aiven_project_name)
        with open("ca.pem", "w") as cafile:
            cafile.write(ca["certificate"])
        with open("service.cert", "w") as certfile:
            certfile.write(self.m_aiven_handle_kafka["connection_info"]["kafka_access_cert"])
        with open("service.key", "w") as keyfile:
            keyfile.write(self.m_aiven_handle_kafka["connection_info"]["kafka_access_key"])

    def postgres_init(self):
        """
        Used for creating a Postgres service in Aiven.
        """

        try:
            self.m_aiven_handle_postgres = self.m_aiven_client.get_service(self.m_aiven_project_name, self.m_service_config_postgres.m_serviceName)
        except ac.Error:
            self.m_aiven_handle_postgres = self.m_aiven_client.create_service(self.m_aiven_project_name, self.m_service_config_postgres.m_serviceName, "pg", "hobbyist", "google-australia-southeast1")
        return self.m_aiven_handle_postgres
