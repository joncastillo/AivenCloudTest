class ServiceConfigPostGres:
    """
    Class that defines configuration for Aiven's Postgres service
    """

    def __init__(self, service_name, username, password, port, database_name):
        self.m_serviceName = service_name
        self.m_username = username
        self.m_password = password
        self.m_port = port
        self.m_databaseName = database_name
        self.m_hostname = None

        self.connection = None

    def set_hostname(self, aiven_project_name):
        """
        The service's hostname is a combination of the service name and the project name.
        """
        self.m_hostname = self.m_serviceName + '-' + aiven_project_name + '.aivencloud.com'
