class ServiceConfigKafka:
    """
    Class that defines configuration for Aiven's Postgres service
    """

    def __init__(self, service_name, username, password, port, filename_access_key, filename_access_cert,
                 filename_ca_cert):
        self.m_serviceName = service_name
        self.m_userName = username
        self.m_password = password
        self.m_port = port
        self.m_filenameAccessKey = filename_access_key
        self.m_filenameAccessCertificate = filename_access_cert
        self.m_filenameCaCertificate = filename_ca_cert
        self.m_hostname = None

    def set_hostname(self, aiven_project_name):
        """
        The service's hostname is a combination of the service name and the project name.
        """
        self.m_hostname = self.m_serviceName + '-' + aiven_project_name + '.aivencloud.com'
