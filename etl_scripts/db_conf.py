
import sys
import os
# Configuración de ADOMD.NET para Pyadomd
adomd_path='C:\\Program Files\\Microsoft.NET\\ADOMD.NET\\'
folder=os.listdir(adomd_path)
sys.path.append(adomd_path+folder[0])
print("ADOMD config correctly")
from pyadomd import Pyadomd
import os
from dotenv import load_dotenv, find_dotenv
from hdbcli import dbapi
import pyodbc
# Cargar variables de entorno
load_dotenv(find_dotenv())

class DatabaseConfig:
    """Configuración dinámica de base de datos según ambiente"""
    
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'development').lower()
    
    @classmethod
    def get_sql_config(cls):
        """Retorna configuración SQL según ambiente"""
        if cls.ENVIRONMENT == 'prod':
            return {
                'server': os.getenv('SQL_SERVER'),
                'database': os.getenv('SQL_DATABASE'),
                'user': os.getenv('SQL_USER'),
                'password': os.getenv('SQL_PASSWORD')
            }
        else:  # development
            return {
                'server': os.getenv('SQL_DEV_SERVER'),
                'database': os.getenv('SQL_DEV_DATABASE'),
                'user': os.getenv('SQL_DEV_USER'),
                'password': os.getenv('SQL_DEV_PASSWORD')
            }
    
    @classmethod
    def get_hana_config(cls):
        """SAP HANA (mismo para ambos ambientes)"""
        return dbapi.connect(
            address= os.getenv('HANA_HOST'),
            port= os.getenv('HANA_PORT', '30015'),
            user= os.getenv('HANA_USER'),
            password= os.getenv('HANA_PASSWORD'),
        )

    @classmethod
    def get_hana_connection_string(cls):
        """Cadena de conexión para SAP HANA"""
        return f"Driver=HDBODBC;ServerNode={os.getenv('HANA_HOST')}:{os.getenv('HANA_PORT', '30015')};UID={os.getenv('HANA_USER')};PWD={os.getenv('HANA_PASSWORD')};"


    @classmethod
    def get_sql_connection_string(cls):
        """Connection string dinámico"""
        config = cls.get_sql_config()
        # return f"DRIVER={{SQL Server}};SERVER={config['server']};DATABASE={config['database']};UID={config['user']};PWD={config['password']}"
        return f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={config['server']};DATABASE={config['database']};UID={config['user']};PWD={config['password']};Encrypt=yes;TrustServerCertificate=yes;"

    @classmethod
    def get_conn_sql(cls):
        """Obtiene un cursor para SQL Server"""
        conn_str = cls.get_sql_connection_string()
        return pyodbc.connect(conn_str)
    
    @classmethod
    def get_cube_connection(cls):
        """Obtiene conexión ADOMD.NET para SAP BW"""
        conn = f"Provider={os.getenv('PROVIDER_SQL')};Data Source={os.getenv('DATA_SOURCE_SQL')};Catalog={os.getenv('CATALOG_SQL_VTS')};"
        if not conn:
            raise ValueError("ADOMD_CONNECTION_STRING no está configurada en las variables de entorno.")
        return Pyadomd(conn)



class EmailConfig:
    """Configuración de email (mismo para ambos ambientes)"""
    
    SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
    SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
    EMAIL_USER = os.getenv('EMAIL_USER')
    EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
    SENDER_NAME = os.getenv('SENDER_NAME', 'ETL Reingeniería')
    
    # Recipients ajustados por ambiente
    @classmethod
    def get_recipients(cls, notification_type='admin'):
        """Obtiene destinatarios según tipo y ambiente"""
        env = DatabaseConfig.ENVIRONMENT
        
        if env == 'production':
            recipients = {
                'admin': os.getenv('ADMIN_EMAILS', '').split(','),
                'error': os.getenv('ERROR_EMAILS', '').split(','),
                'success': os.getenv('SUCCESS_EMAILS', '').split(',')
            }
        else:  # development
            # En desarrollo, solo enviar a admins para testing
            dev_emails = os.getenv('DEV_EMAILS', os.getenv('ADMIN_EMAILS', '')).split(',')
            recipients = {
                'admin': dev_emails,
                'error': dev_emails,
                'success': dev_emails
            }
        
        return [email.strip() for email in recipients.get(notification_type, []) if email.strip()]

# Función helper para mostrar configuración actual
def show_current_config():
    """Muestra la configuración actual"""
    print(f"🌍 Ambiente actual: {DatabaseConfig.ENVIRONMENT.upper()}")
    print(f"🗄️  SQL Server: {DatabaseConfig.get_sql_config()['server']}")
    print(f"📧 Email recipients: {EmailConfig.get_recipients('admin')}")
