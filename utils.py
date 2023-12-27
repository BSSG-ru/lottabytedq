import json
import os
import datetime

from airflow.models import Variable

from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, insert
from sqlalchemy.dialects import postgresql
from sqlalchemy.inspection import inspect


import traceback as tb
import smtplib, ssl



class Utils:
    """Вспомогательный класс для выполнения ELT процесса
    """
    def __init__(self):
        """Конструктор
        """
        self.initialize_from_settings()
        

   
    def initialize_from_settings(self):
        f = open ('settings.json', "r")
        self.settings = json.load(f)
        f.close()
        self.today = datetime.date.today()
        self.env = self.settings['env']
        self.pg_schema = self.settings['pg_schema']
        self.exceptions_table = self.settings['pg_exceptions_table']
        self.smtp_host =  self.settings['smtp_host']
        self.smtp_port =  self.settings['smtp_port'] 
        self.smtp_login =  self.settings['smtp_login'] 
        self.smtp_password =  self.settings['smtp_password'] 
        self.exception_email_list =  self.settings['exception_email_list'] 
        self.error_email_list =  self.settings['error_email_list'] 
        self.dq_email_list =  self.settings['dq_email_list'] 
        self.tenant = self.settings['tenant'] 
        self.open_linage_path = self.settings['open_linage_path'] 
        self.kafka_producer_topic = self.settings['kafka_producer_topic'] 
        self.kafka_producer = self.settings['kafka_producer'] 
        self.kafka_bootstrap_servers = self.settings['kafka_bootstrap_servers'] 
        self.kafka_namespace = self.settings['kafka_namespace'] 
        self.kafka_producer_topic = self.settings['kafka_producer_topic'] 
       
        
    def get_pg_engine(self,isolation_level="AUTOCOMMIT"):
        """Создание механизма создания подключения к PG 

        Returns:
            Engine: Механизма создания подключения к PG
        """
        if(self.settings):
            self.pg_database = self.settings['pg_database']
            self.pg_user = self.settings['pg_username']
            self.pg_password = self.settings['pg_password']
            self.pg_port = self.settings['pg_port']
            self.pg_host = self.settings['pg_host']
        else:
            self.pg_database = Variable.get('pg_database')
            self.pg_user = Variable.get('pg_username')
            self.pg_password = Variable.get('pg_password')
            self.pg_port = Variable.get('pg_port')
            self.pg_host = Variable.get('pg_host')
        pg_conn_str = f'postgresql+psycopg2://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_database}'
        self.pg_engine = create_engine(pg_conn_str,isolation_level=isolation_level)
        return self.pg_engine
    
    def create_pg_connection_string(self,url,username,password,isolation_level="AUTOCOMMIT"):
        return f'postgresql+psycopg2://{username}:{password}@{url.replace("jdbc:postgresql://","")}'
    
    def create_pg_engine(self,conn_str,isolation_level="AUTOCOMMIT"):
        """Создание механизма создания подключения к PG 

        Returns:
            Engine: Механизма создания подключения к PG
        """
        return create_engine(conn_str,
                            isolation_level=isolation_level,
                            pool_pre_ping=True, 
                            pool_recycle=3600)

  

   
    def get_pg_table(self, name,schema = "dq"):
        """Создание мета-таблицы PG

        Args:
            name (string): название таблицы

        Returns:
            Table: таблица
        """
        pg_metadata = MetaData(schema=schema)
        return Table(name, pg_metadata, autoload=True, autoload_with=self.pg_engine)
    

    def get_pg_exceptions_table(self):
        return self.get_pg_table(self.exceptions_table)
   
    def write_exception(self, e, type):
         new_data = {
                    'date': datetime.datetime.now(),
                    'message': str(e),
                    'type': type
                }
         self.send_mail("Исключение в Airflow", f'\nОшибка: {e if isinstance(e, str) is str else "".join(tb.format_exception(None, e, e.__traceback__))}, \nтип : {type}, \nдата : {datetime.datetime.now()}', self.exception_email_list)
         
         postgres_table = self.get_pg_exceptions_table()
         with self.pg_engine.connect() as conn:
            stmt = insert(postgres_table).values(new_data)
            conn.execute(stmt)
         print(f'Error occurred: {e}')

    def get_meta_file(self, file):
        """Чтение файла по имени

        Args:
            file (string): название файла

        Returns:
             dict:json с содержимым файла
        """
        with open(f'{self.path}/{file}', 'r') as meta:
              meta_data = meta.read()
        return json.loads(meta_data)
    
    def upsert_table_pg(self, table_values, table_name, schema, pg_engine):
        """Вставка/обновление данных в таблице table_name данными из объекта table_values

        Args:
            table_values (DataFrame): данные для вставки/обновления
            table_name (string): таблица для вставки/обновления
            schema (string): схема таблицы для вставки/обновления
            pg_engine (Engine): Механизма создания подключения к PG

        Returns:
            string: результат выполнения запроса
        """
        metadata = MetaData(schema=schema)
        metadata.bind = pg_engine

        table = Table(table_name, metadata, schema=schema, autoload=True)

        # get list of fields making up primary key
        primary_keys = [key.name for key in inspect(table).primary_key]

        print(f'--- Primary keys{primary_keys}')
        # assemble base statement
        stmt = postgresql.insert(table).values(table_values)


        # define dict of non-primary keys for updating
        update_dict = {
            c.name: c
            for c in stmt.excluded
            if not c.primary_key
        }
        print(f'--- update_dict{update_dict}')
        # assemble new statement with 'on conflict do update' clause
        update_stmt = stmt.on_conflict_do_update(
            index_elements=primary_keys,
            set_=update_dict,
        )

        # execute
        with self.pg_engine.connect() as conn:
            result = conn.execute(update_stmt)
            print('Done')
            return result

    def execute_sp(self, function_name):
        with self.get_pg_engine().connect() as conn:
                conn.execute(f'CALL {self.pg_schema}.{function_name}()')
                
    def get_sorted_list_dir(self, path):
        os.chdir(path)
        return sorted(filter(os.path.isfile, os.listdir(path)), key=os.path.getmtime)
    
    def send_mail(self, subject, text,email_list):

        
        smtp_server = self.smtp_host
        sender_email = self.smtp_login
        receiver_email = email_list
        password = self.smtp_password
        message = f'Content-Type: text/plain; charset="utf-8"\nContent-Transfer-Encoding: 8bit\nMIME-Version: 1.0\r\n'
        message = f'{message}From: {sender_email}\r\nTo: admin\r\n'

        message =  f'{message}Subject: {subject}\n\nДобрый день.\n\nДанное письмо направлено оркестратором интеграции  из {  "тестового" if self.env=="test"  else "продуктивного"} ландшафта\n\nСообщение:\n\n{text}\n\n С уважением,\nрассылка lottabyte'
        
        with smtplib.SMTP_SSL(smtp_server) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email.split(','), message.encode('utf8'))
            server.quit()
            
    def create_rule_to_connect_for_schedule(self):
        return  f'''SELECT s.id AS system_id,
                s.name AS system_name,
                dr.name AS rule_name,
                dr.id AS rule_id,
                dr.rule_ref,
                r.settings AS rule_settings,
                sc.connector_id,
                t.system_connection_id,
                eq.name AS query_name,
                r.id AS entity_sample_to_dq_rule_id,
                e.name AS entity_name,
                da.id as data_asset_id,
                da1.id as data_asset_id1,
                dqt.id as dq_tasks_id,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_username\') AS username,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_password\') AS password,
                ( SELECT replace(connector_params.param_value, \'jdbc:postgresql://\', \'\') AS replace
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_url\') AS url,
                    r.send_mail
            FROM da_{self.tenant}.entity_sample_to_dq_rule r
                LEFT JOIN da_{self.tenant}.dq_rule dr ON r.dq_rule_id = dr.id
                LEFT JOIN da_{self.tenant}.entity_sample es ON r.entity_sample_id = es.id
                LEFT JOIN da_{self.tenant}.entity_query eq ON es.entity_query_id = eq.id
                LEFT JOIN da_{self.tenant}.entity e ON e.id = eq.entity_id
                LEFT JOIN da_{self.tenant}.task t ON t.query_id = eq.id
                LEFT JOIN da_{self.tenant}.indicator i ON i.id = r.indicator_id
                LEFT JOIN da_{self.tenant}.product p ON p.id = r.product_id
                LEFT JOIN da_{self.tenant}.system_connection sc ON sc.id = t.system_connection_id
                LEFT JOIN da_{self.tenant}.system s ON s.id = eq.system_id 
                LEFT JOIN da_{self.tenant}.data_asset da ON da.id = r.asset_id 
                LEFT JOIN da_{self.tenant}.data_asset da1 ON da1.system_id = s.id and da1.entity_id = e.id
                JOIN da_{self.tenant}.dq_tasks dqt ON dqt.rule_id = r.id and dqt.state <> 2
                where (r.disabled = false or r.disabled is null)
                and ((p.state = \'PUBLISHED\' or p.state is null) and (i.state = \'PUBLISHED\'  or i.state is null) and (da.state = \'PUBLISHED\'  or da.state is null) and (da1.state = \'PUBLISHED\'  or da1.state is null)) and  (dr.rule_ref is not null) 
                and r.settings like \'%%crontab%%\' '''
    
    def create_rule_to_connect_regular_for_schedule(self):
        return  f'''SELECT s.id AS system_id,
                s.name AS system_name,
                dr.name AS rule_name,
                dr.id AS rule_id,
                dr.rule_ref,
                r.settings AS rule_settings,
                sc.connector_id,
                t.system_connection_id,
                eq.name AS query_name,
                r.id AS entity_sample_to_dq_rule_id,
                e.name AS entity_name,
                da.id as data_asset_id,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_username\') AS username,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_password\') AS password,
                ( SELECT replace(connector_params.param_value, \'jdbc:postgresql://\', \'\') AS replace
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_url\') AS url,
                    r.send_mail
            FROM da_{self.tenant}.entity_sample_to_dq_rule r
                LEFT JOIN da_{self.tenant}.dq_rule dr ON r.dq_rule_id = dr.id
                LEFT JOIN da_{self.tenant}.entity_sample es ON r.entity_sample_id = es.id
                LEFT JOIN da_{self.tenant}.entity_query eq ON es.entity_query_id = eq.id
                LEFT JOIN da_{self.tenant}.entity e ON e.id = eq.entity_id
                LEFT JOIN da_{self.tenant}.task t ON t.query_id = eq.id
                LEFT JOIN da_{self.tenant}.indicator i ON i.id = r.indicator_id
                LEFT JOIN da_{self.tenant}.product p ON p.id = r.product_id
                LEFT JOIN da_{self.tenant}.system_connection sc ON sc.id = t.system_connection_id
                LEFT JOIN da_{self.tenant}.system s ON s.id = eq.system_id 
                LEFT JOIN da_{self.tenant}.data_asset da ON da.system_id = s.id and da.entity_id = e.id and da.state = \'PUBLISHED\'
                where (r.disabled = false or r.disabled is null)
                and ((p.state = \'PUBLISHED\' or p.state is null) and (i.state = \'PUBLISHED\'  or i.state is null)) and  (dr.rule_ref is not null) 
                and r.settings like \'%%crontab%%\''''
    
    def create_rule_to_connect_regular(self):
        return  f'''SELECT s.id AS system_id,
                s.name AS system_name,
                dr.name AS rule_name,
                dr.id AS rule_id,
                dr.rule_ref,
                r.settings AS rule_settings,
                sc.connector_id,
                t.system_connection_id,
                eq.name AS query_name,
                r.id AS entity_sample_to_dq_rule_id,
                e.name AS entity_name,
                da.id as data_asset_id,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_username\') AS username,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_password\') AS password,
                ( SELECT replace(connector_params.param_value, \'jdbc:postgresql://\', \'\') AS replace
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_url\') AS url,
                    r.send_mail
            FROM da_{self.tenant}.entity_sample_to_dq_rule r
                LEFT JOIN da_{self.tenant}.dq_rule dr ON r.dq_rule_id = dr.id
                LEFT JOIN da_{self.tenant}.entity_sample es ON r.entity_sample_id = es.id
                LEFT JOIN da_{self.tenant}.entity_query eq ON es.entity_query_id = eq.id
                LEFT JOIN da_{self.tenant}.entity e ON e.id = eq.entity_id
                LEFT JOIN da_{self.tenant}.task t ON t.query_id = eq.id
                LEFT JOIN da_{self.tenant}.indicator i ON i.id = r.indicator_id
                LEFT JOIN da_{self.tenant}.product p ON p.id = r.product_id
                LEFT JOIN da_{self.tenant}.system_connection sc ON sc.id = t.system_connection_id
                LEFT JOIN da_{self.tenant}.system s ON s.id = eq.system_id 
                LEFT JOIN da_{self.tenant}.data_asset da ON da.system_id = s.id and da.entity_id = e.id and da.state = \'PUBLISHED\'
                where (r.disabled = false or r.disabled is null)
                and ((p.state = \'PUBLISHED\' or p.state is null) and (i.state = \'PUBLISHED\'  or i.state is null)) and  (dr.rule_ref is not null) 
                and (r.settings  not like \'%%Kafka%%\')'''
    
    def create_rule_to_connect_for_product(self, product_id):
        return  f'''SELECT s.id AS system_id,
                s.name AS system_name,
                dr.name AS rule_name,
                dr.id AS rule_id,
                dr.rule_ref,
                r.settings AS rule_settings,
                sc.connector_id,
                t.system_connection_id,
                eq.name AS query_name,
                r.id AS entity_sample_to_dq_rule_id,
                e.name AS entity_name,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_username\') AS username,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_password\') AS password,
                ( SELECT replace(connector_params.param_value, \'jdbc:postgresql://\', \'\') AS replace
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_url\') AS url,
                    r.send_mail
            FROM da_{self.tenant}.entity_sample_to_dq_rule r
                LEFT JOIN da_{self.tenant}.dq_rule dr ON r.dq_rule_id = dr.id
                LEFT JOIN da_{self.tenant}.entity_sample es ON r.entity_sample_id = es.id
                LEFT JOIN da_{self.tenant}.entity_query eq ON es.entity_query_id = eq.id
                LEFT JOIN da_{self.tenant}.entity e ON e.id = eq.entity_id
                LEFT JOIN da_{self.tenant}.task t ON t.query_id = eq.id
                LEFT JOIN da_{self.tenant}.indicator i ON i.id = r.indicator_id
                LEFT JOIN da_{self.tenant}.product p ON p.id = r.product_id
                LEFT JOIN da_{self.tenant}.system_connection sc ON sc.id = t.system_connection_id
                LEFT JOIN da_{self.tenant}.system s ON s.id = eq.system_id where (r.disabled = false or r.disabled is null) 
                and ((p.state = \'PUBLISHED\' or p.state is null) and (i.state = \'PUBLISHED\'  or i.state is null)) and  (dr.rule_ref is not null) 
                and (p.id  =  \'{product_id}\')'''
                
    def create_rule_to_connect_for_asset(self, asset_id):
        return  f'''SELECT s.id AS system_id,
                s.name AS system_name,
                dr.name AS rule_name,
                dr.id AS rule_id,
                dr.rule_ref,
                r.settings AS rule_settings,
                sc.connector_id,
                t.system_connection_id,
                eq.name AS query_name,
                r.id AS entity_sample_to_dq_rule_id,
                e.name AS entity_name,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_username\') AS username,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_password\') AS password,
                ( SELECT replace(connector_params.param_value, \'jdbc:postgresql://\', \'\') AS replace
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'jdbc_url\') AS url,
                    r.send_mail
            FROM da_{self.tenant}.entity_sample_to_dq_rule r
                LEFT JOIN da_{self.tenant}.dq_rule dr ON r.dq_rule_id = dr.id
                LEFT JOIN da_{self.tenant}.entity_sample es ON r.entity_sample_id = es.id
                LEFT JOIN da_{self.tenant}.entity_query eq ON es.entity_query_id = eq.id
                LEFT JOIN da_{self.tenant}.entity e ON e.id = eq.entity_id
                LEFT JOIN da_{self.tenant}.task t ON t.query_id = eq.id
                LEFT JOIN da_{self.tenant}.indicator i ON i.id = r.indicator_id
                LEFT JOIN da_{self.tenant}.data_asset da ON da.id = r.asset_id
                LEFT JOIN da_{self.tenant}.product p ON p.id = r.product_id
                LEFT JOIN da_{self.tenant}.system_connection sc ON sc.id = t.system_connection_id
                LEFT JOIN da_{self.tenant}.system s ON s.id = eq.system_id where (r.disabled = false or r.disabled is null) 
                and ((p.state = \'PUBLISHED\' or p.state is null) and (i.state = \'PUBLISHED\'  or i.state is null)
                and (da.state = \'PUBLISHED\'  or da.state is null)) and  (dr.rule_ref is not null) 
                and (da.id  =  \'{asset_id}\')'''
    
    def create_rule_to_connect_kafka(self):
        return  f'''SELECT s.id AS system_id,
                s.name AS system_name,
                dr.name AS rule_name,
                dr.id AS rule_id,
                dr.rule_ref,
                r.settings AS rule_settings,
                sc.connector_id,
                t.system_connection_id,
                eq.name AS query_name,
                r.id AS entity_sample_to_dq_rule_id,
                e.name AS entity_name,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'config\') AS config,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'topic\') AS topic,

                    r.send_mail
            FROM da_{self.tenant}.entity_sample_to_dq_rule r
                LEFT JOIN da_{self.tenant}.dq_rule dr ON r.dq_rule_id = dr.id
                LEFT JOIN da_{self.tenant}.entity_sample es ON r.entity_sample_id = es.id
                LEFT JOIN da_{self.tenant}.entity_query eq ON es.entity_query_id = eq.id
                LEFT JOIN da_{self.tenant}.entity e ON e.id = eq.entity_id
                LEFT JOIN da_{self.tenant}.task t ON t.query_id = eq.id
                LEFT JOIN da_{self.tenant}.indicator i ON i.id = r.indicator_id
                LEFT JOIN da_{self.tenant}.product p ON p.id = r.product_id
                LEFT JOIN da_{self.tenant}.system_connection sc ON sc.id = t.system_connection_id
                LEFT JOIN da_{self.tenant}.system s ON s.id = eq.system_id where (r.disabled = false or r.disabled is null) 
                and ((p.state = \'PUBLISHED\' or p.state is null) and (i.state = \'PUBLISHED\'  or i.state is null)) and  (dr.rule_ref is not null) 
                 and (r.settings   like \'%%Kafka%%\')'''
    
    def create_rule_to_connect_check(self):
        return  f'''SELECT s.id AS system_id,
                s.name AS system_name,
                dr.name AS rule_name,
                dr.id AS rule_id,
                dr.rule_ref,
                r.settings AS rule_settings,
                sc.connector_id,
                t.system_connection_id,
                eq.name AS query_name,
                r.id AS entity_sample_to_dq_rule_id,
                e.name AS entity_name,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'config\') AS config,
                ( SELECT connector_params.param_value
                    FROM da_{self.tenant}.connector_params
                    WHERE connector_params.system_connection_id = t.system_connection_id AND connector_params.param_name = \'topic\') AS topic,

                    r.send_mail
            FROM da_{self.tenant}.entity_sample_to_dq_rule r
                LEFT JOIN da_{self.tenant}.dq_rule dr ON r.dq_rule_id = dr.id
                LEFT JOIN da_{self.tenant}.entity_sample es ON r.entity_sample_id = es.id
                LEFT JOIN da_{self.tenant}.entity_query eq ON es.entity_query_id = eq.id
                LEFT JOIN da_{self.tenant}.entity e ON e.id = eq.entity_id
                LEFT JOIN da_{self.tenant}.task t ON t.query_id = eq.id
                LEFT JOIN da_{self.tenant}.indicator i ON i.id = r.indicator_id
                LEFT JOIN da_{self.tenant}.product p ON p.id = r.product_id
                LEFT JOIN da_{self.tenant}.system_connection sc ON sc.id = t.system_connection_id
                LEFT JOIN da_{self.tenant}.system s ON s.id = eq.system_id where (r.disabled = false or r.disabled is null) 
                and ((p.state = \'PUBLISHED\' or p.state is null) and (i.state = \'PUBLISHED\'  or i.state is null)) and  (dr.rule_ref is not null) 
                  and (r.rule_ref   like \'%%check_nominal_time%%\')'''    
    def create_pg_connection_string_from_settings(self, settings):
        connection = self.get_param_from_settings(settings, 'connection')
    
        with self.get_pg_engine().connect() as conn:
            username =  self.get_connection_param(conn,connection,'jdbc_username')
            password =  self.get_connection_param(conn,connection,'jdbc_password')
            url =  self.get_connection_param(conn,connection,'jdbc_url')
            return self.create_pg_connection_string(url, username, password)
    
    def get_settings_for_kafka(self, settings):
        connection = self.get_param_from_settings(settings, 'connection')
    
        with self.get_pg_engine().connect() as conn:
            topic =  self.get_connection_param(conn,connection,'topic')
            config =  self.get_connection_param(conn,connection,'config')
            return {'topic':topic,'config':config}       
            
    def get_param_from_settings(self, settings, p_name):
        s = json.loads(settings)
        return  s[p_name]
    
    def get_connection_param(self, conn,connection, p_name):
        sp = f'''SELECT scp.param_value
            FROM da_{self.tenant}.system_connection_param scp
            LEFT JOIN da.connector_param cp ON cp.id = scp.connector_param_id
            LEFT JOIN da_{self.tenant}.system_connection sc on sc.id = scp.system_connection_id where sc.name = \'{connection}\' and cp.name =\'{p_name}\' '''
        res = conn.execute(sp).fetchone()
        return res.param_value
       
        



     
