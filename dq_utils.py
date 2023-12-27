import os
import sys
import json
import pandas as pd
import numpy as np

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert


import pandas as pd



import time
from datetime import datetime
import warnings
import re 

sys.path.append('/Users/ob/Work/bssg/OpenLineage/client/python')

from openlineage.client.facet import (
    ParentRunFacet,
    DataQualityAssertionsDatasetFacet,
    Assertion
)

class DQUtils:
    """Вспомогательный класс для работы с функциями качества
    """
    def __init__(self, util):
        """Конструктор класса

        Args:
            util (Utils): Ссылка на объект вспомогательного класса
        """
        
        
        self.tenant = util.tenant
        self.util = util
        self.now = datetime.now()
        self.pg_engine = self.util.get_pg_engine()
        self.log = self.util.get_pg_table('dq_log')

    
    def execute_rules(self):
        """Выполнение правил
        """
        self.pg_engine = self.util.get_pg_engine()
        self.log = self.util.get_pg_table('dq_log')
        print('\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/')
        with self.pg_engine.connect() as conn:
        
            result = conn.execute(self.util.create_rule_to_connect_regular()).fetchall()

        for record in result:
            try:
                print(f'record.rule_ref =  {record.rule_ref} ')
                getattr(self, record.rule_ref)(
                    record.entity_sample_to_dq_rule_id, record.rule_name,  record.rule_settings, record.send_mail==True,
                    self.util.create_pg_connection_string (record.url, record.username, record.password) 
                    if record.url else 
                    self.util.create_pg_connection_string_from_settings(record.rule_settings))
            except Exception as e:
                print(e)
                self.util.send_mail("Ошибка проверки качества в Airflow", f'Ошибка выполнения проверки правила: {record.rule_name}\nОшибка : {e}\nДата проверки : {self.util.today}',self.util.dq_email_list)
        
        print('\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/')
        
    def execute_rules_for_column(self, data,  data_asset_id):
        """Выполнение правил
        """
        self.pg_engine = self.util.get_pg_engine()
        self.log = self.util.get_pg_table('dq_log')
        print('\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/')
        with self.pg_engine.connect() as conn:
        
            result = conn.execute(self.util.create_rule_to_connect_for_asset(data_asset_id)).fetchall()
        
        assertions : List[Assertion] = []
        for record in result:
            try:
                if(len(record.rule_settings) == 0): continue
                
                settings = json.loads(record.rule_settings)
                if('table'  in settings):continue
                print(f'record.rule_ref =  {record.rule_ref} ')
                entity_sample_to_dq_rule_id = str(record.entity_sample_to_dq_rule_id); 
                
                if('columns'  in settings):
                    for c in settings['columns'].split(";"):
                        res = ''
                        if('http' in  record.rule_ref):
                            import requests
                            settings['value'] = data[c]
                            
                            res = requests.post(record.rule_ref, json = settings)
                            res = json.loads(res.content)['res']
                        else:
                            res = getattr(self, record.rule_ref)(c, data, settings) 
                        if(res):
                            assertions.append(Assertion(assertion = record.rule_ref,success = True,column = c, state="2", msg =f'Идентификатор записи {data["id"]}' if 'id' in data else None,rule_id = entity_sample_to_dq_rule_id))
                        else:
                            assertions.append(Assertion(assertion = record.rule_ref,success = False,column = c, state="0", msg =f'Идентификатор записи {data["id"]}' if 'id' in data else None,rule_id = entity_sample_to_dq_rule_id))
                     
            except Exception as e:
                print(e)
                self.util.send_mail("Ошибка проверки качества", f'Ошибка выполнения проверки правила: {record.rule_name}\nОшибка : {e}\nДата проверки : {self.util.today}',self.util.dq_email_list)
        
        print('\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/')
        return assertions
    
    def execute_rules_for_table(self,  data_asset_id):
        """Выполнение правил
        """
        self.pg_engine = self.util.get_pg_engine()
        self.log = self.util.get_pg_table('dq_log')
        print('\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/')
        with self.pg_engine.connect() as conn:
        
            result = conn.execute(self.util.create_rule_to_connect_for_asset(data_asset_id)).fetchall()
        
        assertions : List[Assertion] = []
        for record in result:
            try:
                if(len(record.rule_settings) == 0): continue
                
                settings = json.loads(record.rule_settings)
                
                print(f'record.rule_ref =  {record.rule_ref} ')
               
                if('columns'  in settings and 'table'  in settings) :
                    res = getattr(self, record.rule_ref)(
                    record.entity_sample_to_dq_rule_id, record.rule_name,  record.rule_settings, record.send_mail==True,
                    self.util.create_pg_connection_string (record.url, record.username, record.password) 
                    if record.url else 
                    self.util.create_pg_connection_string_from_settings(record.rule_settings))
                    return res;
            except Exception as e:
                print(e)
                self.util.send_mail("Ошибка проверки качества", f'Ошибка выполнения проверки правила: {record.rule_name}\nОшибка : {e}\nДата проверки : {self.util.today}',self.util.dq_email_list)
        
        print('\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/')
        return assertions
    
    def execute_rules_for_parquet(self,  data_asset_id):
        """Выполнение правил
        """
        self.pg_engine = self.util.get_pg_engine()
        self.log = self.util.get_pg_table('dq_log')
        print('\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/')
        with self.pg_engine.connect() as conn:
        
            result = conn.execute(self.util.create_rule_to_connect_for_asset(data_asset_id)).fetchall()
        
        assertions : List[Assertion] = []
        for record in result:
            try:
                if(len(record.rule_settings) == 0): continue
                
                settings = json.loads(record.rule_settings)
                
                print(f'record.rule_ref =  {record.rule_ref} ')
               
                if('path'  in settings ) :
                    res = getattr(self, record.rule_ref)(
                    record.entity_sample_to_dq_rule_id, record.rule_name,  record.rule_settings, record.send_mail==True)
                    return res;
            except Exception as e:
                print(e)
                self.util.send_mail("Ошибка проверки качества", f'Ошибка выполнения проверки правила: {record.rule_name}\nОшибка : {e}\nДата проверки : {self.util.today}',self.util.dq_email_list)
        
        print('\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/')
        return assertions
    
    def count_equal(self, id,name, settings,send_mail):
        s = json.loads(settings)
        from pyspark.sql import SparkSession
        assertions : List[Assertion] = []
        spark = SparkSession.builder.appName("check").getOrCreate()
        if('path'  in s and 'count'  in s) :
            df = spark.read.parquet(s['path'])
            if(s['count'] != str(df.count())):
                msg =  f'В файле {s["path"]} к-во записей не равно {s["count"]}'
                assertions.append(Assertion(assertion = "count_equal",success = False,column = "*", state="0", msg =msg,rule_id = str(id)))
                self.log_entry(0, msg, id,name,send_mail)
            else:
                msg =  f'В файле {s["path"]} к-во записей  равно {s["count"]}'
                assertions.append(Assertion(assertion = "count_equal",success = True,column = "*", state="2", msg =msg,rule_id = str(id)))
                self.log_entry(1, msg, id,name,send_mail)
        return assertions
    
    def not_null(self, column, data, settings):
        val = data[column]
        return val != None
    
    def check_condition(self, column, data, settings):
        if('regex' in settings):
            regex = settings['regex']
            val = data[column]
            return re.search(regex, val)
            
    def check_uniqueness(self, id, name, settings,send_mail, url):
        """Проверка уникальности

        Args:
            id (string): идентификатор правила
            settings (string): настройки
         {
            "table":"\"finDB\".equities",
            "columns":"symbol;name"
        }
        """
        print(f'Выполняется проверка check_unique')

       
           
        s = json.loads(settings)
        table =  s['table']
        columns =  s['columns'].split(';')
        message = ''
        for c in columns:
            print(f'Проверка колонки {c} для таблицы {table}')
            message = f'{message}{self.check_uniqueness_for_column(url,table, c)}'
            
        if len(message) > 0: 
            self.log_entry(0, f'В таблице {table} найдены дубликаты: {message}', id,name,send_mail)
        else:
            self.log_entry(1, f'Ошибок нет. Колонки {s["columns"]} не содержат дубликаты.', id,name,send_mail)
                
    def check_uniqueness_for_column(self,url,table, table_column):
        """Проверка уникальности
        """
        with self.util.create_pg_engine(url).connect() as conn:
          
            result = conn.execute(f'select {table_column} as column, count(*) count from {table} where {table_column} is not null group by {table_column} HAVING count(*) > 1').fetchall()
            for record in result:
                column = record['column']
                count = record['count']
                if(count>0):
                    return  f'Значение "{column}" повторяется {count} раз(а) в колонке {table_column};'
            return '';
    
         
         
            
    def check_null_values(self, id,name, settings,send_mail,url):
        """Проверка на пустые значения

        Args:
            id (string): идентификатор правила
            settings (string): настройки
           {
                "table":"\"finDB\".equities",
                "columns":"symbol;currency"
            }
        """
        print(f'Выполняется проверка check_null_values')
        with self.util.create_pg_engine(url).connect() as conn:
            print(f'settings = {settings}')
            s = json.loads(settings)
            table =  s['table']
            columns =  s['columns'].split(';')
            message = ''
            for c in columns:
                print(f'Проверка колонки {c} для таблицы {table}')
                message = f'{message}{self.check_null_values_for_column(url,table, c)}'
               
        if len(message) > 0: 
            print(f'message = {message}')
            self.log_entry(0, f'В таблице {table} найдены пустые значения: {message}', id,name,send_mail)
        else:
            self.log_entry(1, f'Ошибок нет. Колонки {s["columns"]} не пустые.', id,name,send_mail)
                
    def check_null_values_for_column(self,url,table, table_column):
        """Проверка на пустые значения
        """
        with self.util.create_pg_engine(url).connect() as conn:
          
            result = conn.execute(f'select  count(*) count from {table} where {table_column} is null').fetchall()
            for record in result:
                
                count = record['count']
                if(count>0):
                    return  f'В колонке {table_column} {count} пустых значений;'
            return '';
    
    def check_range(self, id,name, settings,send_mail,url):
        """Проверка диапазона

        Args:
            id (string): идентификатор правила
            settings (string): настройки
            {
                "table":"datasets.transactions",
                "columns":"buysell;\"ACTION\"",
                "ranges":"('B','S');(0,1,2)"
            }
        """
        print(f'Выполняется проверка check_range')
        assertions : List[Assertion] = []
        with self.util.create_pg_engine(url).connect() as conn:
            print(f'settings = {settings}')
            s = json.loads(settings)
            table =  s['table']
            columns =  s['columns'].split(';')
            ranges =  s['ranges'].split(';')
            message = ''
            i = 0;
            for c,r in zip(columns,ranges):
                print(f'Проверка колонки {c} для таблицы {table}')
                message = f'{message}{self.check_range_for_column(url,table, c,r)}'
                i = i+1
        
        if(len(message) > 0):
            msg =  f'В таблице {table} найдены записи вне диапазона: {message}'
            assertions.append(Assertion(assertion = "check_range",success = False,column = columns, state="0", msg =msg,rule_id = str(id)))
            self.log_entry(0, msg, id,name,send_mail)
        else:
            msg = 'Ошибок нет. Колонки {s["columns"]} не содержат ошибок.'
            assertions.append(Assertion(assertion = "check_range",success = True,column = columns, state="2", msg =msg,rule_id = str(id)))
            self.log_entry(1, msg, id,name,send_mail)
        return assertions
                
    def check_range_for_column(self,url,table, table_column, table_column_range):
        """Проверка диапазона
        """
        with self.util.create_pg_engine(url).connect() as conn:
          
            q = f'select  count(*) count from {table} where not ({table_column} {"in" if "(" in table_column_range  else ""} {table_column_range})'
            
            result = conn.execute(q).fetchall()
            for record in result:
                
                count = record['count']
                if(count>0):
                    return  f'В колонке {table_column} {count} значений вне диапазона {table_column_range};'
            return '';
       
         
    def log_entry(self, status, description, rule_id, rule_name,sm):
            """Запись лога выполнения проверок

            Args:
                status (string): статус
                description (string): описание
                rule_id (string): ключ правила
            """
            new_data = {
                'status': status,
                'date': self.now,
                'description': description,
                'rule_id': rule_id,
                'tenant': self.tenant
            }
            print(f'sm = {sm} status = {status}')
            if status == 0 and  sm:
                self.util.send_mail("Ошибка проверки качества", f'Название правила: {rule_name}\nОписание : {description}\nДата проверки : {self.util.today}',self.util.dq_email_list)
        

            if(self.pg_engine):
                with self.pg_engine.connect() as conn:
                    stmt = insert(self.log).values(new_data)
                    conn.execute(stmt)
                print(description)
