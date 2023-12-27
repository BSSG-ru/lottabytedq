import os
import threading
import sys
import json
import uuid
import pandas as pd
import numpy as np
from enum import Enum
from decimal import Decimal
from typing import Any, Dict, List, Optional
from confluent_kafka import Consumer

from utils  import Utils
util = Utils()
sys.path.append(util.open_linage_path)
from openlineage.client.client import OpenLineageClient
from openlineage.client.transport import KafkaTransport,KafkaConfig
from openlineage.client.run import Job, Run, RunEvent, RunState,Dataset,InputDataset,OutputDataset
from openlineage.client.facet import (
    ParentRunFacet,
    DataQualityAssertionsDatasetFacet,
    Assertion
)

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

import pandas as pd
import time
from datetime import datetime
import warnings



class DQKafkaSubscriber:

    def __init__(self, util):
        
        self.tenant = util.tenant
        self.util = util
        self.now = datetime.now()

    
    def execute_rules(self):
        """Выполнение правил
        """
        try:
            self.pg_engine = self.util.get_pg_engine()
            self.log = self.util.get_pg_table('dq_log',f'da_{self.tenant}')
            print('\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/')
            sql  = self.util.create_rule_to_connect_kafka()
            print(sql)
            with self.pg_engine.connect() as conn:
                result = conn.execute(sql).fetchall()
            procs = []
            for record in result:
                try:
                    s =  util.get_settings_for_kafka(record.rule_settings)
                    print(f'record.rule_ref =  {record.rule_ref} ')
                
                    ff = getattr(self, record.rule_ref)
                    proc = threading.Thread(target=ff, args=(record.entity_sample_to_dq_rule_id, record.rule_name,  record.rule_settings, record.send_mail==True,
                        s['config'], s['topic']))
            
                    procs.append(proc)
            
                
                except Exception as e:
                    print(e)
                    self.util.send_mail("Ошибка проверки качества в Lottabyte", f'Ошибка выполнения проверки правила: {record.rule_name}\nОшибка : {e}\nДата проверки : {self.util.today}',self.util.dq_email_list)
            for p in procs:
                p.start()
            for proc in procs:
                proc.join()
            print('\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/')
        except Exception as e:
                print(e)
        
    def inputDataset(self, dataset, dq):
        input_facets = {
            "dataQuality": dq,
        }
        return InputDataset(dataset.namespace, dataset.name, input_facets)
  
    def check_not_null(self, id, name, settings,send_mail, config, topic):
      
        print(f'Выполняется проверка check_not_null')
        conf = json.loads(config)
        kafka_config = KafkaConfig(topic= 'linage',config= conf)
        kafka_transport = KafkaTransport(config = kafka_config)
        client = OpenLineageClient(transport=kafka_transport)
       

        consumer = Consumer({
            'bootstrap.servers': conf['bootstrap.servers'],
            'group.id': '80',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe([topic])
   
   

        producer = "DQ tool"
        transaction_in = Dataset(namespace="e570d56d-58f6-4f4b-9ea7-d2c105cec8a2", name="dq_kafka")
   
        job = Job(namespace="dq_rule/edit/97ee80ed-e74b-4bac-b74a-4385665b7de5", name="check_not_null")
        run_id = str(uuid.uuid4())
        run = None
        parent_id= None
        
        while True:
            message = consumer.poll(3)
            if(message == None):
                continue
            try:
                v = json.loads(message.value())
                
                dq = DataQualityAssertionsDatasetFacet(assertions = self.check_null(settings,v,id,name,send_mail))
                if(run==None):
                    parent_id = v['run_id']
                    run = Run(run_id,
                    facets={
                    "parent": ParentRunFacet.create(runId = parent_id, namespace="moex.transaction", name= "parentjob")
                    }
                    )
                    client.emit(
                        RunEvent(
                            RunState.START,
                            datetime.now().isoformat(),
                            run, job, producer,inputs=[self.inputDataset(transaction_in,dq)]
                        )
                    )
                else:
                    if (parent_id != v['run_id']):
                        client.emit(
                            RunEvent(
                                RunState.COMPLETE,
                                datetime.now().isoformat(),
                                run, job, producer,
                                inputs=[self.inputDataset(transaction_in,dq)],
                            )
                        )
                        parent_id = v['run_id']
                        run_id= str(uuid.uuid4())
                        run = Run(run_id,
                            facets={
                            "parent": ParentRunFacet.create(runId =  parent_id, namespace="moex.transaction", name= "parentjob")
                            }
                            )
                        client.emit(
                                RunEvent(
                                    RunState.START,
                                    datetime.now().isoformat(),
                                    run, job, producer,inputs=[self.inputDataset(transaction_in,dq)]
                                )
                            ) 
                    else:
                        client.emit(
                        RunEvent(
                            RunState.RUNNING,
                            datetime.now().isoformat(),
                            run, job, producer,inputs=[self.inputDataset(transaction_in,dq)]
                        )
                    )
                print ("%s:%d:%d: key=%s value=%s" % (message.topic(), message.partition(),
                                                    message.offset(), message.key(),
                                                    message.value()))
                
            
            except Exception as e:
                print(e)
                client.emit(
                    RunEvent(
                        RunState.FAIL,
                        datetime.now().isoformat(),
                        run, job, producer,
                        inputs=[transaction_in]
                    )
                )
        
        client.emit(
            RunEvent(
                RunState.COMPLETE,
                datetime.now().isoformat(),
                run, job, producer,
                inputs=[transaction_in],
            )
        )
                
            
        consumer.close()
    
    def check_null(self, settings, val,id,name,send_mail):       
        s = json.loads(settings)
        fields =  s['fields'].split(';')
        message = ''
        assertions : List[Assertion] = []
        for c in fields:
            print(f'Проверка поля {c}')
            if(c not in val):
                m = f'Поле {c} не заполнено(key - {val["key"]})'
                message = f'{message} {m}'
                assertions.append(Assertion(assertion = 'not_null',success = False,column = c, state="0", msg = m,rule_id = str(id)))
            else:
                assertions.append(Assertion(assertion = 'not_null',success = True,column = c,state="2", msg = None,rule_id = str(id)))
            
        if len(message) > 0: 
            self.log_entry(0, f'Не заполнены обязательные поля: {message}', id,name,send_mail)
        else:
            self.log_entry(1, f'Ошибок нет. Поля {s["fields"]} не пустые.', id,name,send_mail)
        return assertions
    
    ##########
    
    def check_avg(self, id, name, settings,send_mail, config, topic):
      
        print(f'Выполняется проверка check_avg')
        conf = json.loads(config)
        kafka_config = KafkaConfig(topic= 'linage',config= conf)
        kafka_transport = KafkaTransport(config = kafka_config)
        client = OpenLineageClient(transport=kafka_transport)
       

        consumer = Consumer({
            'bootstrap.servers': conf['bootstrap.servers'],
            'group.id': '90',
            'auto.offset.reset': 'earliest'
        })
        consumer.subscribe([topic])
   
   

        producer = "DQ tool"
        transaction_in = Dataset(namespace="e570d56d-58f6-4f4b-9ea7-d2c105cec8a2", name="dq_kafka")
        job = Job(namespace="dq_rule/edit/88a3e170-7268-48e1-b25b-0b9791027f4e", name="check_avg")
        run_id = str(uuid.uuid4())
        run = None
        parent_id= None
        
        while True:
            message = consumer.poll(3)
            if(message == None):
                continue
            try:
                v = json.loads(message.value())
                
                dq = DataQualityAssertionsDatasetFacet(assertions = self.check_avg_single(settings,v,id,name,send_mail))
                if(run==None):
                    parent_id = v['run_id']
                    run = Run(run_id,
                    facets={
                    "parent": ParentRunFacet.create(runId = parent_id, namespace="moex.transaction", name= "parentjob")
                    }
                    )
                    client.emit(
                        RunEvent(
                            RunState.START,
                            datetime.now().isoformat(),
                            run, job, producer,inputs=[self.inputDataset(transaction_in,dq)]
                        )
                    )
                else:
                    if (parent_id != v['run_id']):
                        client.emit(
                            RunEvent(
                                RunState.COMPLETE,
                                datetime.now().isoformat(),
                                run, job, producer,
                                inputs=[self.inputDataset(transaction_in,dq)],
                            )
                        )
                        parent_id = v['run_id']
                        run_id= str(uuid.uuid4())
                        run = Run(run_id,
                            facets={
                            "parent": ParentRunFacet.create(runId =  parent_id, namespace="moex.transaction", name= "parentjob")
                            }
                            )
                        client.emit(
                                RunEvent(
                                    RunState.START,
                                    datetime.now().isoformat(),
                                    run, job, producer,inputs=[self.inputDataset(transaction_in,dq)]
                                )
                            ) 
                    else:
                        client.emit(
                        RunEvent(
                            RunState.RUNNING,
                            datetime.now().isoformat(),
                            run, job, producer,inputs=[self.inputDataset(transaction_in,dq)]
                        )
                    )
                print ("%s:%d:%d: key=%s value=%s" % (message.topic(), message.partition(),
                                                    message.offset(), message.key(),
                                                    message.value()))
                
            
            except Exception as e:
                print(e)
                client.emit(
                    RunEvent(
                        RunState.FAIL,
                        datetime.now().isoformat(),
                        run, job, producer,
                        inputs=[transaction_in]
                    )
                )
        

    def get_avg(self,f):
        with self.pg_engine.connect() as conn:
            return  conn.execute(f'select avg({f}) from moex_poc.transactions').scalar()
        
    def check_avg_single(self, settings, val,id,name,send_mail):       
        s = json.loads(settings)
        fields =  s['fields'].split(';')
        message = ''
        assertions : List[Assertion] = []
        for f in fields:
            c =  f.split('|')[0]
            warning =  Decimal(f.split('|')[1])
            error =  Decimal(f.split('|')[2])
            avg = self.get_avg(c)
            state = 1
            v = Decimal(val[c])
            delta = abs(avg-v)
            if(100*delta/avg  > error):
                state = 0
            elif(100*delta/avg  > warning):
                state = 1
            else :
                state = 2
            
            print(f'Проверка поля {c}')
            m = f'Поле {c} значительно отличается от среднего ({ round(100*delta/avg,0) }%, key - {val["key"]})'
            assertions.append(Assertion(assertion = 'check_avg',success = (state==2),column = c, state=str(state), msg = m,rule_id = str(id)))
            
            if(state!=1):
                message = f'{message} {m}'
            
        if len(message) > 0: 
            self.log_entry(0, f'Поле значительно отличается от среднего: {message}', id,name,send_mail)
        else:
            self.log_entry(1, f'Ошибок нет. Поля {s["fields"]} не отличаются от среднего.', id,name,send_mail)
        return assertions
    
    
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
    
        #if status == 0 and  sm:
        #    self.util.send_mail("Ошибка проверки качества в Lottabyte (потоковые данные)", f'Название правила: {rule_name}\nОписание : {description}\nДата проверки : {self.util.today}',self.util.dq_email_list)
    

        with self.pg_engine.connect() as conn:
            stmt = insert(self.log).values(new_data)
            conn.execute(stmt)
        print(description)



d = DQKafkaSubscriber(util)
d.execute_rules()