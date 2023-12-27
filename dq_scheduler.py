import sys
import json
import uuid
import croniter
from dq_utils import DQUtils
sys.path.append('./OpenLineage/client/python')
from openlineage.client.client import OpenLineageClient
from openlineage.client.transport import KafkaTransport,KafkaConfig
from openlineage.client.run import Job, Run, RunEvent, RunState,Dataset,InputDataset,OutputDataset
from openlineage.client.facet import (
    DataQualityAssertionsDatasetFacet,
    NominalTimeRunFacet
)

from sqlalchemy import select ,update
from sqlalchemy.dialects.postgresql import insert


import time
from datetime import datetime,timedelta


class DQScheduler:

    def __init__(self, util):
        
        self.tenant = util.tenant
        self.util = util
        self.now = datetime.now()
        self.dq_util =  DQUtils(util)

    def create_run(self,id):
        
        return Run(id,facets={
                    "nominalTime": NominalTimeRunFacet(
                        nominalStartTime=datetime.now().isoformat(),
                        nominalEndTime=(datetime.now() + timedelta(minutes=1)).isoformat(),
                    ),
                })
    
    def prepare_dq_tasks(self):

        try:
            self.pg_engine = self.util.get_pg_engine()
            self.log = self.util.get_pg_table('dq_log','dq')
            self.dq_tasks = self.util.get_pg_table('dq_tasks','dq')
            sql  = self.util.create_rule_to_connect_regular_for_schedule()
            print(sql)
            
           
                                                         
            while (True):
                with self.pg_engine.connect() as conn:
                    result = conn.execute(sql).fetchall()
                for record in result:
                    try:
                        if(len(record.rule_settings)==0): continue
                        s = json.loads(record.rule_settings)
                        if('crontab' in s ):
                            with self.pg_engine.connect() as conn:
                                    stmt = select(self.dq_tasks).where(self.dq_tasks.c.rule_id == str(record.entity_sample_to_dq_rule_id)).order_by(self.dq_tasks.c.time.desc())
                                    cur_row_date = conn.execute(stmt).fetchone()
                                
                            if(cur_row_date):
                                now = cur_row_date.time
                                cron = croniter.croniter(s['crontab'], now)
                                next_date = cron.get_next(datetime)
                                next_date = next_date.replace(second=0, microsecond=0)
                                if next_date<=datetime.now():
                                    with self.pg_engine.connect() as conn:
                                        stmt = insert(self.dq_tasks).values(id=str(uuid.uuid4()),time = next_date,rule_id = str(record.entity_sample_to_dq_rule_id) ,state = 0)
                                        conn.execute(stmt)
                            else:
                                now = datetime.now()
                                now = now.replace(second=0, microsecond=0)
                                with self.pg_engine.connect() as conn:
                                    stmt = insert(self.dq_tasks).values(id=str(uuid.uuid4()),time = now,rule_id = str(record.entity_sample_to_dq_rule_id) ,state = 0)
                                    conn.execute(stmt)
                            
                    
                    except Exception as e:
                        print(e)
                        self.util.send_mail("Ошибка проверки качества в Lottabyte", f'Ошибка выполнения проверки правила: {record.rule_name}\nОшибка : {e}\nДата проверки : {self.util.today}',self.util.dq_email_list)
                time.sleep(60)

        except Exception as e:
                print(e)
     
    
    def execute_rules(self):

        try:
            self.pg_engine = self.util.get_pg_engine()
            self.log = self.util.get_pg_table('dq_log','dq')
            self.dq_tasks = self.util.get_pg_table('dq_tasks','dq')
            sql  = self.util.create_rule_to_connect_for_schedule()
            
            producer = self.util.kafka_producer
            topic = self.util.kafka_producer_topic
            conf = {'bootstrap.servers': self.util.kafka_bootstrap_servers}
            kafka_config = KafkaConfig(topic=topic,config= conf)
            kafka_transport = KafkaTransport(config = kafka_config)
            client = OpenLineageClient(transport=kafka_transport)
                                                         
            while (True):
                with self.pg_engine.connect() as conn:
                    result = conn.execute(sql).fetchall()
                for record in result:
                    try:
                        
                        run_id = str(uuid.uuid4())

                        run = self.create_run(run_id)
                        job = Job(namespace=self.util.kafka_namespace, name="check_dq")
                        
                        transaction_in = Dataset(namespace=str(record.data_asset_id if record.data_asset_id else record.data_asset_id1), name="transaction")
                        with self.pg_engine.connect() as conn:
                            stmt = update(self.dq_tasks).values(state=1).where(self.dq_tasks.c.id == str(record.dq_tasks_id))
                            conn.execute(stmt)
                        
                        assertions = getattr(self.dq_util, record.rule_ref)(
                                record.entity_sample_to_dq_rule_id, record.rule_name,  record.rule_settings, record.send_mail==True,
                                self.util.create_pg_connection_string (record.url, record.username, record.password)) 
                        dq = DataQualityAssertionsDatasetFacet(assertions)
                       
                        client.emit(
                                RunEvent(
                                    RunState.START,
                                    datetime.now().isoformat(),
                                    run, job, producer,
                                    
                                    inputs=[self.inputDataset(transaction_in,dq)]
                                )
                            )
                        print("1")
                        client.emit(
                                RunEvent(
                                    RunState.COMPLETE,
                                    datetime.now().isoformat(),
                                    run, job, producer
                                    
                                )
                            )
                        with self.pg_engine.connect() as conn:
                            stmt = update(self.dq_tasks).values(state=2).where(self.dq_tasks.c.id == str(record.dq_tasks_id))
                            conn.execute(stmt)   
                
                
                
                    
                    except Exception as e:
                        print(e)
                        self.util.send_mail("Ошибка проверки качества в Lottabyte", f'Ошибка выполнения проверки правила: {record.rule_name}\nОшибка : {e}\nДата проверки : {self.util.today}',self.util.dq_email_list)
                time.sleep(30)

        except Exception as e:
                print(e)
        
    def inputDataset(self, dataset, dq):
        input_facets = {
            "dataQuality": dq,
        }
        return InputDataset(dataset.namespace, dataset.name, input_facets)

