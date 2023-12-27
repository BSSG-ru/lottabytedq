import sys
import uuid
import json
from datetime import datetime
from confluent_kafka import Consumer
from utils  import Utils
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert


util = Utils()
pg_engine = util.get_pg_engine()
openlinage_log = util.get_pg_table('openlinage_log',f'da_{util.tenant}')
openlinage_log_extended = util.get_pg_table('openlinage_log_extended',f'da_{util.tenant}')
openlinage_log_assertions = util.get_pg_table('openlinage_log_assertions',f'da_{util.tenant}')
openlinage_log_inputs = util.get_pg_table('openlinage_log_inputs',f'da_{util.tenant}')
openlinage_log_outputs = util.get_pg_table('openlinage_log_outputs',f'da_{util.tenant}')

topic = util.kafka_producer_topic
consumer = Consumer({
    'bootstrap.servers': util.kafka_bootstrap_servers,
    'group.id': '2',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([topic])
while True:
    try:
        message = consumer.poll(1)
        if(message == None):
            continue
        v = json.loads(message.value())
        if('run' not in v):
            print(f'ERROR {v}')
            continue
        flag = True
        with util.pg_engine.connect() as conn:

            id=uuid.uuid4()
            try:
                stmt = insert(openlinage_log).values(id=str(id),event_time = v['eventTime'],event_type = v['eventType'],producer = v['producer'],run_id = str(v['run']['runId']),
                                                        parent_run_id = str(v['run']['facets']['parent']['run']['runId']) if  'parent' in v['run']['facets'] else None,
                                                        nominal_end_time = v['run']['facets']['nominalTime']['nominalEndTime'] if  'nominalTime' in v['run']['facets'] else None,
                                                        nominal_start_time = v['run']['facets']['nominalTime']['nominalStartTime'] if  'nominalTime' in v['run']['facets'] else None
                                                        )
                conn.execute(stmt)
            except Exception as e:
                print(e)
            if(flag and v['eventType'] =='COMPLETE'):
                based_end_time = conn.execute(select(openlinage_log_extended.c.based_end_time).where(openlinage_log_extended.c.id == str(id))).scalar()
                if(based_end_time):
                    eventTime =v['eventTime']
                    diff = (based_end_time - datetime.strptime(eventTime, '%Y-%m-%dT%H:%M:%S.%f') ).total_seconds()
                    flag = False
                    if(based_end_time < datetime.strptime(eventTime, '%Y-%m-%dT%H:%M:%S.%f')):
                        try:
                            stmt = insert(openlinage_log_assertions).values(id=str(uuid.uuid4()),assertion = 'time_expired',success = False,state = 0,ol_id = str(id),msg = f'Превышено время выполнения на {diff} секунд',rule_id = '389df21d-5294-4cd5-ad61-63b62ee07b54')
                            conn.execute(stmt)
                        except Exception as e:
                                print(e)
                    else:
                        try:
                            stmt = insert(openlinage_log_assertions).values(id=str(uuid.uuid4()),assertion = 'time_expired',success = True,state = 2,ol_id = str(id),msg = '',rule_id = '389df21d-5294-4cd5-ad61-63b62ee07b54')
                            conn.execute(stmt)
                        except Exception as e:
                                print(e)
                
            for i in v['outputs']:
                try:
                    stmt = insert(openlinage_log_outputs).values(id=str(uuid.uuid4()),ns = i['namespace'],name = i['name'],ol_id = str(id))
                    conn.execute(stmt)
                except Exception as e:
                            print(e)
            for i in v['inputs']:
                try:
                    stmt = insert(openlinage_log_inputs).values(id=str(uuid.uuid4()),ns = i['namespace'],name = i['name'],ol_id = str(id))
                    conn.execute(stmt)
                except Exception as e:
                            print(e)
                if('facets' in i and 'dataQuality' in i['facets']):
                    for a in i['facets']['dataQuality']['assertions']:
                        try:
                            stmt = insert(openlinage_log_assertions).values(id=str(uuid.uuid4()),assertion = a['assertion'],column = a['column'],success = a['success'],state = a['state'],msg = a['msg'] if 'msg' in a else None,ol_id = str(id),rule_id = a['rule_id'] if 'rule_id' in a else None)
                            conn.execute(stmt)
                        except Exception as e:
                            print(e)
            
    except Exception as e:
        print(e)


consumer.close()
