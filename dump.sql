-- Drop table

-- DROP TABLE da_1000.dq_log;

CREATE TABLE da_1000.dq_log (
	id serial4 NOT NULL,
	"date" timestamp NOT NULL,
	status int4 NOT NULL,
	description text NULL,
	rule_id text NOT NULL,
	tenant text NULL,
	CONSTRAINT dq_log_pk PRIMARY KEY (id)
);



-- Drop table

-- DROP TABLE da_1000.exceptions_log;

CREATE TABLE da_1000.exceptions_log (
	id bigserial NOT NULL,
	"date" timestamp NOT NULL,
	message text NOT NULL,
	"type" text NULL,
	CONSTRAINT exceptions_log_pk PRIMARY KEY (id)
);


-- Drop table

-- DROP TABLE da_1000.openlinage_log;

CREATE TABLE da_1000.openlinage_log (
	id uuid NOT NULL,
	event_time timestamp NULL,
	event_type varchar NULL,
	producer varchar NULL,
	run_id uuid NULL,
	parent_run_id uuid NULL,
	nominal_end_time timestamp NULL,
	nominal_start_time timestamp NULL,
	CONSTRAINT openlinage_log_pk PRIMARY KEY (id)
);



-- Drop table

-- DROP TABLE da_1000.openlinage_log_assertions;

CREATE TABLE da_1000.openlinage_log_assertions (
	"assertion" varchar NULL,
	"column" varchar NULL,
	success bool NULL,
	id uuid NOT NULL,
	ol_id uuid NULL,
	state varchar NULL,
	msg varchar NULL,
	rule_id uuid NULL,
	CONSTRAINT openlinage_log_assertions_pk PRIMARY KEY (id)
);


-- Drop table

-- DROP TABLE da_1000.openlinage_log_inputs;

CREATE TABLE da_1000.openlinage_log_inputs (
	"name" varchar NULL,
	ns varchar NULL,
	id uuid NOT NULL,
	ol_id uuid NULL,
	CONSTRAINT openlinage_log_inputs_pk PRIMARY KEY (id)
);



-- Drop table

-- DROP TABLE da_1000.openlinage_log_outputs;

CREATE TABLE da_1000.openlinage_log_outputs (
	"name" varchar NULL,
	ns varchar NULL,
	id uuid NOT NULL,
	ol_id uuid NULL,
	CONSTRAINT openlinage_log_outputs_pk PRIMARY KEY (id)
);


CREATE OR REPLACE VIEW da_1000.connector_params
AS SELECT sc.name AS connection_name,
    cp.name AS param_name,
    scp.param_value,
    scp.system_connection_id
   FROM da_1000.system_connection_param scp
     LEFT JOIN da.connector_param cp ON cp.id = scp.connector_param_id
     LEFT JOIN da_1000.system_connection sc ON sc.id = scp.system_connection_id;


CREATE OR REPLACE VIEW da_1000.dq_rule_tasks
AS SELECT r.id,
    s.id AS system_id,
    s.name AS system_name,
    dr.name AS rule_name,
    dr.id AS rule_id,
    dr.rule_ref,
    r.settings AS rule_settings,
    eq.name AS query_name,
    r.id AS entity_sample_to_dq_rule_id,
    e.name AS entity_name,
    p.name AS product_name,
    i.name AS indicator_name,
    da.name AS data_asset_name,
    p.id AS product_id,
    i.id AS indicator_id,
    da.id AS data_asset_id,
    es.id AS entity_sample_id,
    es.name AS entity_sample_name,
        CASE
            WHEN r.settings ~~ '%crontab%'::text THEN 1
            ELSE 0
        END AS is_crontab
   FROM da_1000.entity_sample_to_dq_rule r
     LEFT JOIN da_1000.dq_rule dr ON r.dq_rule_id = dr.id
     LEFT JOIN da_1000.entity_sample es ON r.entity_sample_id = es.id
     LEFT JOIN da_1000.entity_query eq ON es.entity_query_id = eq.id
     LEFT JOIN da_1000.entity e ON e.id = eq.entity_id
     LEFT JOIN da_1000.task t ON t.query_id = eq.id
     LEFT JOIN da_1000.indicator i ON i.id = r.indicator_id
     LEFT JOIN da_1000.product p ON p.id = r.product_id
     LEFT JOIN da_1000.system s ON s.id = eq.system_id
     LEFT JOIN da_1000.data_asset da ON r.asset_id = da.id
  WHERE (r.disabled = false OR r.disabled IS NULL) AND (p.state = 'PUBLISHED'::text OR p.state IS NULL) AND (i.state = 'PUBLISHED'::text OR i.state IS NULL) AND (da.state = 'PUBLISHED'::text OR da.state IS NULL) AND dr.rule_ref IS NOT NULL;



CREATE OR REPLACE VIEW da_1000.openlinage_log_assertions_monitor
AS SELECT
        CASE
            WHEN t.state::text = '0'::text THEN 'Ошибка'::text
            WHEN t.state::text = '1'::text THEN 'Предупреждение'::text
            ELSE 'Успешно'::text
        END AS state_name,
    dr.name AS rule_name,
    t."column",
    t.msg,
    t.rule_id,
    t.ol_id,
    t.id,
    ol.run_id,
    t.assertion,
    t.state
   FROM da_1000.openlinage_log_assertions t
     LEFT JOIN da_1000.entity_sample_to_dq_rule estdr ON estdr.id = t.rule_id
     LEFT JOIN da_1000.dq_rule dr ON dr.id = estdr.dq_rule_id
     LEFT JOIN da_1000.openlinage_log ol ON ol.id = t.ol_id;


CREATE OR REPLACE VIEW da_1000.openlinage_log_extended
AS SELECT dq1.provide_end_time(openlinage_log.parent_run_id) AS based_end_time,
    openlinage_log.id,
    openlinage_log.event_time,
    openlinage_log.event_type,
    openlinage_log.producer,
    openlinage_log.run_id,
    openlinage_log.parent_run_id,
    openlinage_log.nominal_end_time,
    openlinage_log.nominal_start_time
   FROM da_1000.openlinage_log;


CREATE OR REPLACE VIEW da_1000.openlinage_log_monitor
AS SELECT
        CASE
            WHEN (EXISTS ( SELECT 1
               FROM da_1000.openlinage_log ol1
              WHERE ol1.event_type::text = 'FAIL'::text AND ol1.run_id = t.run_id)) THEN 'Ошибка выполнения'::text
            WHEN NOT (EXISTS ( SELECT 1
               FROM da_1000.openlinage_log ol1
              WHERE ol1.event_type::text = 'COMPLETE'::text AND ol1.run_id = t.run_id)) THEN 'Незавершен'::text
            WHEN t.state = '0'::text THEN 'Ошибка'::text
            WHEN t.state = '1'::text THEN 'Предупреждение'::text
            ELSE 'Успешно'::text
        END AS state_name,
        CASE
            WHEN (EXISTS ( SELECT 1
               FROM da_1000.openlinage_log ol1
              WHERE ol1.event_type::text = 'FAIL'::text AND ol1.run_id = t.run_id)) THEN 'Ошибка выполнения'::text
            WHEN NOT (EXISTS ( SELECT 1
               FROM da_1000.openlinage_log ol1
              WHERE ol1.event_type::text = 'COMPLETE'::text AND ol1.run_id = t.run_id)) THEN 'Незавершен'::text
            WHEN t.state_local = '0'::text THEN 'Ошибка'::text
            WHEN t.state_local = '1'::text THEN 'Предупреждение'::text
            ELSE 'Успешно'::text
        END AS state_name_local,
    ( SELECT a.msg
           FROM da_1000.openlinage_log_assertions a
          WHERE t.state = a.state::text AND a.msg IS NOT NULL AND a.ol_id = t.id OR (a.ol_id IN ( SELECT dq1.id
                   FROM da_1000.openlinage_log dq1
                  WHERE dq1.parent_run_id = t.run_id))
         LIMIT 1) AS assertion_msg,
    t.state,
    t.state_local,
    t.id,
    t.run_id,
    t.parent_run_id,
    t.event_type,
    t.event_time,
    t.full_name,
    t.system_producer,
    t.input_asset_domain_id,
    t.output_asset_domain_id,
    t.output_asset_domain_name,
    t.input_asset_domain_name,
    t.system_producer_parent,
    t.input_name,
    t.input_ns,
    t.input_asset_name,
    t.input_id,
    t.input_system_id,
    t.input_system_name,
    t.output_name,
    t.output_ns,
    t.output_asset_name,
    t.output_id,
    t.output_system_id,
    t.output_system_name,
    t.producer,
    t.output_asset_id,
    t.input_asset_id
   FROM ( SELECT ( SELECT min(a.state::text) AS min
                   FROM da_1000.openlinage_log_assertions a
                  WHERE a.ol_id = dq.id OR (a.ol_id IN ( SELECT dq1.id
                           FROM da_1000.openlinage_log dq1
                          WHERE dq1.parent_run_id = dq.run_id))) AS state,
            ( SELECT min(a.state::text) AS min
                   FROM da_1000.openlinage_log_assertions a
                  WHERE (a.ol_id IN ( SELECT dq1.id
                           FROM da_1000.openlinage_log dq1
                          WHERE dq1.run_id = dq.run_id))) AS state_local,
            dq.id,
            dq.run_id,
            dq.parent_run_id,
            dq.event_type,
            dq.event_time,
            da_1000.provide_parent_name(dq.run_id) AS full_name,
            sdai.name AS system_producer,
            dai.domain_id AS input_asset_domain_id,
            dao.domain_id AS output_asset_domain_id,
            daod.name AS output_asset_domain_name,
            daid.name AS input_asset_domain_name,
            da_1000.provide_parent_name(dq.run_id) AS system_producer_parent,
            oli.name AS input_name,
            oli.ns AS input_ns,
            dai.name AS input_asset_name,
            oli.id AS input_id,
            sdai.id AS input_system_id,
            sdai.name AS input_system_name,
            olo.name AS output_name,
            olo.ns AS output_ns,
            dao.name AS output_asset_name,
            olo.id AS output_id,
            sdao.id AS output_system_id,
            sdao.name AS output_system_name,
            dq.producer,
            dao.id AS output_asset_id,
            dai.id AS input_asset_id
           FROM da_1000.openlinage_log dq
             LEFT JOIN da_1000.openlinage_log_inputs oli ON oli.ol_id = dq.id
             LEFT JOIN da_1000.openlinage_log_outputs olo ON olo.ol_id = dq.id
             LEFT JOIN da_1000.data_asset dai ON dai.id = oli.ns::uuid
             LEFT JOIN da_1000.system sdai ON sdai.id = dai.system_id
             LEFT JOIN da_1000.data_asset dao ON dao.id = olo.ns::uuid
             LEFT JOIN da_1000.system sdao ON sdao.id = dao.system_id
             LEFT JOIN da_1000.domain daod ON daod.id = dao.domain_id
             LEFT JOIN da_1000.domain daid ON daid.id = dai.domain_id
          WHERE dq.event_type::text = 'START'::text
          ORDER BY dq.run_id, dq.event_time) t;



CREATE OR REPLACE VIEW da_1000.openlinage_log_monitor_draft
AS SELECT t.state_name,
    t.state_name_local,
    t.assertion_msg,
    t.state,
    t.state_local,
    t.id,
    t.run_id,
    t.parent_run_id,
    t.event_type,
    t.event_time,
    t.full_name,
    t.system_producer,
    t.input_asset_domain_id,
    t.output_asset_domain_id,
    t.output_asset_domain_name,
    t.input_asset_domain_name,
    t.system_producer_parent,
    t.input_name,
    t.input_ns,
    t.input_asset_name,
    t.input_id,
    t.input_system_id,
    t.input_system_name,
    t.output_name,
    t.output_ns,
    t.output_asset_name,
    t.output_id,
    t.output_system_id,
    t.output_system_name,
    t.producer,
    t.output_asset_id,
    t.input_asset_id
   FROM da_1000.openlinage_log_monitor t
  WHERE t.parent_run_id IS NULL;

-- Permissions

ALTER TABLE da_1000.openlinage_log_monitor_draft OWNER TO pgbouncer;
GRANT ALL ON TABLE da_1000.openlinage_log_monitor_draft TO pgbouncer;

-- DROP FUNCTION da_1000.provide_end_time(uuid);

CREATE OR REPLACE FUNCTION da_1000.provide_end_time(id_offer uuid)
 RETURNS timestamp without time zone
 LANGUAGE plpgsql
AS $function$
declare
   rec record;                                
begin
   select run_id , nominal_end_time, parent_run_id 
   into rec                                   
   from da_1000.openlinage_log
   where run_id  = id_offer and event_type = 'COMPLETE';
   
   if rec.parent_run_id is null then                       
    return rec.nominal_end_time;                          
   else
    return dq.provide_end_time(rec.parent_run_id); 
   end if;
end;
$function$
;


-- DROP FUNCTION da_1000.provide_full_name(uuid, text);

CREATE OR REPLACE FUNCTION da_1000.provide_full_name(id_offer uuid, full_name text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
   rec record;                                
begin
   select run_id , system_producer, parent_run_id 
   into rec                                   
   from da_1000.openlinage_log_monitor_draft
   where run_id  = id_offer and event_type = 'START';
   
   if rec.parent_run_id is null then                       
    return rec.system_producer::text ;                          
   else
    return da_1000.provide_full_name(rec.parent_run_id,rec.system_producer::text + '/'+ full_name ); 
   end if;
end;
$function$
;


-- DROP FUNCTION da_1000.provide_parent_name(uuid);

CREATE OR REPLACE FUNCTION da_1000.provide_parent_name(parent_id uuid)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
   rec record; 
   res text;
   last_id uuid;
   last_prev_id uuid;
  
begin
   	select  concat(edai."name",' (',sdai."name" ,')') system_producer, edai."name" input_name  into rec    
  	from da_1000.openlinage_log dq
	left join da_1000.openlinage_log_inputs oli on oli.ol_id = dq.id 
	left join da_1000.openlinage_log_outputs olo  on olo.ol_id = dq.id
	left join da_1000.data_asset dai on   dai.id = uuid(oli.ns)
	left join da_1000.system sdai on   sdai.id = dai.system_id 
	left join da_1000.entity edai on   edai.id = dai.entity_id
	left join da_1000.data_asset dao on dao.id = uuid(olo.ns)
	left join da_1000.system sdao on   sdao.id = dao.system_id 
   	where dq.run_id  = parent_id and event_type = 'START' limit 1;
   	res = rec.system_producer;
   	if rec.input_name is null then res = ''; end if;
   	
   	last_id = parent_id;
   	while last_id is not null loop
      last_prev_id = last_id;
	  select  dq.run_id  into rec from da_1000.openlinage_log dq 
	 where dq.parent_run_id = last_id and event_type = 'START'  and  dq.producer <> 'DQ tool' limit 1;
      last_id = rec.run_id ;
	  
   	end loop;
   	
   	select  concat(edao."name",' (',sdao."name" ,')') system_producer, edao."name" output_name  into rec    
  	from da_1000.openlinage_log dq
	left join da_1000.openlinage_log_inputs oli on oli.ol_id = dq.id 
	left join da_1000.openlinage_log_outputs olo  on olo.ol_id = dq.id
	left join da_1000.data_asset dai on   dai.id = uuid(oli.ns)
	left join da_1000.system sdai on   sdai.id = dai.system_id 
	left join da_1000.entity edai on   edai.id = dai.entity_id
	left join da_1000.data_asset dao on dao.id = uuid(olo.ns)
	left join da_1000.system sdao on   sdao.id = dao.system_id 
	left join da_1000.entity edao on   edao.id = dao.entity_id
   	where dq.run_id  = last_prev_id and event_type = 'START' and dq.producer <> 'DQ tool'  limit 1;
   
   	if rec.output_name is null then 
   		return res ;  
   	else
   		return concat(res ,' - ',rec.system_producer) ; 
   	end if;
  
                       
   
end;
$function$
;

