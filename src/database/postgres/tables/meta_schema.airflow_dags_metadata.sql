drop table if exists meta_schema.airflow_dags_metadata;

CREATE TABLE meta_schema.airflow_dags_metadata (
  batch_name varchar(50),
  sub_task varchar(50),
  parm_name varchar(20),
  parm_value varchar(200),
  last_update_ts TIMESTAMPTZ
);
