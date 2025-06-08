drop table if exists finance.hdfc_insta_alters_new;
CREATE EXTERNAL TABLE finance.hdfc_insta_alters_new (
	message_id STRING,
	imap_id INT,
	mail_from STRING,
	mail_to STRING,
	received_ts STRING,
	subject STRING,
	body STRING,
	money INT,
	transation_type STRING,
	account_number STRING,
	external_account STRING,
	transation_date DATE,
	transation_ref_no STRING,
	widrawal_location STRING
)
PARTITIONED BY (
    received_dt date
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/finance_new.db'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'serialization.format'='1',
    'skip.header.line.count'='0'
);
