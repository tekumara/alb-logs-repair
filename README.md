# alb-logs-repair
Identify and add missing partitions to a partitioned alb_logs Athena table

If you have created a partition Athena table for your ALB logs, eg:
```
CREATE EXTERNAL TABLE `alb_logs`(
  `type` string COMMENT '', 
  `time` string COMMENT '', 
  `elb` string COMMENT '', 
  `client_ip` string COMMENT '', 
  `client_port` int COMMENT '', 
  `target_ip` string COMMENT '', 
  `target_port` int COMMENT '', 
  `request_processing_time` double COMMENT '', 
  `target_processing_time` double COMMENT '', 
  `response_processing_time` double COMMENT '', 
  `elb_status_code` string COMMENT '', 
  `target_status_code` string COMMENT '', 
  `received_bytes` bigint COMMENT '', 
  `sent_bytes` bigint COMMENT '', 
  `request_verb` string COMMENT '', 
  `request_url` string COMMENT '', 
  `request_proto` string COMMENT '', 
  `user_agent` string COMMENT '', 
  `ssl_cipher` string COMMENT '', 
  `ssl_protocol` string COMMENT '', 
  `target_group_arn` string COMMENT '', 
  `trace_id` string COMMENT '', 
  `domain_name` string COMMENT '', 
  `chosen_cert_arn` string COMMENT '', 
  `matched_rule_priority` string COMMENT '', 
  `request_creation_time` string COMMENT '', 
  `actions_executed` string COMMENT '', 
  `redirect_url` string COMMENT '', 
  `new_field` string COMMENT '')
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.RegexSerDe' 
WITH SERDEPROPERTIES ( 
  'input.regex'='([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" \"([^\"]*)\" ([A-Z0-9-]+) ([A-Za-z0-9.-]*) ([^ ]*) \"([^\"]*)\" \"([^\"]*)\" \"([^\"]*)\" ([-.0-9]*) ([^ ]*) \"([^\"]*)\"($| \"[^ ]*\")(.*)') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://elb-access-logs-123456789012/my-app/AWSLogs/123456789012/elasticloadbalancing/ap-southeast-2'
```

you'll know that objects aren't laid out in a partition friendly way, eg: with prefix `year=YYYY/month=MM/day=DD`. So while `MSCK REPAIR TABLE` can identify missing partitions it won't be able to add them.

This script will use `MSCK REPAIR TABLE` to identify missing partitions, and then individually add each one.

## Pre-requisites

You have a partitioned table called `alb_logs` defined as above.  
You have the aws cli installed.

## Usage

Run `alb-logs.repair.sh` to see usage instructions. 

