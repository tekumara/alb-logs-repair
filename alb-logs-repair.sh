#!/usr/bin/env bash

function die() {
    >&2 echo -e "$*"
    exit 1
}

function usage() {
    die "Identify and add missing partitions to a partitioned alb_logs athena table 

    Usage: $0 <alb_logs> <output_location>
           <alb_logs> = s3 url of your alb logs
           <output_location> = s3 url to store results of athena queries
       
       eg: $0 s3://elb-access-logs-123456789012/my-app/AWSLogs/123456789012/elasticloadbalancing/ap-southeast-2 s3://aws-athena-query-results-123456789012-ap-southeast-2/
    "
}

set -oe pipefail
trap 'die "Unhandled error on or near line ${LINENO}"' ERR

alb_logs="$1"
output_location="$2"
query_string="MSCK REPAIR TABLE alb_logs;"

[ -z "$alb_logs" ] || [ -z "$output_location" ] && usage

echo "$query_string"
query_id=$(aws athena start-query-execution \
   --query-string "$query_string" \
   --query-execution-context Database=default \
   --result-configuration OutputLocation="$output_location" \
   --output text)

# wait until query has completed
{
	query_execution=""
	function query_execution_state() {
		jq -rn "$query_execution | .QueryExecution.Status.State"	
	}
	
	while [[ -z "$query_execution" ]] || [[ $(query_execution_state) == "RUNNING" ]]; do
		query_execution=$(aws athena get-query-execution --query-execution-id "$query_id")
		query_execution_state
		sleep 2
	done

	if [[ ! $(query_execution_state) == "SUCCEEDED" ]]; then
		exit 1
	fi	
}

# fetch, parse, generate and execute add partition statements for partitions not in the metastore
aws s3 cp $(jq -rn "$query_execution | .QueryExecution.ResultConfiguration.OutputLocation") - \
	| sed $'s/\t/\\\n/g' \
	| perl -ne 'if (/alb_logs:(\d{4})\/(\d\d)\/(\d\d)/) { print "ALTER TABLE alb_logs ADD PARTITION (year=\"$1\", month=\"$2\", day=\"$3\") LOCATION \"'"$alb_logs"'/$1/$2/$3/\";\n";}' \
	| while read sql; do
		echo "$sql"	
	    aws athena start-query-execution \
		    --query-string "$sql" \
	    	--query-execution-context Database=default \
	    	--result-configuration OutputLocation="$output_location" \
	    	--output text
	  done
