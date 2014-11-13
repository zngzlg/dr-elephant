# add/remove index on flow_exec_url

# --- !Ups

create index ix_job_result_flow_exec_url on job_result (flow_exec_url) using HASH;


# --- !Downs


drop index ix_job_result_flow_exec_url ON job_result;
