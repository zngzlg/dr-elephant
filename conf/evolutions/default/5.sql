# add/remove index

# --- !Ups

create index ix_combined_severity_analysis_name on job_heuristic_result (severity, analysis_name);
drop index ix_job_result_severity_1 on job_result;
drop index ix_job_result_cluster_1 on job_result;

# --- !Downs

drop index ix_combined_severity_analysis_name on job_heuristic_result;
create index ix_job_result_severity_1 on job_result (severity);
create index ix_job_result_cluster_1 on job_result (cluster);