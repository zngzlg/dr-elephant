# Add urls

# --- !Ups

alter table job_result
  add column job_exec_url              varchar(200),
  add column job_url                   varchar(200),
  add column flow_exec_url             varchar(200),
  add column flow_url                  varchar(200),
  drop column meta_urls;

# --- !Downs

alter table job_result
  drop column job_exec_url
  drop column job_url
  drop column flow_exec_utl
  drop column flow_url
  add column meta_urls                 longtext;
