# change col size

# --- !Ups

alter table job_result
  modify column job_exec_url              varchar(2048),
  modify column job_url                   varchar(2048),
  modify column flow_exec_url             varchar(2048),
  modify column flow_url                  varchar(2048);

# --- !Downs

alter table job_result
  modify column job_exec_url              varchar(200),
  modify column job_url                   varchar(200),
  modify column flow_exec_url             varchar(200),
  modify column flow_url                  varchar(200);
