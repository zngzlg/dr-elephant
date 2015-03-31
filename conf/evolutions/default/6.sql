# change col size

# --- !Ups

alter table job_result
  modify job_type            varchar(20);

# --- !Downs

alter table job_result
  modify job_type            varchar(6);
