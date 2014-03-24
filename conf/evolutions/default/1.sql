# --- !Ups

create table analysis_result (
  job_id                    varchar(50) not null,
  success                   tinyint(1) default 0,
  username                  varchar(50),
  start_time                bigint,
  analysis_time             bigint,
  url                       varchar(200),
  message                   varchar(200),
  data                      longtext,
  data_columns              integer,
  constraint pk_analysis_result primary key (job_id),
  INDEX idx_username (username),
  INDEX idx_analysis_time (analysis_time DESC)
  )
;




# --- !Downs

SET FOREIGN_KEY_CHECKS=0;

drop table analysis_result;

SET FOREIGN_KEY_CHECKS=1;

