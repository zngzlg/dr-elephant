#
# Copyright 2016 LinkedIn Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

# --- !Ups

create table job_heuristic_result (
  id                        integer auto_increment not null,
  job_job_id                varchar(50),
  severity                  integer,
  analysis_name             varchar(255),
  data                      longtext,
  data_columns              integer,
  constraint ck_job_heuristic_result_severity check (severity in ('2','4','1','3','0')),
  constraint pk_job_heuristic_result primary key (id))
;

create table job_result (
  job_id                    varchar(50) not null,
  username                  varchar(50),
  job_name                  varchar(100),
  start_time                bigint,
  analysis_time             bigint,
  severity                  integer,
  job_type                  varchar(6),
  url                       varchar(200),
  cluster                   varchar(100),
  meta_urls                 longtext,
  constraint ck_job_result_severity check (severity in ('2','4','1','3','0')),
  constraint ck_job_result_job_type check (job_type in ('Pig','Hive','Hadoop')),
  constraint pk_job_result primary key (job_id))
;

alter table job_heuristic_result add constraint fk_job_heuristic_result_job_1 foreign key (job_job_id) references job_result (job_id) on delete restrict on update restrict;
create index ix_job_heuristic_result_job_1 on job_heuristic_result (job_job_id);
create index ix_job_result_username_1 on job_result (username);
create index ix_job_result_analysis_time_1 on job_result (analysis_time);
create index ix_job_result_severity_1 on job_result (severity);
create index ix_job_result_cluster_1 on job_result (cluster);



# --- !Downs

SET FOREIGN_KEY_CHECKS=0;

drop table job_heuristic_result;

drop table job_result;

SET FOREIGN_KEY_CHECKS=1;

# --- Created by Ebean DDL
# To stop Ebean DDL generation, remove this comment and start using Evolutions
