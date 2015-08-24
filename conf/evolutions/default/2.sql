#
# Copyright 2015 LinkedIn Corp.
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
