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

# add/remove index on flow_url and job_url

# --- !Ups

create index ix_job_result_flow_url on job_result (flow_url) using HASH;
create index ix_job_result_job_url on job_result (job_url) using HASH;


# --- !Downs

drop index ix_job_result_flow_url ON job_result;
drop index ix_job_result_job_url ON job_result;
