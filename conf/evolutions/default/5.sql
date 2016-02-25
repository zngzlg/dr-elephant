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

# add/remove index

# --- !Ups

create index ix_combined_severity_analysis_name on job_heuristic_result (severity, analysis_name);
drop index ix_job_result_severity_1 on job_result;
drop index ix_job_result_cluster_1 on job_result;

# --- !Downs

drop index ix_combined_severity_analysis_name on job_heuristic_result;
create index ix_job_result_severity_1 on job_result (severity);
create index ix_job_result_cluster_1 on job_result (cluster);