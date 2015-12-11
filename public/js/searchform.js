/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

$(document).ready(function(){

    var form = $("#search-form");

    var jobid = $("#form-job-id");
    var flowurl = $("#form-flow-url");
    var user = $("#form-user");
    var jobtypeEnable = $("#form-jobtype-enable");
    var jobtype = $("#form-jobtype");
    var severityEnable = $("#form-severity-enable");
    var severity = $("#form-severity");
    var analysis = $("#form-analysis");
    var datetimeEnable = $("#form-datetime-enable");
    var startDate = $("#form-start-date");
    var endDate = $("#form-end-date");

    startDate.datepicker();
    endDate.datepicker();

    var updateForm = function(){
        if(jobid.val()) {
            flowurl.prop('disabled', true);
            user.prop('disabled', true);
            severity.prop('disabled', true);
            analysis.prop('disabled', true);
            jobtype.prop('disabled', true);
            jobtypeEnable.prop('disabled', true);
            severityEnable.prop('disabled', true);
            datetimeEnable.prop('disabled', true);
            startDate.prop('disabled', true);
            endDate.prop('disabled', true);
        } else if(flowurl.val()) {
            jobid.prop('disabled', true);
            user.prop('disabled', true);
            severity.prop('disabled', true);
            analysis.prop('disabled', true);
            jobtype.prop('disabled', true);
            jobtypeEnable.prop('disabled', true);
            severityEnable.prop('disabled', true);
            datetimeEnable.prop('disabled', true);
            startDate.prop('disabled', true);
            endDate.prop('disabled', true);
        }
        else{
            jobid.prop('disabled', false);
            flowurl.prop('disabled', false);
            jobtypeEnable.prop('disabled', false);
            severityEnable.prop('disabled', false);
            datetimeEnable.prop('disabled', false);
            user.prop('disabled', false);
            if(jobtypeEnable.prop('checked')){
                jobtype.prop('disabled', false);
            }
            else {
                jobtype.prop('disabled', true);
            }
            if(severityEnable.prop('checked')){
                severity.prop('disabled', false);
                analysis.prop('disabled', false);
            }
            else {
                severity.prop('disabled', true);
                analysis.prop('disabled', true);
            }
            if(datetimeEnable.prop('checked')){
                startDate.prop('disabled', false);
                endDate.prop('disabled', false);
            }
            else {
                startDate.prop('disabled', true);
                endDate.prop('disabled', true);
            }
        }
    }
    jobid.on("propertychange keyup input paste", updateForm);
    flowurl.on("propertychange keyup input paste", updateForm);
    jobtypeEnable.change(updateForm);
    severityEnable.change(updateForm);
    datetimeEnable.change(updateForm);

    form.submit(function(event){
        var data = form.serialize();
        localStorage.setItem('search-form', data);
        //Remove useless fields from the URL
        form.find('input[name]').filter(function(){return !$(this).val();}).attr('name', '');
    });

    try {
        var data = localStorage.getItem('search-form');
        form.deserialize(data);
    }
    catch(e){}

    updateForm();
});