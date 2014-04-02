$(document).ready(function(){

    var form = $("#search-form");


    var jobid = $("#form-job-id");
    var user = $("#form-user");
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
            user.prop('disabled', true);
            severity.prop('disabled', true);
            analysis.prop('disabled', true);
            severityEnable.prop('disabled', true);
            datetimeEnable.prop('disabled', true);
            startDate.prop('disabled', true);
            endDate.prop('disabled', true);
        }
        else{
            severityEnable.prop('disabled', false);
            datetimeEnable.prop('disabled', false);
            user.prop('disabled', false);
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