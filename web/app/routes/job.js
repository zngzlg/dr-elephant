import Ember from 'ember';

export default Ember.Route.extend({
    beforeModel: function (transition) {
        this.jobid = transition.queryParams.jobid;
    },
    model(){
        this.jobs = this.store.queryRecord('job', {jobid: this.get("jobid")});
        return this.jobs;
    }
});
