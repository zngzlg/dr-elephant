import Ember from 'ember';

export default Ember.Route.extend({
    beforeModel: function(transition){
        this.workflowid = transition.queryParams.workflowid;
    },
    model(){
        this.workflows = this.store.queryRecord('workflow',{workflowid: this.get("workflowid")});
        return this.workflows;
    }
});
