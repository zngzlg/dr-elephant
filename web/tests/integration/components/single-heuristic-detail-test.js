import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('single-heuristic-detail', 'Integration | Component | single heuristic detail', {
  integration: true
});

test('it renders', function(assert) {

  this.set("yarnappheuristicresult", {
    name: "Mapper Data Skew",
    severity: "None",
    details: [
      {
        name: "Group A",
        value: "4 tasks @ 443 MB avg"
      },
      {
        name: "Group B",
        value: "53 tasks @ 464 MB avg"
      },
      {
        name: "Number of tasks",
        value: "57"
      }
    ]
  });

  this.render(hbs`{{single-heuristic-detail yarnappheuristicresult=yarnappheuristicresult}}`);

  assert.equal(this.$().text().trim().split("\n").join("").replace(/ /g, ''), 'MapperDataSkewSeverity:NoneGroupA4tasks@443MBavgGroupB53tasks@464MBavgNumberoftasks57');

});
