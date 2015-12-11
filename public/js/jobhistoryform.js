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

  /* Plot graph for data obtained from ajax call */
  $.getJSON('/rest/jobgraphdata?url=' + queryString()['historyjoburl'], function(data) {
    updateExecTimezone(data);
    plotter(data, []);
  });

  loadTableTooltips();
});

/**
 * Example tooltip content:
 *
 * Execution 1
 * Job score = 163672
 * Top poor stages:
 * Stage 1   65%
 * Stage 26  25%
 * Stage 12  11%
 */
function getGraphTooltipContent(record, jobDefList) {

  var content = ["<b>" + record.flowtime + "</b>"];
  content.push("Job Score = " + record.score);
  if (record.score != 0) {
    var jobLimit = 3;
    content.push("Top poor stages:");

    var scoreList = [];
    for (var i = 0; i < record.stagescores.length; i++) {
      var scoreWidth = record.stagescores[i]["stagescore"] * 100 / record.score;
      scoreList.push([scoreWidth, i]);
    }

    scoreList.sort(function(left, right) {
      return left[0] > right[0] ? -1 : 1;
    });

    var stageRows = "";
    for (var stageIndex = 0;  stageIndex < scoreList.length; stageIndex++) {

      var width = scoreList[stageIndex][0];
      var index = scoreList[stageIndex][1];

      // Skip after jobLimit jobs are captured or when width becomes 0.
      if (stageIndex >= jobLimit || width == 0) {
        break;
      }

      // cell 1
      var cell1 = "<td width='65px' style='padding:3px;'>Stage " + (index + 1) + "</td>";

      // cell 2
      var stageScoreRect = "<div style='padding:3px;background:red;width:" + width + "%'>" + +width.toFixed(2)
          + "\%</div>";
      var cell2 = "<td>" + stageScoreRect + "</td>";

      stageRows = stageRows + "<tr>" + cell1 + cell2 + "</tr>";
    }

    var jobTable = "<table border='1px' style='width:100%;'>" + stageRows + "</table>";
    content.push(jobTable);
  }
  return content;
}
