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
  $.getJSON('/rest/flowgraphdata?url=' + queryString()['historyflowurl'], function(data) {
    updateExecTimezone(data);

    // Compute the jobDefUrl list such that the job numbers in the tooltip match the corresponding job in the table.
    var jobDefList = [];
    for (var i = data.length - 1 ; i >=0 ; i--) {
      for (var j = 0; j < data[i].jobscores.length; j++) {
        var jobDefUrl = data[i].jobscores[j]["jobdefurl"];
        if (jobDefList.indexOf(jobDefUrl) == -1) {
          jobDefList.push(jobDefUrl);
        }
      }
    }

    plotter(data, jobDefList);
  });

  loadTableTooltips();
});

/**
 * Example tooltip content:
 *
 * Execution 1
 * Flow score = 163672
 * Top poor jobs:
 * Job 25  45%
 * Job 16  20%
 * job 14  10%
 *
 */
function getGraphTooltipContent(record, jobDefList) {

  var content = ["<b>" + record.flowtime + "</b>"];
  content.push("Flow Score = " + record.score);
  if (record.score != 0) {
    var jobLimit = 3;
    content.push("Top poor jobs:");

    var scoreList = [];
    for (var i = 0; i < record.jobscores.length; i++) {
      var scoreWidth = record.jobscores[i]["jobscore"] * 100 / record.score;
      scoreList.push([scoreWidth, i]);
    }

    scoreList.sort(function(left, right) {
      return left[0] > right[0] ? -1 : 1;
    });

    // Traverse ordered list
    var jobRow = "";
    for (var jobIndex = 0;  jobIndex < scoreList.length; jobIndex++) {

      var width = scoreList[jobIndex][0];
      var index = scoreList[jobIndex][1];

      // Skip after jobLimit jobs are captured or when width is 0.
      if (jobIndex >= jobLimit || width == 0) {
        break;
      }

      // cell 1
      var jobDefUrl = record.jobscores[index]['jobdefurl'];
      var jobLink = "/jobhistory?historyjoburl=" + encodeURIComponent(jobDefUrl);
      var jobRef = "<a href='" + jobLink + "'>Job " + (jobDefList.indexOf(jobDefUrl) + 1) + "</a>";
      var cell1 = "<td width='65px' style='padding:3px;'>" + jobRef + "</td>";

      // cell 2
      var jobScoreRect = "<div style='padding:3px;background:red;width:" + width + "%'>" + +width.toFixed(2) + "\%</div>";
      var cell2 = "<td>" + jobScoreRect + "</td>";

      jobRow = jobRow + "<tr>" + cell1 + cell2 + "</tr>";
    }

    var jobTable = "<table border='1px' style='width:100%;'>" + jobRow + "</table>";
    content.push(jobTable);
  }
  return content;
}