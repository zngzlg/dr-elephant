package com.linkedin.drelephant.analysis;

import com.linkedin.drelephant.math.Statistics;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import model.JobResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * This class provides a list of analysis promises to be generated under Hadoop YARN environment
 *
 */
public class YarnAnalysisProvider implements AnalysisProvider {
  private static final Logger logger = Logger.getLogger(YarnAnalysisProvider.class);
  // We provide one minute job fetch delay due to the job sending lag from AM/NM to JobHistoryServer HDFS
  private static final long FETCH_DELAY = 60000;
  private static final long TOKEN_UPDATE_INTERVAL =
      Statistics.MINUTE_IN_MS * 30 + new Random().nextLong() % (3 * Statistics.MINUTE_IN_MS);

  private String _resourceManagerAddress;
  private long _lastTime = 0;
  private long _currentTime = 0;
  private long _tokenUpdatedTime = 0;
  private AuthenticatedURL.Token _token;
  private AuthenticatedURL _authenticatedURL;
  private final ObjectMapper _objectMapper = new ObjectMapper();

  private final Queue<AnalysisPromise> _retryQueue = new ConcurrentLinkedQueue<AnalysisPromise>();

  @Override
  public void configure(Configuration configuration)
      throws Exception {
    _resourceManagerAddress = configuration.get("yarn.resourcemanager.webapp.address");
  }

  @Override
  public List<AnalysisPromise> fetchPromises()
      throws IOException, AuthenticationException {
    List<AnalysisPromise> appList = new ArrayList<AnalysisPromise>();

    // There is a lag of job data from AM/NM to JobHistoryServer HDFS, we shouldn't use the current
    // time, since there might be new jobs arriving after we fetch jobs.
    // We provide one minute delay to address this lag.
    _currentTime = System.currentTimeMillis() - FETCH_DELAY;
    updateAuthToken();

    URL appsURL = new URL(new URL("http://" + _resourceManagerAddress), String
        .format("/ws/v1/cluster/apps?finalStatus=SUCCEEDED&finishedTimeBegin=%s&finishedTimeEnd=%s",
            String.valueOf(_lastTime), String.valueOf(_currentTime)));

    logger.info("Fetching recent finished application runs via URL: " + appsURL);

    JsonNode rootNode = readJsonNode(appsURL);
    JsonNode apps = rootNode.path("apps").path("app");

    for (JsonNode app : apps) {
      String id = app.get("id").getValueAsText();
      String jobId = id.replaceFirst("application", "job");

      if (JobResult.find.byId(jobId) == null) {
        String user = app.get("user").getValueAsText();
        String name = app.get("name").getValueAsText();
        String trackingUrl = app.get("trackingUrl").getValueAsText();
        long startTime = app.get("startedTime").getLongValue();
        long finishTime = app.get("finishedTime").getLongValue();

        ApplicationType type = ApplicationType.getType(app.get("applicationType").getValueAsText());

        // If the application type is supported
        if (type != null) {
          AnalysisPromise promise = new AnalysisPromise();
          promise.setAppId(id);
          promise.setAppType(type);
          promise.setJobId(jobId);
          promise.setUser(user);
          promise.setName(name);
          promise.setTrackingUrl(trackingUrl);
          promise.setStartTime(startTime);
          promise.setFinishTime(finishTime);

          appList.add(promise);
        }
      }
    }

    // Append promises from the retry queue at the end of the list
    while (!_retryQueue.isEmpty()) {
      appList.add(_retryQueue.poll());
    }

    _lastTime = _currentTime;
    return appList;
  }

  @Override
  public void addIntoRetries(AnalysisPromise promise) {
    _retryQueue.add(promise);
  }

  private void updateAuthToken() {
    if (_currentTime - _tokenUpdatedTime > TOKEN_UPDATE_INTERVAL) {
      logger.info("FutureProvider updating its Authenticate Token....");
      _token = new AuthenticatedURL.Token();
      _authenticatedURL = new AuthenticatedURL();
      _tokenUpdatedTime = _currentTime;
    }
  }

  private JsonNode readJsonNode(URL url)
      throws IOException, AuthenticationException {
    HttpURLConnection conn = _authenticatedURL.openConnection(url, _token);
    return _objectMapper.readTree(conn.getInputStream());
  }
}
