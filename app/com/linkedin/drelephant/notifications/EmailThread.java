package com.linkedin.drelephant.notifications;

import com.linkedin.drelephant.analysis.Severity;
import model.JobResult;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import play.Play;
import views.html.emailcritical;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;


public class EmailThread extends Thread {

  private LinkedBlockingQueue<JobResult> _resultQueue;
  private AtomicBoolean _running = new AtomicBoolean(true);

  private String _smtpHost;
  private int _smtpPort;
  private String _smtpFrom;
  private DefaultAuthenticator _authenticator = null;

  public EmailThread() {
    setName("Email Thread");
    _resultQueue = new LinkedBlockingQueue<JobResult>();
    _smtpHost = Play.application().configuration().getString("smtp.host");
    _smtpPort = Play.application().configuration().getInt("smtp.port");
    String smtpUser = Play.application().configuration().getString("smtp.user");
    String smtpPassword = Play.application().configuration().getString("smtp.password");
    if (smtpUser != null && !smtpUser.isEmpty()) {
      _authenticator = new DefaultAuthenticator(smtpUser, smtpPassword);
    }
    _smtpFrom = Play.application().configuration().getString("smtp.from");
  }

  @Override
  public void run() {
    while (_running.get()) {
      JobResult result = null;
      while (result == null && _running.get()) {
        try {
          result = _resultQueue.take();
        } catch (InterruptedException e) {
          //Ignored
        }
      }
      if (!_running.get()) {
        return;
      }
      Severity worstSeverity = result.severity;
      if (worstSeverity == Severity.CRITICAL) {
        //Send email right away
        sendCriticalEmail(result);
      } else if (worstSeverity == Severity.SEVERE) {
        //Keep track of a digest and send in intervals
      }
    }
  }

  public void kill() {
    _running.set(false);
    this.interrupt();
  }

  public void enqueue(JobResult result) {
    try {
      _resultQueue.put(result);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void sendCriticalEmail(JobResult result) {
    try {
      //Generate content
      String html = emailcritical.render(result).body();

      //Send email
      HtmlEmail email = new HtmlEmail();
      email.setHostName(_smtpHost);
      email.setSmtpPort(_smtpPort);
      if (_authenticator != null) {
        email.setAuthenticator(_authenticator);
      }
      email.setSSLOnConnect(true);
      email.setFrom(_smtpFrom);
      email.addTo(result.username + "@linkedin.com");
      email.setSubject("Dr. Elephant - Hadoop Job Status Notification");
      email.setHtmlMsg(html);
      email.setDebug(true);
      ///////////////////
      //
      // WARNING: This next line will send out the emails.
      // Do NOT uncomment before proper testing and mental meditation.
      //
      ///////////////////
      //email.send();
    } catch (EmailException e) {
      e.printStackTrace();
    }
  }
}
