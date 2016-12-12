/*
 * Copyright 2016 LinkedIn Corp.
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

package com.linkedin.drelephant.security;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import play.Play;


/**
 * The class handles authentication when cluster is security enabled
 */
public class HadoopSecurity {
  private static final Logger logger = Logger.getLogger(HadoopSecurity.class);

  private UserGroupInformation _loginUser = null;

  private String _keytabLocation;
  private String _keytabUser;
  private boolean _securityEnabled = false;

  public HadoopSecurity() {
    UserGroupInformation.setConfiguration(new Configuration());
    _securityEnabled = UserGroupInformation.isSecurityEnabled();
  }


  private List<String> fetchListFromURL(String fileUrl) {
    List<String> whitelist = new ArrayList<String>();

    try {
      URLConnection urlConnection = new URL(fileUrl).openConnection();
      BufferedReader bufferedReader = new BufferedReader(
          new InputStreamReader(urlConnection.getInputStream()));
      String inputline;
      while ((inputline = bufferedReader.readLine()) != null) {
        whitelist.add(inputline.trim().toLowerCase());
      }
      bufferedReader.close();
      logger.info("Whitelist from url: " + whitelist.toString());

      return whitelist;
    } catch (Exception e) {
      logger.error("Unable to get whiltelist file from " + fileUrl);
      e.printStackTrace();
    }
    _isEnabled = false; // If error occurs in fetching list, disabling access validation
    // Useful in case URL hosting service (gitli) is down
    return Collections.emptyList();
  }

  public void login() throws IOException {
    if (!_securityEnabled) {
      logger.info("This cluster is not security enabled.");
      return;
    }

    logger.info("This cluster is security enabled.");
    boolean login = true;

    _keytabUser = Play.application().configuration().getString("keytab.user");
    if (_keytabUser == null) {
      logger.error("Keytab user not set. Please set keytab_user in the configuration file");
      login = false;
    }
    _keytabLocation = Play.application().configuration().getString("keytab.location");
    if (_keytabLocation == null) {
      logger.error("Keytab location not set. Please set keytab_location in the configuration file");
      login = false;
    } else if (!new File(_keytabLocation).exists()) {
      logger.error("The keytab file at location [" + _keytabLocation + "] does not exist.");
      login = false;
    }

    if (login) {
      checkLogin();
    } else {
      throw new IOException("Cannot login. This cluster is security enabled.");
    }
  }

  public UserGroupInformation getUGI() throws IOException {
    checkLogin();
    return _loginUser;
  }

  public void checkLogin() throws IOException {
    if (_loginUser == null) {
      logger.info("No login user. Creating login user");
      logger.info("Logging with " + _keytabUser + " and " + _keytabLocation);
      UserGroupInformation.loginUserFromKeytab(_keytabUser, _keytabLocation);
      _loginUser = UserGroupInformation.getLoginUser();
      logger.info("Logged in with user " + _loginUser);
      if(UserGroupInformation.isLoginKeytabBased()) {
        logger.info("Login is keytab based");
      } else {
        logger.info("Login is not keytab based");
      }
    } else {
      _loginUser.checkTGTAndReloginFromKeytab();
    }

  }

  public <T> T doAs(PrivilegedAction<T> action) throws IOException {
    UserGroupInformation ugi = getUGI();
    if (ugi != null) {
      return ugi.doAs(action);
    }
    return null;
  }
}
