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
package com.linkedin.drelephant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import play.Play;


public class HadoopSecurity {
  private static final Logger logger = Logger.getLogger(HadoopSecurity.class);

  private UserGroupInformation _loginUser = null;

  private String _keytabLocation;
  private String _keytabUser;
  private boolean _securityEnabled = false;

  public HadoopSecurity() throws IOException {
    Configuration conf = new Configuration();
    UserGroupInformation.setConfiguration(conf);
    _securityEnabled = UserGroupInformation.isSecurityEnabled();
    if (_securityEnabled) {
      _keytabLocation = Play.application().configuration().getString("keytab.location");
      if (!new File(_keytabLocation).exists()) {
          logger.error("The keytab file at location [" + _keytabLocation + "] does not exist.");
      }
      _keytabUser = Play.application().configuration().getString("keytab.user");
      checkLogin();
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
