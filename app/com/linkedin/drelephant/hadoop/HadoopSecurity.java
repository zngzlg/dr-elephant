package com.linkedin.drelephant.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import play.Play;

import java.io.IOException;
import java.security.PrivilegedAction;

public class HadoopSecurity {
    private static final Logger logger = Logger.getLogger(HadoopSecurity.class);

    private UserGroupInformation loginUser = null;

    private String keytabLocation;
    private String keytabUser;
    private boolean securityEnabled = false;

    public HadoopSecurity() {
        Configuration conf = new Configuration();
        UserGroupInformation.setConfiguration(conf);
        securityEnabled = UserGroupInformation.isSecurityEnabled();
        if (securityEnabled) {
            keytabLocation = Play.application().configuration().getString("keytab.location");
            keytabUser = Play.application().configuration().getString("keytab.user");
            checkLogin();
        }
    }

    public UserGroupInformation getUGI() {
        checkLogin();
        return loginUser;
    }

    public void checkLogin() {
        // try login
        try {
            if (loginUser == null) {
                logger.info("No login user. Creating login user");
                logger.info("Logging with " + keytabUser + " and " + keytabLocation);
                UserGroupInformation.loginUserFromKeytab(keytabUser, keytabLocation);
                loginUser = UserGroupInformation.getLoginUser();
                logger.info("Logged in with user " + loginUser);
            } else {
                loginUser.checkTGTAndReloginFromKeytab();
            }
        } catch (IOException e) {
            logger.error("Failed to login with kerberos ", e);
        }
    }

    public <T> T doAs(PrivilegedAction<T> action) {
        UserGroupInformation ugi = getUGI();
        if (ugi != null) {
            return ugi.doAs(action);
        }
        return null;
    }
}
