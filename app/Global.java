import com.linkedin.drelephant.DrElephant;
import com.sun.security.sasl.util.AbstractSaslImpl;

import play.Application;
import play.GlobalSettings;
import play.Logger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.logging.Level;


public class Global extends GlobalSettings {

  DrElephant drElephant;

  public void onStart(Application app) {
    Logger.info("Application has started");

    fixJavaKerberos();

    try {
      drElephant = new DrElephant();
      drElephant.start();
    } catch (IOException e) {
      Logger.error("Application start failed...", e);
    }
  }

  public void onStop(Application app) {
    Logger.info("Application shutdown...");
    if (drElephant != null) {
      drElephant.kill();
    }
  }

  /**
   * This hack is done in order to fix a problem in Java 1.6 when using Kerberos
   * <p/>
   * Specific error:
   * java.lang.NullPointerException
   * at com.sun.security.sasl.util.AbstractSaslImpl.traceOutput(AbstractSaslImpl.java:241)
   * at com.sun.security.sasl.gsskerb.GssKrb5Client.evaluateChallenge(GssKrb5Client.java:180)
   */
  private static void fixJavaKerberos() {
    try {
      Field loggerField = AbstractSaslImpl.class.getDeclaredField("logger");
      loggerField.setAccessible(true);

      java.util.logging.Logger logger = (java.util.logging.Logger) loggerField.get(null);
      if (logger == null) {
        logger = java.util.logging.Logger.getLogger("javax.security.sasl");
        setFinalStatic(loggerField, logger);
      }
      //Prevent the code in GssKrb5Client.evaluateChallenge to call traceOutput()
      logger.setLevel(Level.OFF);
    } catch (Exception e) {
      Logger.error("Error trying to fix Kerberos connection", e);
    }

  }

  static void setFinalStatic(Field field, Object newValue) throws Exception {
    field.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    field.set(null, newValue);
  }
}
