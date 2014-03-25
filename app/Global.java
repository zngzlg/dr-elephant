import com.linkedin.drelephant.DrElephant;
import com.linkedin.drelephant.ElephantAnalyser;
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
        ElephantAnalyser.instance();
        try {
            drElephant = new DrElephant();
            drElephant.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void onStop(Application app) {
        Logger.info("Application shutdown...");
        if (drElephant != null) {
            drElephant.kill();
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