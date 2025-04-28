import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class EnvLoader {
    public static void loadEnv(String path) {
        try {
            Properties props = new Properties();
            props.load(new FileInputStream(path));
            for (String name : props.stringPropertyNames()) {
                // System environment variables cannot be set at runtime easily,
                // but you can set System properties
                System.setProperty(name, props.getProperty(name));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
