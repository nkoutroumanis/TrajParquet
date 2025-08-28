package gr.ds.unipi.spatialnodb;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

public class AppConfig {
    private final Config config;

    private AppConfig(String pathOfConfigFile){
        config = ConfigFactory.parseFile(new File(pathOfConfigFile));
    }

    public static AppConfig newAppConfig(String pathOfConfigFile){
        return new AppConfig(pathOfConfigFile);
    }

    public Config getConfig() {
        return config;
    }


    public static Config loadConfig(String defaultResource) {
        boolean runningFromJar = false;
        try {
            String path = AppConfig.class.getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI()
                    .getPath();
            runningFromJar = path.endsWith(".jar");
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (runningFromJar) {
            // Must provide external config via -Dconfig.file
            String externalPath = System.getProperty("config.file");
            if (externalPath == null) {
                throw new RuntimeException(
                        "You must specify a config file with -Dconfig.file=/path/to/config.conf"
                );
            }
            File externalFile = new File(externalPath);
            if (!externalFile.exists()) {
                throw new RuntimeException(
                        "External config file not found: " + externalPath
                );
            }
            return ConfigFactory.parseFile(externalFile).resolve();
        } else {
            return ConfigFactory.parseResources(defaultResource).resolve();
        }
    }

}
