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
}
