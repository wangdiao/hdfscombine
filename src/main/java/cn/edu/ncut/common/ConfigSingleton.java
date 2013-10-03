package cn.edu.ncut.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.Logger;

public class ConfigSingleton {
	private final static Logger logger = Logger
			.getLogger(ConfigSingleton.class);

	private Properties properties;

	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
	public String getProperty(String key){
		return properties.getProperty(key);
	}

	protected ConfigSingleton() {
		properties = new Properties();
		InputStream is = null;
		try {
			is = ConfigSingleton.class
					.getResourceAsStream("/config.properties");
			properties.load(is);
		} catch (Exception e) {
			logger.debug(e.toString());
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				logger.debug(e.toString());
			}
		}
	}

	public static ConfigSingleton getInstance() {
		return ((ConfigSingleton) SingletonRegistry.getInstance(ConfigSingleton.class.getName()));
	}
}
