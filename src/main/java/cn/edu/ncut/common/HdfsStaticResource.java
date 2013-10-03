package cn.edu.ncut.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HdfsStaticResource {
	
	private static Configuration conf;
	private static FileSystem fs;
	
	static{
		ConfigSingleton configSingleton = ConfigSingleton.getInstance();
		conf = new Configuration();
		conf.set("fs.default.name", configSingleton.getProperty("fs.default.name"));
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Configuration getConfiguration() {
		return conf;
	}
	
	public static FileSystem getFileSystem() {
		return fs;
	}

}
