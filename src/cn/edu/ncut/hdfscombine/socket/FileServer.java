package cn.edu.ncut.hdfscombine.socket;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import cn.edu.ncut.common.ConfigSingleton;

public class FileServer implements Runnable {
	
	private final static Logger logger = Logger.getLogger(FileServer.class);

	private final int port = Integer.parseInt(ConfigSingleton
			.getInstance().getProperty("port"));

	private ServerSocket server;

	public void run() {
		try {
			server = new ServerSocket(port);
			ExecutorService writeservice = Executors.newCachedThreadPool();
			// 开始循环
			while (true) {
				Socket socket = server.accept();
				writeservice.execute(new HandleRunnable(socket));
			}
		} catch (Exception e) {
			logger.error(e);
		}
	}


}
