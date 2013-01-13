package cn.edu.ncut.hdfscombine.socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * 处理接收的请求，对文件的上传、删除操作
 * @author wang
 *
 */
public class HandleRunnable implements Runnable {

	private final static Logger logger = Logger.getLogger(HandleRunnable.class);
	private Socket socket;

	public HandleRunnable(Socket socket) {
		this.socket = socket;
	}

	@Override
	public void run() {
			try {
				InputStream is = socket.getInputStream();
				OutputStream out = socket.getOutputStream();
				SocketService service = new SocketService(is, out);
				service.invoke();
			} catch (IOException e) {
				logger.error(e);
			}
			IOUtils.closeSocket(socket);
	}

}
