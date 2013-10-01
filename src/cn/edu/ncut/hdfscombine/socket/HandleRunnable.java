package cn.edu.ncut.hdfscombine.socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.Socket;

import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import cn.edu.ncut.common.FileOperateMark;
import cn.edu.ncut.common.SocketStream;
import cn.edu.ncut.hdfscombine.service.DirectHDFSFileService;
import cn.edu.ncut.hdfscombine.service.CommonFileService;

/**
 * 处理接收的请求，对文件的上传、删除操作 接收Socket进行处理 Socket输入流： int:上传标记 string:请求方法, string:文件名,
 * [int:数据流长度，数据流] Socket输出流： int:操作标记 [int:数据流长度，数据流]
 * 
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
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			int flag = SocketStream.readInteger(in);
			String methodName = SocketStream.readString(in);
			Method method;
			switch (flag) {
			case FileOperateMark.IN_COMMON:
				method = CommonFileService.class.getMethod(methodName,
						InputStream.class, OutputStream.class);
				method.invoke(null, in, out);
				break;
			case FileOperateMark.IN_DIRECT:
				method = DirectHDFSFileService.class.getDeclaredMethod(
						methodName, InputStream.class, OutputStream.class);
				method.invoke(null, in, out);
				break;
			}

		} catch (IOException | SecurityException | IllegalArgumentException
				| ReflectiveOperationException e) {
			e.printStackTrace();
		}
		IOUtils.closeSocket(socket);
	}

}
