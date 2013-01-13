package cn.edu.ncut.hdfscombine.socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;

import org.apache.log4j.Logger;

import cn.edu.ncut.common.SocketStream;
import cn.edu.ncut.hdfscombine.service.HandleFileService;

/**
 * 接收Socket进行处理
 * Socket输入流：
 * string:请求方法,string:文件名,[int:数据流长度，数据流]
 * Socket输出流：
 * int:操作标记 [int:数据流长度，数据流]
 * @author wang
 *
 */
public class SocketService {

	private final static Logger logger = Logger.getLogger(SocketService.class);

	private String methodName;
	private InputStream inputStream;
	private OutputStream outputStream;

	public SocketService(InputStream in, OutputStream out) {
		this.inputStream = in;
		this.outputStream = out;
		try {
			methodName = SocketStream.readString(in);
		} catch (IOException e) {
			logger.error(e);
		}
	}

	public void invoke() {
		try {
			Method method = HandleFileService.class.getMethod(methodName,
					InputStream.class, OutputStream.class);
			method.invoke(null, inputStream, outputStream);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getMethodName() {
		return methodName;
	}

	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}

}
