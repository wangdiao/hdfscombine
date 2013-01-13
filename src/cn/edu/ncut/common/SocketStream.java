package cn.edu.ncut.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SocketStream {
	// 读取一个数字
	public static int readInteger(InputStream is) throws IOException {
		byte[] bytes = new byte[4];
		is.read(bytes);
		return ByteHelper.b2i(bytes);
	}
	
	// 写入一个数字
	public static void writeInteger(int num, OutputStream out) throws IOException {
		out.write(ByteHelper.i2b(num));
	}

	// 读取一个字符串
	public static String readString(InputStream is) throws IOException {
		int len = readInteger(is);
		byte[] bytes = new byte[len];
		is.read(bytes);
		return new String(bytes);
	}

	// 写入字符串
	public static void writeString(String str, OutputStream os)
			throws IOException {
		byte[] fn_bytes = str.getBytes();

		os.write(ByteHelper.i2b(fn_bytes.length)); // 输出文件名长度
		os.write(fn_bytes); // 输出文件名
	}

}
