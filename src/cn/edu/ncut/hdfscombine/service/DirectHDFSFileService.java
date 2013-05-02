package cn.edu.ncut.hdfscombine.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import cn.edu.ncut.common.HdfsHelper;
import cn.edu.ncut.common.SocketStream;

public class DirectHDFSFileService {
	private final static Logger logger = Logger
			.getLogger(DirectHDFSFileService.class);

	public static void upload(InputStream is, OutputStream out) {
		try {
			String filename = SocketStream.readString(is);
			int len = SocketStream.readInteger(is);
			HdfsHelper.upload(is, len, filename);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void fetch(InputStream is, OutputStream out) {
		try {
			String filename = SocketStream.readString(is);
			HdfsHelper.fetch(out, filename);
		} catch (IOException e) {
			logger.error(e);
		}
	}

	public static void delete(InputStream is, OutputStream out) {
		try {
			String filename = SocketStream.readString(is);
			HdfsHelper.delete(filename, false);
		} catch (IOException e) {
			logger.error(e);
		}
	}
}
