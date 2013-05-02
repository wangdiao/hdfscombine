package cn.edu.ncut.hdfscombine.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.log4j.Logger;

import cn.edu.ncut.common.FileOperateMark;
import cn.edu.ncut.common.SocketStream;
import cn.edu.ncut.hdfscombine.redis.HdfsfilesRepository;

/**
 * 文件操作类，以direct开头的方法为直接操作hdfs
 * 
 * @author wang
 * 
 */
public class CommonFileService {

	private final static Logger logger = Logger
			.getLogger(CommonFileService.class);

	private static HdfsfilesRepository hdfsfilesRepository;

	public static void setHdfsfilesRepository(
			HdfsfilesRepository _hdfsfilesRepository) {
		hdfsfilesRepository = _hdfsfilesRepository;
	}

	/**
	 * 
	 * @param is
	 * @throws IOException
	 */
	public static void upload(InputStream is, OutputStream out) {
		try {
			String filename = SocketStream.readString(is);
			int len = SocketStream.readInteger(is);
			if (hdfsfilesRepository.isExist(filename)) {
				SocketStream.writeInteger(FileOperateMark.EXISTED, out);
				return;
			}
			HDFSEXTFile file = HDFSEXTFileFactory.createHDFSEXTFile(len,
					hdfsfilesRepository);
			file.save(is, len, filename);
			SocketStream.writeInteger(FileOperateMark.UPLOADSUCCESS, out);
		} catch (IOException e) {
			try {
				SocketStream.writeInteger(FileOperateMark.UPLOADFAILD, out);
			} catch (IOException e1) {
				logger.error(e);
			}
		}
	}

	/**
	 * 从文件系统上获取文件，输出流为：操作标识、文件大小、文件内容
	 * 
	 * @param is
	 * @param out
	 */
	public static void fetch(InputStream is, OutputStream out) {
		try {
			String filename = SocketStream.readString(is);
			if (!hdfsfilesRepository.isExist(filename)) {
				SocketStream.writeInteger(FileOperateMark.NOTFOUND, out);
				return;
			}
			SmallFile smallFile = new SmallFile();
			smallFile.setHdfsfilesRepository(hdfsfilesRepository);
			SocketStream.writeInteger(FileOperateMark.FETCHEDSUCCESS, out);
			if (!smallFile.fetch(out, filename)) {
				BigFile bigFile = new BigFile();
				bigFile.setHdfsfilesRepository(hdfsfilesRepository);
				bigFile.fetch(out, filename);
			}
		} catch (IOException e) {
			try {
				SocketStream.writeInteger(FileOperateMark.FETCHEDFAILD, out);
			} catch (IOException e1) {
				logger.error(e);
			}
		}
	}

	public static void delete(InputStream is, OutputStream out) {
		try {
			String filename = SocketStream.readString(is);
			if (!hdfsfilesRepository.isExist(filename)) {
				SocketStream.writeInteger(FileOperateMark.NOTFOUND, out);
				return;
			}
			SmallFile smallFile = new SmallFile();
			smallFile.setHdfsfilesRepository(hdfsfilesRepository);
			if (!smallFile.delete(filename)) {
				BigFile bigFile = new BigFile();
				bigFile.setHdfsfilesRepository(hdfsfilesRepository);
				bigFile.delete(filename);
			}
		} catch (IOException e) {
			try {
				SocketStream.writeInteger(FileOperateMark.DELETEDFAILD, out);
			} catch (IOException e1) {
				logger.error(e);
			}
		}
	}
}
