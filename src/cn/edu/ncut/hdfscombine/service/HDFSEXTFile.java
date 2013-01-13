package cn.edu.ncut.hdfscombine.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import cn.edu.ncut.hdfscombine.redis.HdfsfilesRepository;

public interface HDFSEXTFile {

	/**
	 * 保存文件
	 * 
	 * @param is
	 *            文件上传流
	 * @param filelen
	 *            文件长度
	 * @param filename
	 *            文件名
	 * @throws IOException
	 */
	public abstract void save(InputStream is, long filelen, String filename)
			throws IOException;

	/**
	 * 删除文件
	 * 
	 * @param hdfsfileObj
	 *            文件名
	 */
	public abstract boolean delete(String filename) throws IOException;

	public abstract boolean fetch(OutputStream out, String filename)
			throws IOException;
	
	public abstract void setHdfsfilesRepository(HdfsfilesRepository hdfsfilesRepository);

}