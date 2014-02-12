package cn.edu.ncut.hdfscombine.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;

import cn.edu.ncut.common.ConfigSingleton;
import cn.edu.ncut.common.HdfsHelper;
import cn.edu.ncut.common.SocketStream;
import cn.edu.ncut.hdfscombine.model.MetaFile;
import cn.edu.ncut.hdfscombine.redis.HdfsfilesRepository;

public class BigFile implements HDFSEXTFile {

	private final static Logger logger = Logger.getLogger(BigFile.class);

	private static String basepath = ConfigSingleton.getInstance().getProperty(
			"bigbasepath");
	
	public static String getBasepath(){
		return basepath;
	}

	private HdfsfilesRepository hdfsfilesRepository;
	
	public void setHdfsfilesRepository(HdfsfilesRepository hdfsfilesRepository){
		this.hdfsfilesRepository = hdfsfilesRepository;
	}

	@Override
	public void save(InputStream is, int filelen, String filename)
			throws IOException {
		String md5filename = DigestUtils.md5Hex(filename);
		String hdfsname = basepath+md5filename;
		
		//存储到HDFS上
		try {
			HdfsHelper.upload(is, filelen, hdfsname);
		} catch (IOException e) {
			logger.error(e);
			throw e;
		}
		
		//添加元数据项
		//TODO: 大文件计划不保存元数据
		MetaFile metaFile = new MetaFile();
		metaFile.setLength(new Long(filelen));
		metaFile.setName(new File(filename).getName());
		metaFile.setStorename(md5filename);
		metaFile.setStorepos(-1L);
		hdfsfilesRepository.addMetaFile(filename, metaFile);
	}

	@Override
	public boolean delete(String filename) throws IOException {
		MetaFile metaFile = hdfsfilesRepository.getMetaFile(filename);
		HdfsHelper.delete(metaFile.getStorename(), false);
		//删除元数据
		hdfsfilesRepository.delMetaFile(filename);
		return true;
	}

	@Override
	public boolean fetch(OutputStream out, String filename) throws IOException {
		MetaFile metaFile = hdfsfilesRepository.getMetaFile(filename);
		SocketStream.writeInteger(metaFile.getLength().intValue(), out);
		HdfsHelper.fetch(out, metaFile.getStorename());
		return true;
	}

}
