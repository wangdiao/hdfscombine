package cn.edu.ncut.hdfscombine.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import cn.edu.ncut.common.ConfigSingleton;
import cn.edu.ncut.common.HdfsHelper;
import cn.edu.ncut.common.SocketStream;
import cn.edu.ncut.hdfscombine.model.CacheFile;
import cn.edu.ncut.hdfscombine.model.MetaFile;
import cn.edu.ncut.hdfscombine.redis.HdfsfilesRepository;

public class SmallFile implements HDFSEXTFile {

	private final static Logger logger = Logger.getLogger(SmallFile.class);

	public void setHdfsfilesRepository(HdfsfilesRepository hdfsfilesRepository) {
		this.hdfsfilesRepository = hdfsfilesRepository;
	}

	private final static String basepath = ConfigSingleton.getInstance()
			.getProperty("smallbasepath");
	private long filesize = Long.parseLong(ConfigSingleton.getInstance()
			.getProperty("mfilesize"));

	public static String getBasepath() {
		return basepath;
	}

	@Autowired
	private HdfsfilesRepository hdfsfilesRepository;

	@Override
	public void save(InputStream is, int filelen, String storepath)
			throws IOException {
		// 保存到文件缓存中
		CacheFile cacheFile = new CacheFile();
		cacheFile.setContent(IOUtils.toByteArray(is, filelen));
		cacheFile.setLength(new Long(filelen));
		cacheFile.setName(new File(storepath).getName());
		long maplen = hdfsfilesRepository.addCacheFileByName(storepath, cacheFile);

		// 打包
		if (maplen + filelen > filesize) {
			Runnable runnable = new Runnable() {
				@Override
				public void run() {
					synchronized (SmallFile.class) {
						logger.debug("Start Combine!");
						String storename = new Long(System.currentTimeMillis())
								.toString();
						hdfsfilesRepository.StartCombineCache();
						hdfsfilesRepository.CombineCache(storename);
						hdfsfilesRepository.EndCombineCache();
						logger.debug("-------------------------------------End Combine!");
					}
				}
			};
			new Thread(runnable).start();
		}
	}

	@Override
	public boolean delete(String filename) throws IOException {
		if (hdfsfilesRepository.isExist(filename)) {
			String pathid = hdfsfilesRepository.getPathId(filename);
			hdfsfilesRepository.delPath(filename);
			hdfsfilesRepository.delCacheFile(pathid);
		} else {
			hdfsfilesRepository.delPath(filename);
			MetaFile metaFile = hdfsfilesRepository.getMetaFile(filename);
			if (metaFile.getStorepos() != -1L)
				return false;
			hdfsfilesRepository.disableMetaFile(filename);
		}
		return true;
	}

	@Override
	public boolean fetch(OutputStream out, String filename) throws IOException {
		CacheFile cacheFile;
		String pathid = hdfsfilesRepository.getPathId(filename);
		int pos = hdfsfilesRepository.existPos(pathid);
		if (pos == 1) {
			cacheFile = hdfsfilesRepository.getCacheFile(pathid);
		} else if (pos == 2) {
			MetaFile metaFile = hdfsfilesRepository.getMetaFile(pathid);
			if (metaFile.getStorepos() == -1L)
				return false;
			cacheFile = new CacheFile();
			cacheFile.setLength(metaFile.getLength());
			cacheFile.setName(metaFile.getName());
			String uri = basepath + metaFile.getStorename();
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			HdfsHelper.fetchSequence(os, uri, metaFile.getStorepos(), pathid);
			cacheFile.setContent(os.toByteArray());
		} else {
			return false;
		}
		InputStream is = new ByteArrayInputStream(cacheFile.getContent());
		SocketStream.writeInteger(cacheFile.getLength().intValue(), out);
		IOUtils.copy(is, out);
		return true;
	}

}
