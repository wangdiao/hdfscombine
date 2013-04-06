package cn.edu.ncut.hdfscombine.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.io.IOUtils;
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
	public void save(InputStream is, long filelen, String filename)
			throws IOException {
		// 保存到文件缓存中
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			IOUtils.copyBytes(is, out, filelen, false);
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw e;
		}
		CacheFile cacheFile = new CacheFile();
		cacheFile.setContent(out.toByteArray());
		cacheFile.setLength(filelen);
		cacheFile.setName(new File(filename).getName());
		long maplen = hdfsfilesRepository.addCacheFile(filename, cacheFile);

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
			hdfsfilesRepository.delCacheFile(filename);
		} else {
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
		int pos = hdfsfilesRepository.existPos(filename);
		if (pos==1) {
			cacheFile = hdfsfilesRepository.getCacheFile(filename);
		} else if(pos==2) {
			String dirid = hdfsfilesRepository.getDirid(new File(filename).getParent());
			MetaFile metaFile = hdfsfilesRepository.getMetaFile(dirid,
					new File(filename).getName());
			if (metaFile.getStorepos() == -1L)
				return false;
			cacheFile = new CacheFile();
			cacheFile.setLength(metaFile.getLength());
			cacheFile.setName(metaFile.getName());
			String uri = basepath + metaFile.getStorename();
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			HdfsHelper.fetchSequence(os, uri, metaFile.getStorepos(), dirid
					+ ":" + metaFile.getName());
			cacheFile.setContent(os.toByteArray());
		}else{
			return false;
		}
		InputStream is = new ByteArrayInputStream(cacheFile.getContent());
		SocketStream.writeInteger(cacheFile.getLength().intValue(), out);
		IOUtils.copyBytes(is, out, 4096);
		return true;
	}

}
