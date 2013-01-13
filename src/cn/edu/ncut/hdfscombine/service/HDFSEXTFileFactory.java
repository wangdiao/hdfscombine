package cn.edu.ncut.hdfscombine.service;

import cn.edu.ncut.common.ConfigSingleton;
import cn.edu.ncut.hdfscombine.redis.HdfsfilesRepository;

public class HDFSEXTFileFactory {
	private static final Long filesize = Long.parseLong(ConfigSingleton
			.getInstance().getProperty("sfilesize"));

	public static HDFSEXTFile createHDFSEXTFile(long len) {
		HDFSEXTFile file;
		if (len < filesize) {
			file = new SmallFile();
		} else {
			file = new BigFile();
		}
		return file;
	}

	public static HDFSEXTFile createHDFSEXTFile(long len,
			HdfsfilesRepository hdfsfilesRepository) {
		HDFSEXTFile file = createHDFSEXTFile(len);
		file.setHdfsfilesRepository(hdfsfilesRepository);
		return file;
	}

}
