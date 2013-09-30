package cn.edu.ncut.hdfscombine.redis;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.support.atomic.RedisAtomicLong;
import org.springframework.stereotype.Service;

import cn.edu.ncut.common.HdfsStaticResource;
import cn.edu.ncut.hdfscombine.model.CacheFile;
import cn.edu.ncut.hdfscombine.model.MetaFile;
import cn.edu.ncut.hdfscombine.service.SmallFile;

@Service
public class HdfsfilesRepository {
	private final static Logger logger = Logger
			.getLogger(HdfsfilesRepository.class);
	private final StringRedisTemplate stringRedisTemplate;
	private final RedisAtomicLong dirIdCounter;
	private final RedisAtomicLong cacheLengthCounter;
	private final JdkSerializationRedisSerializer jdkSerializationRedisSerializer = new JdkSerializationRedisSerializer();
	private final StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();

	@Autowired
	public HdfsfilesRepository(StringRedisTemplate template) {
		this.stringRedisTemplate = template;
		dirIdCounter = new RedisAtomicLong(KeyUtils.GLOBALDIRID,
				template.getConnectionFactory());
		cacheLengthCounter = new RedisAtomicLong(KeyUtils.CACHEFILESLENGTH,
				template.getConnectionFactory());
		stringRedisTemplate
				.setHashValueSerializer(jdkSerializationRedisSerializer);
	}

	public void addMetaFile(String filename, MetaFile file) {
		String dirname = new File(filename).getParent();
		String dirid = getDirid(dirname);
		addMetaFileByDirid(dirid, file);
	}

	public void addMetaFileByDirid(String dirid, MetaFile file) {
		BoundHashOperations<String, String, MetaFile> filesOps = stringRedisTemplate
				.boundHashOps(KeyUtils.filesid(dirid));
		filesOps.put(file.getName(), file);
	}

	public MetaFile getMetaFile(String filename) {
		File file = new File(filename);
		String name = file.getName();
		String dirname = file.getParent();
		String dirid = getDirid(dirname);
		BoundHashOperations<String, String, MetaFile> filesOps = stringRedisTemplate
				.boundHashOps(KeyUtils.filesid(dirid));
		return filesOps.get(name);
	}

	public MetaFile getMetaFile(String dirid, String name) {
		BoundHashOperations<String, String, MetaFile> filesOps = stringRedisTemplate
				.boundHashOps(KeyUtils.filesid(dirid));
		return filesOps.get(name);
	}

	public void delMetaFile(String filename) {
		File file = new File(filename);
		String name = file.getName();
		String dirname = file.getParent();
		String dirid = getDirid(dirname);
		BoundHashOperations<String, String, MetaFile> filesOps = stringRedisTemplate
				.boundHashOps(KeyUtils.filesid(dirid));
		filesOps.delete(name);
	}

	public void disableMetaFile(String filename) {
		File file = new File(filename);
		String name = file.getName();
		String dirname = file.getParent();
		String dirid = getDirid(dirname);
		BoundHashOperations<String, String, MetaFile> filesOps = stringRedisTemplate
				.boundHashOps(KeyUtils.filesid(dirid));
		MetaFile metaFile = filesOps.get(name);
		metaFile.setStatus(false);
		filesOps.put(name, metaFile);
	}

	public long addCacheFile(String path, CacheFile cacheFile) {
		File file = new File(path);
		String name = file.getName();
		String dirname = file.getParent();
		String dirid = getDirid(dirname);
		BoundHashOperations<String, String, CacheFile> filesOps = stringRedisTemplate
				.boundHashOps(KeyUtils.CACHEFILESHASH);
		filesOps.put(dirid + ":" + name, cacheFile);
		return cacheLengthCounter.addAndGet(cacheFile.getLength());
	}

	public CacheFile getCacheFile(String path) {
		File file = new File(path);
		String name = file.getName();
		String dirname = file.getParent();
		String dirid = getDirid(dirname);
		BoundHashOperations<String, String, CacheFile> filesOps = stringRedisTemplate
				.boundHashOps(KeyUtils.CACHEFILESHASH);
		return filesOps.get(dirid + ":" + name);
	}

	public void delCacheFile(String path) {
		File file = new File(path);
		String name = file.getName();
		String dirname = file.getParent();
		String dirid = getDirid(dirname);
		BoundHashOperations<String, String, CacheFile> filesOps = stringRedisTemplate
				.boundHashOps(KeyUtils.CACHEFILESHASH);
		CacheFile cacheFile = filesOps.get(dirid + ":" + name);
		cacheLengthCounter.addAndGet(0L - cacheFile.getLength());
		filesOps.delete(dirid + ":" + name);
	}

	public void StartCombineCache() {
		cacheLengthCounter.set(0L);
		BoundHashOperations<String, String, CacheFile> filesOps = stringRedisTemplate
				.boundHashOps(KeyUtils.CACHEFILESHASH);
		filesOps.rename(KeyUtils.CACHEFILESHASHBAK);
	}

	public void CombineCache(String path) {
		BoundHashOperations<String, String, CacheFile> filesOps = stringRedisTemplate
				.boundHashOps(KeyUtils.CACHEFILESHASHBAK);
		SequenceFile.Writer writer = null;
		try {
			Option fileOption = SequenceFile.Writer.file(new Path(SmallFile
					.getBasepath() + path));
			Option compressionOption = SequenceFile.Writer
					.compression(CompressionType.BLOCK);
			Option keyclassOption = SequenceFile.Writer.keyClass(Text.class);
			Option valueclassOption = SequenceFile.Writer
					.valueClass(BytesWritable.class);
			writer = SequenceFile.createWriter(
					HdfsStaticResource.getConfiguration(), fileOption,
					compressionOption, keyclassOption, valueclassOption);
			
			Text key = new Text();
			BytesWritable value = null;
			for (String skey : filesOps.keys()) {
				CacheFile cacheFile = filesOps.get(skey);
				key.set(skey);
				value = new BytesWritable(cacheFile.getContent());

				MetaFile metaFile = new MetaFile();
				metaFile.setName(cacheFile.getName());
				metaFile.setLength(cacheFile.getLength());
				metaFile.setStorename(path);
				metaFile.setStorepos(writer.getLength());
				writer.append(key, value);

				String[] akey = skey.split(":");
				addMetaFileByDirid(akey[0], metaFile);
			}
			writer.hflush();
		} catch (IOException e) {
			logger.error(e);
		} finally {
			IOUtils.closeStream(writer);
		}

	}

	public void EndCombineCache() {
		stringRedisTemplate.delete(KeyUtils.CACHEFILESHASHBAK);
	}

	public void addSubDir(String dirname) {
		String dirid = getDirid(dirname);
		stringRedisTemplate.setHashValueSerializer(stringRedisSerializer);
		BoundHashOperations<String, String, String> dirOps = stringRedisTemplate
				.boundHashOps(KeyUtils.subdirid(dirid));
		dirOps.put(KeyUtils.subdirid(dirid),
				String.valueOf(dirIdCounter.incrementAndGet()));
		stringRedisTemplate
				.setHashValueSerializer(jdkSerializationRedisSerializer);
	}

	/**
	 * 判断文件系统中是否存在文件
	 * 
	 * @param path
	 *            文件路径
	 * @return
	 */
	public boolean isExist(String path) {
		return existPos(path) != 0;
	}

	/**
	 * 判断文件系统中文件的位置
	 * 
	 * @param path
	 *            文件路径
	 * @return 0-不存在，1-cache中，2-hdfs上
	 */
	public int existPos(String path) {
		File file = new File(path);
		String name = file.getName();
		String dirname = file.getParent();
		String dirid = getDirid(dirname);
		BoundHashOperations<String, String, CacheFile> cachefilesOps = stringRedisTemplate
				.boundHashOps(KeyUtils.CACHEFILESHASH);
		if (cachefilesOps.hasKey(dirid + ":" + name)) {
			return 1;
		} else {
			BoundHashOperations<String, String, MetaFile> filesOps = stringRedisTemplate
					.boundHashOps(KeyUtils.filesid(dirid));
			if (filesOps.hasKey(name)) {
				return 2;
			}
		}
		return 0;
	}

	public String getDirid(String dirname) {
		String dirid = "0";
		if (dirname == null || "".equals(dirname))
			return dirid;
		String[] dirarray = dirname.split("/");
		List<String> dirlist = new ArrayList<String>();
		for (String item : dirarray) {
			if (!"".equals(item)) {
				dirlist.add(item);
			}
		}

		stringRedisTemplate.setHashValueSerializer(stringRedisSerializer);
		BoundHashOperations<String, String, String> dirOps = stringRedisTemplate
				.boundHashOps(KeyUtils.subdirid(dirid));
		for (String item : dirlist) {
			dirid = dirOps.get(item);
		}
		stringRedisTemplate
				.setHashValueSerializer(jdkSerializationRedisSerializer);
		return dirid;
	}

}