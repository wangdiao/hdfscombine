package cn.edu.ncut.hdfscombine.redis;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
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
	private final StringRedisTemplate jdkRedisTemplate;
	private final RedisAtomicLong pathIdCounter;
	private final RedisAtomicLong cacheLengthCounter;
	private final JdkSerializationRedisSerializer jdkSerializationRedisSerializer = new JdkSerializationRedisSerializer();
	private final StringRedisSerializer stringRedisSerializer = new StringRedisSerializer();

	@Autowired
	public HdfsfilesRepository(StringRedisTemplate stringRedisTemplate,
			StringRedisTemplate jdkRedisTemplate) {
		this.stringRedisTemplate = stringRedisTemplate;
		this.jdkRedisTemplate = jdkRedisTemplate;
		pathIdCounter = new RedisAtomicLong(KeyUtils.GLOBALPATHID,
				stringRedisTemplate.getConnectionFactory());
		cacheLengthCounter = new RedisAtomicLong(KeyUtils.CACHEFILESLENGTH,
				stringRedisTemplate.getConnectionFactory());
		jdkRedisTemplate
				.setHashValueSerializer(jdkSerializationRedisSerializer);
		stringRedisTemplate.setStringSerializer(stringRedisSerializer);
	}

	/**
	 * 添加HDFS上保存的元数据文件
	 * 
	 * @param pathid
	 * @param file
	 */
	public void addMetaFile(String pathid, MetaFile file) {
			BoundHashOperations<String, String, MetaFile> filesOps = jdkRedisTemplate
					.boundHashOps(KeyUtils.METAFILESHASH);
			filesOps.put(pathid, file);
	}

	public MetaFile getMetaFile(String pathid) {
			BoundHashOperations<String, String, MetaFile> filesOps = jdkRedisTemplate
					.boundHashOps(KeyUtils.METAFILESHASH);
			return filesOps.get(pathid);
			}

	public void delMetaFile(String pathid) {
			BoundHashOperations<String, String, MetaFile> filesOps = jdkRedisTemplate
					.boundHashOps(KeyUtils.METAFILESHASH);
			filesOps.delete(pathid);
	}

	public void disableMetaFile(String pathid) {
			BoundHashOperations<String, String, MetaFile> filesOps = jdkRedisTemplate
					.boundHashOps(KeyUtils.METAFILESHASH);
			MetaFile metaFile = filesOps.get(pathid);
			metaFile.setStatus(false);
			filesOps.put(pathid, metaFile);
	}

	public long addCacheFile(String pathid, CacheFile cacheFile) {
			BoundHashOperations<String, String, CacheFile> filesOps = jdkRedisTemplate
					.boundHashOps(KeyUtils.CACHEFILESHASH);
			filesOps.put(pathid, cacheFile);
			return cacheLengthCounter.addAndGet(cacheFile.getLength());
	}

	public long addCacheFileByName(String filename, CacheFile cacheFile) {
		String pathid = this.addStorePath(filename);
		return this.addCacheFile(pathid, cacheFile);
	}

	public CacheFile getCacheFile(String pathid) {
			BoundHashOperations<String, String, CacheFile> filesOps = jdkRedisTemplate
					.boundHashOps(KeyUtils.CACHEFILESHASH);
			return filesOps.get(pathid);
	}

	public void delCacheFile(String pathid) {
			BoundHashOperations<String, String, CacheFile> filesOps = jdkRedisTemplate
					.boundHashOps(KeyUtils.CACHEFILESHASH);
			CacheFile cacheFile = filesOps.get(pathid);
			cacheLengthCounter.addAndGet(0L - cacheFile.getLength());
			filesOps.delete(pathid);
	}

	public void StartCombineCache() {
			cacheLengthCounter.set(0L);
			BoundHashOperations<String, String, CacheFile> filesOps = jdkRedisTemplate
					.boundHashOps(KeyUtils.CACHEFILESHASH);
			filesOps.rename(KeyUtils.CACHEFILESHASHBAK);
	}

	public void CombineCache(String path) {
		BoundHashOperations<String, String, CacheFile> filesOps = jdkRedisTemplate
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
				addMetaFile(skey, metaFile);
			}
			writer.hflush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}

	}

	public void EndCombineCache() {
		stringRedisTemplate.delete(KeyUtils.CACHEFILESHASHBAK);
	}

	/**
	 * 判断文件系统中是否存在文件
	 * 
	 * @param storepath
	 *            文件存储标记
	 * @return
	 */
	public boolean isExist(String storepath) {
		return !StringUtils.isEmpty(getPathId(storepath));
	}

	public String getPathId(String storepath) {
			BoundHashOperations<String, String, String> dirOps = stringRedisTemplate
					.boundHashOps(KeyUtils.STOREPATHHASH);
			String pathid = dirOps.get(storepath);
			return pathid;
	}

	public String addStorePath(String storepath) {
		String pathid = Long.valueOf(pathIdCounter.addAndGet(1L)).toString();
		BoundHashOperations<String, String, String> dirOps = stringRedisTemplate
				.boundHashOps(KeyUtils.STOREPATHHASH);
		dirOps.put(storepath, pathid);
		return pathid;
	}

	public int existPos(String pathid) {
		if (this.getCacheFile(pathid) != null) {
			return 1;
		} else {
			if (this.getMetaFile(pathid) != null)
				return 2;
		}
		return 0;
	}

}