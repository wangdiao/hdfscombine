package cn.edu.ncut.common;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

public class HdfsHelper {

	private static Configuration conf = HdfsStaticResource.getConfiguration();
	private static FileSystem fs = HdfsStaticResource.getFileSystem();
	
	public static void upload(InputStream is, long filelen, String path) throws IOException{
		OutputStream os = fs.create(new Path(path));
		IOUtils.copyBytes(is, os, filelen, false);
		IOUtils.closeStream(os);
	}
	
	public static void delete(String path, boolean recursion) throws IOException{
		fs.delete(new Path(path), recursion);
	}
	
	public static void fetch(OutputStream os, String path) throws IOException{
		InputStream is = fs.open(new Path(path));
		IOUtils.copyBytes(is, os,4096);
		IOUtils.closeStream(is);
	}
	
	public static void fetchSequence(OutputStream os, String path, long pos, String name) throws IOException{
		SequenceFile.Reader reader = null;
		try {
//			reader = new SequenceFile.Reader(fs, new Path(path), conf);
			Reader.Option fileOption = Reader.file(new Path(path));
			reader = new SequenceFile.Reader(conf, fileOption);
			reader.seek(pos);
			Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(),
					conf);
			BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			while(reader.next(key)){
				if(key.toString().equals(name)) {
					reader.getCurrentValue(value);
					break;
				}
			}
			if(value.getLength() == 0){
				return;
			}
			InputStream is = new ByteArrayInputStream(value.getBytes());
			IOUtils.copyBytes(is, os,4096);
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
