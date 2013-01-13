package cn.edu.ncut.hdfscombine.redis;

public class KeyUtils {
	public static final String CACHEFILESHASH = "cachefileshash";
	public static final String CACHEFILESHASHBAK = "cachefileshashbak";
	public static final String CACHEFILESLENGTH = "cachefileslength";
	public static final String FILESHASHNAME = "files";
	public static final String SUBDIRHASHNAME = "subdir";
	public static final String GLOBALDIRID = "global:dirid";
	public static String subdirid(String dirname){
		return FILESHASHNAME+dirname;
	}
	
	public static String filesid(String dirname){
		return SUBDIRHASHNAME+dirname;
	}
}
