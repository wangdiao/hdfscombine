package cn.edu.ncut.hdfscombine.model;

import java.io.Serializable;

public class CacheFile implements Serializable {
	private static final long serialVersionUID = 7934570271913550086L;

	private String name;
	private Long length;
	private byte[] content;

	public Long getLength() {
		return length;
	}

	public void setLength(Long length) {
		this.length = length;
	}

	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
