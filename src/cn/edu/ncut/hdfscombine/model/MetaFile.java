package cn.edu.ncut.hdfscombine.model;

import java.io.Serializable;

public class MetaFile implements Serializable {
	private static final long serialVersionUID = 7531910647699440274L;
	
	private String name;
	private Long length;
	private Long storepos;
	private String storename;
	private Boolean status;
	
	public MetaFile(){
		status = true;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Long getLength() {
		return length;
	}
	public void setLength(Long length) {
		this.length = length;
	}
	public Long getStorepos() {
		return storepos;
	}
	public void setStorepos(Long storepos) {
		this.storepos = storepos;
	}
	public String getStorename() {
		return storename;
	}
	public void setStorename(String storename) {
		this.storename = storename;
	}
	public Boolean getStatus() {
		return status;
	}
	public void setStatus(Boolean status) {
		this.status = status;
	}

	
}
