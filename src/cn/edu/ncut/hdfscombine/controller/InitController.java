package cn.edu.ncut.hdfscombine.controller;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import cn.edu.ncut.hdfscombine.redis.HdfsfilesRepository;
import cn.edu.ncut.hdfscombine.service.HandleFileService;
import cn.edu.ncut.hdfscombine.socket.FileServer;

@Controller
public class InitController {
	
	@Autowired
	private HdfsfilesRepository hdfsfilesRepository;
	
	@PostConstruct
	public void init(){
		HandleFileService.setHdfsfilesRepository(hdfsfilesRepository);
		Thread thread = new Thread(new FileServer());
		thread.start();
		System.out.println("Socket接收服务启动完成");
	}
}
