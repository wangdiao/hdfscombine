package cn.edu.ncut.hdfscombine.config;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;

import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import cn.edu.ncut.hdfscombine.redis.HdfsfilesRepository;
import cn.edu.ncut.hdfscombine.service.HandleFileService;
import cn.edu.ncut.hdfscombine.socket.FileServer;

/**
 * Servlet implementation class InitHdfscombine
 */
@WebServlet(urlPatterns = "/InitHdfscombine", loadOnStartup = 1)
public class InitHdfscombine extends HttpServlet {
	
	private static final long serialVersionUID = 1L;

	/**
	 * @see Servlet#init(ServletConfig)
	 */
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		Thread thread = new Thread(new FileServer());
		thread.setDaemon(true);
		thread.start();
		System.out.println("Socket接收服务启动完成");
		ApplicationContext ac = WebApplicationContextUtils
				.getRequiredWebApplicationContext(config.getServletContext());
		HdfsfilesRepository hdfsfilesRepository = (HdfsfilesRepository) ac
				.getBean("hdfsfilesRepository");
		HandleFileService.setHdfsfilesRepository(hdfsfilesRepository);
	}

}
