package cn.wonhigh.dc.client.common.monitor;

import cn.wonhigh.dc.client.common.util.ParseProXMLFileUtil;
import cn.wonhigh.dc.client.common.util.ParseSQLXMLFileUtil;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;
import com.yougou.logistics.base.common.exception.ManagerException;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.log4j.Logger;

import java.io.File;

//目录文件监听      提供各种监听时候的事件处理
public class FileSQLListener extends FileAlterationListenerAdaptor{
	private static final Logger logger = Logger.getLogger(FileSQLListener.class);
	@Override
	public void onDirectoryChange(File directory) {
		logger.warn("文件目录变更了:"+directory.getAbsolutePath());
	}

	@Override
	public void onDirectoryCreate(File directory) {
		logger.warn("文件目录创建了:"+directory.getAbsolutePath());
	}

	@Override
	public void onDirectoryDelete(File directory) {
		logger.warn("文件目录删除了:"+directory.getAbsolutePath());
	}

	@Override
	public void onFileChange(File file) {
		logger.warn("文件变更了:"+file.getAbsolutePath());
	    this.saveOrUpdate(file);
	}

	@Override
	public void onFileCreate(File file) {
       logger.warn("文件创建了:"+file.getAbsolutePath());
       this.saveOrUpdate(file);
      
	}
    
	public void saveOrUpdate(File file){
		 //判断更新的文件目录
	       String path = file.getAbsolutePath();
	       String filename=file.getName();
	      if ((filename != null) && (filename.length() > 0)) {   
	             int dot = filename.lastIndexOf('.');   
	             if ((dot >-1) && (dot < (filename.length() - 1))) {   
	                   String prifx=filename.substring(dot + 1);
	                   logger.info("前缀是：" + prifx);
	                   if(!prifx.equals("xml")){
	                	   logger.info("文件后缀名不符合规范 不予刷如内存");
	                	   return;
	                   }
	             }   
	         }   
	        if( path.indexOf("data_analysis") > 0){
	        	try {
	        		ParseSQLXMLFileUtil.saveOrUpdate(file);
		  			} catch (ManagerException e) {
		  				e.printStackTrace();
		  			} 
	        }
	}
	@Override
	public void onFileDelete(File file) {
		logger.warn("文件删除了:"+file.getAbsolutePath());
	}

	@Override
	public void onStart(FileAlterationObserver observer) {
		logger.debug("开始监听:"+observer.getDirectory());
	}

	@Override
	public void onStop(FileAlterationObserver observer) {
		logger.debug("停止监听:"+observer.getDirectory());
	}

}

