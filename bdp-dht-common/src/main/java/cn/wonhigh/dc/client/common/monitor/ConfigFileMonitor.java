package cn.wonhigh.dc.client.common.monitor;

import java.io.File;

import com.yougou.logistics.base.common.exception.ManagerException;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;

public class ConfigFileMonitor {
    /**
     * 监控配置文件
     */
	public static void  startMonitor(){
		File file = new File(MessageConstant.WINDOW_CONFIG_TASK_XML_PATH);
		if (!file.exists()) {
			file = new File(MessageConstant.LINUX_CONFIG_TASK_XML_PATH);
		}
		FileObserver ob = new FileObserver(file.getAbsolutePath());
		FileListener listener = new FileListener();
		ob.addListener(listener);
		FileMonitor monitor = new FileMonitor(ob);
		monitor.start();
	}
	
	
	public static void main(String args[]){
		try {
			ParseXMLFileUtil.initTask();
		} catch (ManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		startMonitor();
		new Thread(new Runnable(){

			@Override
			public void run() {
				// TODO Auto-generated method stub
				while(true){
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println(ParseXMLFileUtil.getDbById(1));
					
				}

				
			}
			
		}).start();
	}
}

