package cn.wonhigh.dc.client.common.monitor;

import java.io.File;

import com.yougou.logistics.base.common.exception.ManagerException;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.util.ParseSQLXMLFileUtil;
import cn.wonhigh.dc.client.common.util.ParseXMLFileUtil;

public class ConfigSQLFileMonitor {
    /**
     * 监控配置文件
     */
	public static void  startMonitor(){
		File file = new File(MessageConstant.WINDOW_EXECUTE_SQL_TASK_XML_PATH);
		if (!file.exists()) {
			file = new File(MessageConstant.LINUX_EXECUTE_SQL_TASK_XML_PATH);
		}
		FileSQLObserver ob = new FileSQLObserver(file.getAbsolutePath());  //调用第一参数的构造函数
		FileSQLListener listener = new FileSQLListener();
		ob.addListener(listener);  // 添加监听器     参数必须继承FileAlterationListener
		FileSQLMonitor monitor = new FileSQLMonitor(ob);  //参数必须继承FileAlterationObserver
		monitor.start();  //启动监控[observer.initialize()]
	}
	
	
	public static void main(String args[]){
		try {
			ParseSQLXMLFileUtil.initTask();
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
					System.out.println(ParseSQLXMLFileUtil.getTaskConfig("db_rest_client", "shop_pepole_flow_data_2").getSourceDbId());
					
				}

				
			}
			
		}).start();
	}
}

