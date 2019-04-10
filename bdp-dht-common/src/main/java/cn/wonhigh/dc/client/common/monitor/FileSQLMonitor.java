package cn.wonhigh.dc.client.common.monitor;

import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import cn.wonhigh.dc.client.common.util.PropertyFile;

/**
 * 监控类.
 * @author xiao.py
 *
 */
public class FileSQLMonitor {
	/**
	 * 监控对象FileAlterationMonitor monitor
	 */
	FileAlterationMonitor monitor = null;

	/**
	 * 默认10妙看一次
	 * 10000
	 * @param ob
	 */
	public FileSQLMonitor(FileAlterationObserver ob){
		this(PropertyFile.getLongValue(MessageConstant.CONFIG_SCAN_TIME, 10000),ob);
	}

	/**
	 * 每隔多少时候看一次,观察者
	 * @param fileName
	 * @param ob
	 */
	public FileSQLMonitor(long interval,FileAlterationObserver ob){
		monitor = new FileAlterationMonitor(interval,new FileAlterationObserver[]{ob});
	}

	/**
	 * 添加观察者
	 * @param observer
	 */
	public void addObserver(FileAlterationObserver observer){
		monitor.addObserver(observer);
	}

	/**
	 * 移除观察者
	 * @param observer
	 */
	public void removeObserver(FileAlterationObserver observer){
		monitor.removeObserver(observer);
	}

	/**
	 * 获取所有观察者
	 * @return
	 */
	public Iterable<FileAlterationObserver> getObservers() {
		return monitor.getObservers();
	}

	/**
	 * 启动监控[observer.initialize()]
	 */
	public void start(){
		try {
			monitor.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 停止监控[observer.destroy()]
	 */
	public void stop(){
		try {
			monitor.stop();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * [不做调用]
	 * 具体的监控操作:
	 *     observer.checkAndNotify()
	 */
	private void run(){
		monitor.run();
	}


}

