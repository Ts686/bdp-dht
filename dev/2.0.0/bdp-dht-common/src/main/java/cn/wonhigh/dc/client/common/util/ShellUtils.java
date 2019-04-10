package cn.wonhigh.dc.client.common.util;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import cn.wonhigh.dc.client.common.vo.ShellVo;

/**
 * java调用脚本工具
 * 
 * @author zhangc
 *
 */
public class ShellUtils {

	/**
	 * 通过ssh执行cmd命令
	 * @param host
	 * @param user
	 * @param password
	 * @param cmd
	 * @return
	 * @throws IOException
	 */
	public static ShellVo runSSH(String host, String user, String password,
			String cmd) throws IOException {
		Connection conn = getOpenedConnection(host, user, password);
		/* Create a session */
		Session sess = conn.openSession();
		sess.execCommand(cmd);

		InputStream stdout = new StreamGobbler(sess.getStdout());
		InputStream stderr = new StreamGobbler(sess.getStderr());

		BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdout));
		BufferedReader stderrReader = new BufferedReader(new InputStreamReader(stderr));

		ShellVo shellVo = new ShellVo();

		StringBuilder info = new StringBuilder();
		while (true) {
			String line = stdoutReader.readLine();
			if (line == null)
				break;
			info.append(line).append("\n");
		}

		StringBuilder error = new StringBuilder();
		while (true) {
			String line = stderrReader.readLine();
			if (line == null)
				break;
			error.append(line).append("\n");
		}

		/* Close this session */
		sess.close();
		/* Close the connection */
		conn.close();

		shellVo.setStdout(info.toString());
		shellVo.setStderr(error.toString());
		shellVo.setStatus(sess.getExitStatus());

		return shellVo;
	}

	/**
	 * 获得ssh连接
	 * @param host
	 * @param user
	 * @param password
	 * @return
	 * @throws IOException
	 */
	private static Connection getOpenedConnection(String host, String user, String password) throws IOException {
		/* Create a connection instance */
		Connection conn = new Connection(host);
		conn.connect();
		/* Authenticate */
		boolean isAuthenticated = conn.authenticateWithPassword(user, password);
		if (isAuthenticated == false)
			throw new IOException("Authentication failed.");
		return conn;
	}

	/**
	 * 执行本地cmd命令
	 * @param cmd
	 * @return
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static ShellVo runLocal(String... cmd) throws IOException, InterruptedException {
		Runtime rt = Runtime.getRuntime();
		Process p = rt.exec(cmd);
		int exitValue = p.waitFor();//后期增加，防止执行耗时cmd命令时异常退出
		InputStream stdout = new StreamGobbler(p.getInputStream());
		InputStream stderr = new StreamGobbler(p.getErrorStream());

		BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(stdout));
		BufferedReader stderrReader = new BufferedReader(new InputStreamReader(stderr));

		ShellVo shellVo = new ShellVo();

		StringBuilder info = new StringBuilder();
		while (true) {
			String line = stdoutReader.readLine();
			if (line == null)
				break;
			info.append(line).append("\n");
		}

		StringBuilder error = new StringBuilder();
		while (true) {
			String line = stderrReader.readLine();
			if (line == null)
				break;
			error.append(line).append("\n");
		}

		shellVo.setStdout(info.toString());
		shellVo.setStderr(error.toString());
		shellVo.setStatus(p.exitValue());

		return shellVo;
	}
	
	
	 public static ShellVo executeShellFile(String shellCommand) throws IOException {
	        System.out.println("shellCommand:"+shellCommand);
	        int success = 0;
			ShellVo shellVo = new ShellVo();
	        StringBuffer stringBuffer = new StringBuffer();
	        BufferedReader bufferedReader = null;
	        try {
	            Process pid = null;
	            String[] cmd = { "/bin/sh", "-c", shellCommand };
	           // String  cmd=shellCommand;
	            // 执行Shell命令
	            pid = Runtime.getRuntime().exec(cmd);
	            if (pid != null) {
	                stringBuffer.append("进程号：").append(pid.toString())
	                        .append("\r\n");
	                // bufferedReader用于读取Shell的输出内容
	                bufferedReader = new BufferedReader(new InputStreamReader(pid.getInputStream()), 1024);
	                pid.waitFor();
	                success = 1;
	            } else {
	                stringBuffer.append("没有pid\r\n");
	                success = 0;
	            }
	            String line = null;
	            // 读取Shell的输出内容，并添加到stringBuffer中
	            while (bufferedReader != null
	                    && (line = bufferedReader.readLine()) != null) {
	                stringBuffer.append(line).append("\r\n");
	            }
	            System.out.println("stringBuffer:"+stringBuffer);
	        } catch (Exception ioe) {
	        	 success = 0;
	            stringBuffer.append("执行Shell命令时发生异常：\r\n").append(ioe.getMessage())
	                    .append("\r\n");
	        } finally {
	            if (bufferedReader != null) {
	                try {
	                    bufferedReader.close();
	                    // 将Shell的执行情况输出到日志文件中
	                  //  OutputStream outputStream = new FileOutputStream(executeShellLogFile);
	                    shellVo.setStdout(stringBuffer.toString());
	                } catch (Exception e) {
	                    e.printStackTrace();
	                } 
	            }
	           
	        }
	    	shellVo.setStatus(success);
	        return shellVo;
	    }
	public static void main(String[] args) throws IOException, InterruptedException {
//		String host = "172.17.210.120";
//		String user = "root";
//		String password = "172.17.210.120_Nnc0i4&1)P72";
//		String cmds = "ls;ps -ef | grep java;sh date.sh &";
//		//String cmd = "sh -x " + "/usr/local/wonhigh/test/shTest.sh" ;
//		//String cmd="/usr/local/sqoop-1.4.5/bin/sqoop import --connect jdbc:mysql://172.17.210.180:3306/retail_mdm --username retail_mdm --password retail_mdm --query 'select * from brand b where $CONDITIONS  limit 100000'  --split-by b.name --fields-terminated-by '\t' --lines-terminated-by '\n' --hive-import --append --create-hive-table --hive-table brand --null-string 'NULL' --null-non-string 'NULL'  --target-dir /user/hive/warehouse/brand";
//		String cmd="/usr/local/apache-hive-0.13.1-bin/bin/hive";
//		ShellVo shellVo = ShellUtils.runSSH(host, user, password, cmd);
//		System.out.println("标准输出：\n" + shellVo.getStdout());
//		System.out.println("错误输出：\n" + shellVo.getStderr());
//		System.out.println("返回值：" + shellVo.getStatus());
//
//		shellVo = ShellUtils.runLocal("netstat -an");
//		System.out.println("标准输出：\n" + shellVo.getStdout());
//		System.out.println("错误输出：\n" + shellVo.getStderr());
//		System.out.println("返回值：" + shellVo.getStatus());
		
		ShellVo  shellvo =ShellUtils.executeShellFile("cmd /c start  " +  
                 
                "F:\\start.bat");
		System.out.println(shellvo.getStatus());
	}

}
