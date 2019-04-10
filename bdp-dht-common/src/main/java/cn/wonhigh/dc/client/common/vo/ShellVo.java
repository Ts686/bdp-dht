package cn.wonhigh.dc.client.common.vo;

/**
 * java调用脚本返回对象
 * 
 * @author zhangc
 *
 */
public class ShellVo {

	private String stdout;// 标准输出
	private String stderr;// 错误输出
	private int status;// 返回值

	public String getStdout() {
		return stdout;
	}

	public void setStdout(String stdout) {
		this.stdout = stdout;
	}

	public String getStderr() {
		return stderr;
	}

	public void setStderr(String stderr) {
		this.stderr = stderr;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

}
