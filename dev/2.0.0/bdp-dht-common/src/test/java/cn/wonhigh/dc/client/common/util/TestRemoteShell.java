package cn.wonhigh.dc.client.common.util;

import com.jcraft.jsch.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class TestRemoteShell {
    public static final String SSH_COMMOND_STR = "exec";

    public static final String CUR_USER_PRI_RSA = "id_rsa";

    public static final String SH_COMMOND_STR = "sh";

    public static final String SH_FILE_NAME_SUBFIX = ".sh";

    private static String localkeyPath = "";
    private static String userName = "";
    private static String host = "";
    private static String port = "";

    private static String commandPath = "";


    /**
     * 判断字符串是否为空或空串
     *
     * @param str
     * @return
     */
    public static boolean isBlank(String str) {
        if (str == null || str.trim().length() == 0) {
            return true;
        }
        return false;
    }

    /**
     * 检查文件是否符合要求 1、文件名不能为空、且要以 .sh 结尾 2、判断文件是否存在 3、判断文件是否具备可执行权限
     *
     * @param remoteShellPath 远程文件路径
     * @param fileName        远程文件名
     * @return
     */
    public static boolean isShellAndExistsFile(String remoteShellPath, String fileName) {
        if (isBlank(fileName)) {
            return false;
        }

        if (!fileName.endsWith(SH_FILE_NAME_SUBFIX)) {
            return false;
        }

        if (!new File(remoteShellPath + fileName).exists()) {
            return false;
        }

        if (!new File(remoteShellPath + fileName).canExecute()) {
            return false;
        }

        return true;
    }

    public static void main(String[] args) throws JSchException, IOException {

        localkeyPath = args[0];
        host = args[1];
        userName = args[2];
        commandPath = "sh "+args[3];
        System.out.println(localkeyPath+" "+host+" "+userName+" "+commandPath);

        JSch jsch = new JSch();
        jsch.addIdentity(localkeyPath);
        Session session=jsch.getSession(userName,host, 22);//为了连接做准备
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();
        ChannelExec channel=(ChannelExec)session.openChannel("exec");
        channel.setCommand(commandPath);
//        channel.setCommand("sh /usr/local/test/hello.sh");

//        Channel channel=session.openChannel("shell");
//        channel.setInputStream(System.in);
//        channel.setOutputStream(System.out);


        channel.connect();

        BufferedReader in = new BufferedReader(new InputStreamReader(channel.getInputStream()));

        String msg;

        while((msg = in.readLine()) != null){
            System.out.println(msg);
        }
        channel.disconnect();
        session.disconnect();
    }
}
