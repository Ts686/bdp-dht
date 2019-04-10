package cn.wonhigh.dc.client.common.util;

import cn.wonhigh.dc.client.common.constans.MessageConstant;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 执行远程脚本工具
 *    1.bdp-dht服务要与远程服务器之间开通免密登录，否则无法使用该工具
 */
public class RemoteShellExeUtil {

    private static final Logger logger = Logger.getLogger(RemoteShellExeUtil.class);
    private static int default_port = 22;

    /**
     *
     * @param exeUserName :具有运行脚本权限用户
     * @param host ：目标主机ip
     * @param port ：端口，默认为22
     * @param shell ：可运行shell命令，格式为： sh  /xx/xx/xx.sh  p1 p2
     */

    public static String exeShell(String exeUserName,String host,int port,String shell){
        StringBuilder  stringBuilder = new StringBuilder();
        String result = "";

        if(0 == port){
            port = default_port;
        }

        try {
            JSch jsch = new JSch();
            jsch.addIdentity(MessageConstant.DC_CLIENT_PRIVATE_KEY_PATH);
            Session session=jsch.getSession(exeUserName,host, port);//为了连接做准备
            session.setConfig("StrictHostKeyChecking", "no");
            session.connect();
            ChannelExec channel=(ChannelExec)session.openChannel("exec");
            channel.setCommand(shell);


            channel.connect();

            BufferedReader in = new BufferedReader(new InputStreamReader(channel.getInputStream()));

            String msg="";
            while((msg = in.readLine()) != null){
                stringBuilder.append(msg+"\n");
            }
            result = stringBuilder.toString();
            logger.info("执行脚本返回信息：【"+result+"】");
            channel.disconnect();
            session.disconnect();
        } catch (JSchException e) {
            logger.error("RemoteShellExeUtil工具类执行远程脚本出错");
            e.printStackTrace();
        } catch (IOException e) {
            logger.error("RemoteShellExeUtil工具类执行远程脚本出错");
            e.printStackTrace();
        }

        return result;

    }
}
