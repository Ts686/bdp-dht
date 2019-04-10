package cn.wonhigh.dc.client.common.util;

import java.util.UUID;


/**
 * uuid工具
 * 
 * @author wangl
 *
 */
public class GernerateUuidUtils {
	/**  
     * 生成32位作业id  
     * @return string  
     */    
    public static String getUUID(){    
        String uuid = UUID.randomUUID().toString().trim().replaceAll("-", "");    
        return uuid;    
    }
}
