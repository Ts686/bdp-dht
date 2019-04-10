package cn.wonhigh.dc.client.common.util;

import org.apache.commons.lang.exception.ExceptionUtils;


public class ExceptionHandleUtils {
	public static String getExceptionMsg(Throwable e){
		if(e == null){
			return "";
		}
		String msg = ExceptionUtils.getFullStackTrace(e);
		return msg;
	}

}
