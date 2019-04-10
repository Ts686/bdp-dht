package cn.wonhigh.dc.client.common.util;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {

	/**
	 * 将list集合拼成指定的字符串
	 * 
	 * @param arrayList
	 * @return
	 */
	public static String list2String(List<String> arrayList, String splitChar) {
		StringBuffer sbBuffer = new StringBuffer();
		for (String str : arrayList) {
			sbBuffer.append(str);
			sbBuffer.append(splitChar);
		}
		if (sbBuffer.length() > 0) {
			sbBuffer.delete(sbBuffer.length() - 1, sbBuffer.length());
		}
		return sbBuffer.toString();
	}
	
	/**
	 * 将list集合拼成指定的字符串
	 * 
	 * @param arrayList
	 * @return
	 */
	public static String list2String1(List<Integer> arrayList, String splitChar) {
		StringBuffer sbBuffer = new StringBuffer();
		for (Integer str : arrayList) {
			sbBuffer.append(str);
			sbBuffer.append(splitChar);
		}
		if (sbBuffer.length() > 0) {
			sbBuffer.delete(sbBuffer.length() - 1, sbBuffer.length());
		}
		return sbBuffer.toString();
	}

	/**
	 * 统计字符串中出现子串的总数
	 * 忽略大小写统计
	 * @param str
	 * @param subStr 子串
	 * @return
	 */

	public static  int countStringContainSubString(String str,String subStr){
		int count = 0;
		Pattern p = Pattern.compile(subStr, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(str);
		while (m.find()) {
			count++;
		}
		return count;
	}

	/**
	 * 统计字符串中出现子串的总数
	 * 大小写敏感
	 * @param str
	 * @param subStr
	 * @return
	 */
	public static  int countContainSubSensitive(String str,String subStr){
		int count = 0;
		Pattern p = Pattern.compile(subStr);
		Matcher m = p.matcher(str);
		while (m.find()) {
			count++;
		}
		return count;
	}
}
