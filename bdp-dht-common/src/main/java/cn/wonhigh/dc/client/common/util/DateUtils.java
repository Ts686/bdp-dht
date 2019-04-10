package cn.wonhigh.dc.client.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import cn.wonhigh.dc.client.common.constans.CommonEnumCollection.DateFormatStrEnum;

/**
 * 日期工具
 * 
 * @author zhangc
 *
 */
public class DateUtils {

	public static final String FORMAT_DT = "yyyy-MM-dd HH:mm:ss";

	public static String formatDatetime() {
		return formatDatetime(new Date(), FORMAT_DT);
	}

	public static String formatDatetime(String format) {
		return formatDatetime(new Date(), format);
	}

	public static String formatDatetime(Date date) {
		return formatDatetime(date, FORMAT_DT);
	}

	/**
	 * 格式化日期
	 * 
	 * @param date
	 * @param format
	 * @return
	 */
	public static String formatDatetime(Date date, String format) {
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		return formatter.format(date);
	}

	/**
	 * 毫秒转日期
	 * 
	 * @param millis
	 * @return
	 */
	public static Date millisToDate(long millis) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(millis);
		return calendar.getTime();
	}

	/**
	 * Get next day
	 * 
	 * @param date
	 * @return
	 */
	public static Date getNextDay(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH, 1);
		date = calendar.getTime();
		return date;
	}

	/**
	 * 获取想要的时间，前或后
	 * Get WantedDay day
	 *
	 * @param date 当前日期
	 *  @param interval 间隔天数：大于0则表示未来n天，小于0则表示之前n天
	 * @return
	 */
	public static Date getWantedDay(Date date,int interval) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH, interval);
		date = calendar.getTime();
		return date;
	}

	/**
	 * 取到 hours 以前时间
	 * @param hours: 负数：表示当前时间往之前n个小时 正数：未来n个小时
	 * @return
	 */
	public static Date getHeadDate(Date  date ,int hours) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.HOUR_OF_DAY, hours);
		return cal.getTime();
	}
	/**
	 * Get next day
	 * 
	 * @param date
	 * @return
	 */
	public static Date getNextMonth(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.MONTH, 1);
		date = calendar.getTime();
		return date;
	}

	/**
	 * date' nextDayBegin
	 * 
	 * @param date
	 * @return
	 */
	public static Date nextDateBegin(Date date) {
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy/MM/dd");
		Date nextDayBegin = new Date(sdfDate.format(getNextDay(date)));
		return nextDayBegin;
	}

	/**
	 * @param date
	 * @return
	 */
	public static Date nextMonthBegin(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		calendar.add(Calendar.MONTH, 1);
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy/MM/dd");
		Date nextDayBegin = new Date(sdfDate.format(calendar.getTime()));
		return nextDayBegin;
	}

	public static Date getLastDayBegin(Date date) {
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy/MM/dd");
		Date lastDayBegin = new Date(sdfDate.format(getLastDay(date)));
		return lastDayBegin;
	}

	public static Date getTodayBegin(Date date) {
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy/MM/dd");
		Date todayDayBegin = new Date(sdfDate.format(date));
		return todayDayBegin;
	}

	public static Date getLastDay(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH, -1);
		date = calendar.getTime();
		return date;
	}

	/**
	 * 获取两日期之间的日期字符串
	 * 
	 * @param startDate
	 * @param endDate
	 * @param sdfStr
	 * @return
	 */
	public static List<String> getDaysByBeginAndEnd(Date startDate, Date endDate, String sdfStr) {
		List<String> res = new ArrayList<String>();
		SimpleDateFormat sdf = new SimpleDateFormat(sdfStr);
		String format = "";
		while (startDate.before(endDate)) {
			format = sdf.format(startDate);
			res.add(format.replaceAll("-", ""));
			startDate = getNextDay(startDate);
		}
		format = sdf.format(endDate);
		res.add(format.replaceAll("-", ""));
		return res;
	}

	/**
	 * 获取两日期之间的日期字符串
	 * 
	 * @param startDate
	 * @param endDate
	 * @param sdfStr
	 * @return
	 */
	public static List<String> getMonthsByBeginAndEnd(Date startDate, Date endDate, String sdfStr) {
		List<String> res = new ArrayList<String>();
		SimpleDateFormat sdf = new SimpleDateFormat(sdfStr);
		String format = "";
		format = sdf.format(endDate);
		String end = format.replaceAll("-", "").substring(0, 6);
		res.add(format.replaceAll("-", "").substring(0, 6));
		while (startDate.before(endDate)) {
			format = sdf.format(startDate);
			if (!end.startsWith(format.replaceAll("-", "").substring(0, 6))) {
				res.add(format.replaceAll("-", "").substring(0, 6));
			}
			startDate = getNextMonth(startDate);
		}
		return res;
	}

	public static void main(String[] args) throws ParseException {
//		String wmsFormat = DateFormatStrEnum.JAVA_YYYY_MM_DD.getValue();
//		String startTime = "2018-5-4 00:00:00";
//		String endTime = "2018-9-2 00:00:00";
//		SimpleDateFormat sdf = new SimpleDateFormat(wmsFormat);
//		Date startDate = sdf.parse(startTime);
//		Date endDate = sdf.parse(endTime);
//		getMonthsByBeginAndEnd(startDate, endDate, wmsFormat);
		Date date = new Date();
		System.out.println(date);
		System.out.println(getHeadDate(date,-2).getTime());

	}
};