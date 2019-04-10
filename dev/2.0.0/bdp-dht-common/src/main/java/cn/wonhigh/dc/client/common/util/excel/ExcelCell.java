package cn.wonhigh.dc.client.common.util.excel;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * TODO: Excel列
 * 
 * @author yuan.yc
 * @date 2014-9-25 下午1:36:35
 * @version 0.1.0 
 * @copyright yougou.com 
 */
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface ExcelCell {
	/**
	 * excel中的标题
	 * @return
	 */
	String value();

}
