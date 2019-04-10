package cn.wonhigh.dc.client.common.util.excel;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * TODO: 增加描述
 * 
 * @author yuan.yc
 * @date 2014-9-26 上午11:10:16
 * @version 0.1.0 
 * @copyright yougou.com 
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface Excel {
    public enum Struct {
        ANNOTATION, XML
    }
    
    /**
     * 
     * TODO: 行注解类型
     * SIMPLE：字段名称;适用于单标题行导入
     * MULTIPLE：Excel列标，如A,B,C,D;适用于多标题行或有合并列导入，如尺码横排导入
     */
    public enum TitleType {
        SIMPLE, MULTIPLE
    }
    /**
     * 从多少行开始解析数据
     * @return
     */
    int start() default 2;
    /**
     * 解析到多少行结束（暂时没实现）
     * @return
     */
    int end() default Integer.MAX_VALUE;
    /**
     * 默认使用注解配置
     * @return
     */
    Struct struct() default Struct.ANNOTATION;
    
    TitleType titleType() default TitleType.SIMPLE;
}
