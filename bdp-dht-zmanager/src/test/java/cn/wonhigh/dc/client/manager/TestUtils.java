package cn.wonhigh.dc.client.manager;

import cn.wonhigh.dc.client.common.util.DateUtils;

import java.util.Date;

public class TestUtils {
    public static void main(String[] args) throws Exception{

        System.out.println(DateUtils.formatDatetime(new Date(1552963849194L)));
        System.out.println(DateUtils.getHeadDate(new Date(),-2).getTime());

        new Thread().start();
    }
}
