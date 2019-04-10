package cn.wonhigh.dc.client.manager.task;

import cn.wonhigh.dc.client.manager.ExecuteShellTaskImpl;
import com.yougou.logistics.base.common.vo.scheduler.RemoteJobInvokeParamsDto;
import org.junit.Test;

public class TestShellTask {

    @Test
    public void testExeShell() {
        String jobId="xxxxxx_test_shell_job";

        RemoteJobInvokeParamsDto remoteJobInvokeParamsDto = new RemoteJobInvokeParamsDto();
        remoteJobInvokeParamsDto.addParam("shellName","/Users/richmo/work/data/hello.sh");

        //业务参数:默认 $1:开始时间 $2:结束时间 $3:真正的业务参数
        remoteJobInvokeParamsDto.addParam("name","moyongfeng");
        remoteJobInvokeParamsDto.addParam("age","18");

        //businessParamNames:规定$3之后参数的顺序
        remoteJobInvokeParamsDto.addParam("businessParamNames","name,age");



        ExecuteShellTaskImpl executeShellTask = new ExecuteShellTaskImpl();

        executeShellTask.executeJobWithParams(jobId,"test","test",remoteJobInvokeParamsDto);


    }
}
