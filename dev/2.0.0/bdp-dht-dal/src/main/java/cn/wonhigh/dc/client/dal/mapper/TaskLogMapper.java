package cn.wonhigh.dc.client.dal.mapper;

import java.util.Date;
import java.util.List;

import org.apache.ibatis.annotations.Param;

import cn.wonhigh.dc.client.common.model.TaskLog;

import com.yougou.logistics.base.dal.database.BaseCrudMapper;

public interface TaskLogMapper extends BaseCrudMapper {

	public List<TaskLog> selectByCreateTime(
			@Param("triggerName") String triggerName,
			@Param("groupName") String groupName,
			@Param("createTime") Date createTime);

	public Integer getTaskStatus(@Param("jobId") String jobId,
			@Param("triggerName") String triggerName,
			@Param("groupName") String groupName);

}
