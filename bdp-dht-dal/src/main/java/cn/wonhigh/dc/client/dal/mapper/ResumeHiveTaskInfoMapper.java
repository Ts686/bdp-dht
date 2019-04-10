package cn.wonhigh.dc.client.dal.mapper;

import cn.wonhigh.dc.client.common.model.ResumeHiveTaskInfo;
import cn.wonhigh.dc.client.common.model.ResumeHiveTaskInfoCondition;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface ResumeHiveTaskInfoMapper {
    int countByCondition(ResumeHiveTaskInfoCondition example)throws Exception;

    int deleteByCondition(ResumeHiveTaskInfoCondition example)throws Exception;

    int deleteByPrimaryKey(Integer id)throws Exception;

    int insert(ResumeHiveTaskInfo record)throws Exception;

    int insertSelective(ResumeHiveTaskInfo record) throws Exception;

    List<ResumeHiveTaskInfo> selectByCondition(ResumeHiveTaskInfoCondition example)throws Exception;

    ResumeHiveTaskInfo selectByPrimaryKey(Integer id)throws Exception;

    int updateByConditionSelective(@Param("record") ResumeHiveTaskInfo record, @Param("example") ResumeHiveTaskInfoCondition example)throws Exception;

    int updateByCondition(@Param("record") ResumeHiveTaskInfo record, @Param("example") ResumeHiveTaskInfoCondition example)throws Exception;

    int updateByPrimaryKeySelective(ResumeHiveTaskInfo record)throws Exception;

    int updateByPrimaryKey(ResumeHiveTaskInfo record)throws Exception;
}