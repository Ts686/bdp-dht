package cn.wonhigh.dc.client.service;


import cn.wonhigh.dc.client.common.model.ResumeHiveTaskInfo;
import cn.wonhigh.dc.client.common.model.ResumeHiveTaskInfoCondition;
import cn.wonhigh.dc.client.dal.mapper.ResumeHiveTaskInfoMapper;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

@Service("resumeHiveTaskInfoService")
public class ResumeHiveTaskInfoServiceImpl implements ResumeHiveTaskInfoService {
    @Autowired
    private ResumeHiveTaskInfoMapper resumeHiveTaskInfoMapper;
    private static final Logger logger = Logger.getLogger(ResumeHiveTaskInfoServiceImpl.class);

    @Override
    public int countByCondition(ResumeHiveTaskInfoCondition example) throws Exception{
        return resumeHiveTaskInfoMapper.countByCondition(example);
    }

    @Override
    public int deleteByCondition(ResumeHiveTaskInfoCondition example) throws Exception{
        return resumeHiveTaskInfoMapper.deleteByCondition(example);
    }

    @Override
    public int deleteByPrimaryKey(Integer id)throws Exception {
        return resumeHiveTaskInfoMapper.deleteByPrimaryKey(id);
    }

    @Override
    public int insert(ResumeHiveTaskInfo record)throws Exception {
        return resumeHiveTaskInfoMapper.insert(record);
    }

    @Override
    public int insertSelective(ResumeHiveTaskInfo record) throws Exception{
       int count = 0 ;
        try {
            count = resumeHiveTaskInfoMapper.insertSelective(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return count;
    }

    @Override
    public List<ResumeHiveTaskInfo> selectByCondition(ResumeHiveTaskInfoCondition example)throws Exception {
        return resumeHiveTaskInfoMapper.selectByCondition(example);
    }

    @Override
    public ResumeHiveTaskInfo selectByPrimaryKey(Integer id)throws Exception {
        return resumeHiveTaskInfoMapper.selectByPrimaryKey(id);
    }

    @Override
    public int updateByConditionSelective(ResumeHiveTaskInfo record, ResumeHiveTaskInfoCondition example) throws Exception{
        return resumeHiveTaskInfoMapper.updateByConditionSelective(record,example);
    }

    @Override
    public int updateByCondition(ResumeHiveTaskInfo record, ResumeHiveTaskInfoCondition example) throws Exception{
        return resumeHiveTaskInfoMapper.updateByCondition(record,example);
    }

    @Override
    public int updateByPrimaryKeySelective(ResumeHiveTaskInfo record) throws Exception{
        return resumeHiveTaskInfoMapper.updateByPrimaryKeySelective(record);
    }

    @Override
    public int updateByPrimaryKey(ResumeHiveTaskInfo record) throws Exception{
        return resumeHiveTaskInfoMapper.updateByPrimaryKey(record);
    }

    /**
     * 根据传入对象，判断是否存在，存在则更新，不存在则插入
     * @param record
     * @return
     */
    @Override
    public int insertOrUpdateInstance(ResumeHiveTaskInfo record) throws Exception{

        int count = 0;

        ResumeHiveTaskInfoCondition condition = new ResumeHiveTaskInfoCondition();
        condition.createCriteria().andJobIdEqualTo(record.getJobId());
        List<ResumeHiveTaskInfo> resumeHiveTaskInfos = resumeHiveTaskInfoMapper.selectByCondition(condition);

        try {
            if(null == resumeHiveTaskInfos || 0 == resumeHiveTaskInfos.size()){
                //没有记录
                record.setUpdateTime(new Date());
                record.setCreateTime(new Date());
                count = resumeHiveTaskInfoMapper.insertSelective(record);
                logger.info("记录hive任务执行失败数据成功");
            }else{
                //加入主键进行更新
                record.setId(resumeHiveTaskInfos.get(0).getId());
                record.setUpdateTime(new Date());
                record.setCreateTime(resumeHiveTaskInfos.get(0).getCreateTime());
                record.setCreateUser(resumeHiveTaskInfos.get(0).getCreateUser());
                record.setCreateUserId(resumeHiveTaskInfos.get(0).getCreateUserId());
                record.setUpdateUser(resumeHiveTaskInfos.get(0).getUpdateUser());

                count = resumeHiveTaskInfoMapper.updateByPrimaryKey(record);
                logger.info("更新hive任务执行失败数据成功");
            }
        } catch (Exception e) {
            logger.error("新增或更新hive任务记录失败："+e.getMessage());
            e.printStackTrace();
        }


        return count;
    }
}
