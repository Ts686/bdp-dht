#-------------------------------------------------------------------------------
#  客户端各表结构脚本
#  version: bdp-dht-0.9.0
#
#  MODIFIED     (MM/DD/YY)
#  zhang.c1      04/27/15   - 创建
#  wang.l1      04/30/15   - 修改模板
#
#-------------------------------------------------------------------------------

#创建任务日志轨迹表
SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for client_task_status_log
-- ----------------------------

CREATE TABLE client_task_status_log (
  id int(11) NOT NULL AUTO_INCREMENT COMMENT '日志id',
  task_id varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '任务id',
  group_name varchar(255) COLLATE utf8_bin DEFAULT NULL COMMENT '调度任务组名',
  scheduler_name varchar(255) COLLATE utf8_bin DEFAULT NULL COMMENT '调度任务名',
  create_time datetime DEFAULT NULL COMMENT '任务创建时间',
  task_status varchar(255) COLLATE utf8_bin DEFAULT NULL COMMENT '任务状态',
  task_status_desc varchar(1000) COLLATE utf8_bin DEFAULT NULL COMMENT '任务状态描述',
  sync_begin_time datetime DEFAULT NULL COMMENT '同步开始时间',
  sync_end_time datetime DEFAULT NULL COMMENT '同步结束时间' ,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT '任务日志轨迹表';

create index idx_client_task_status_log_1 on client_task_status_log(group_name,scheduler_name);
create index idx_client_task_status_log_2 on client_task_status_log(create_time);