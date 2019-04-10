DROP TABLE IF EXISTS usr_dc_ods.all_task_latest_status;

CREATE TABLE IF NOT EXISTS usr_dc_ods.all_task_latest_status (
  group_name text NOT NULL,
  task_name text NOT NULL, 
  finshed_time timestamp ,
  status text NOT NULL,
  PRIMARY KEY (group_name, task_name)
);

COMMENT ON COLUMN usr_dc_ods.all_task_latest_status.group_name IS '组名';
COMMENT ON COLUMN usr_dc_ods.all_task_latest_status.task_name IS '调度名称';
COMMENT ON COLUMN usr_dc_ods.all_task_latest_status.finshed_time IS '完成时间';
COMMENT ON COLUMN usr_dc_ods.all_task_latest_status.status IS '状态';
COMMENT ON TABLE usr_dc_ods.all_task_latest_status IS 'bi 同步时间依据表';