DROP TABLE IF EXISTS retail_pos_assistant_signin;
DROP TABLE IF EXISTS retail_pos_assistant_signin_cln;
CREATE TABLE IF NOT EXISTS retail_pos_assistant_signin (
  id varchar(32)  COMMENT '主键',
  assistant_type tinyint  COMMENT '人员类型,1-店铺 2-营运',
  add_type tinyint  COMMENT '新增类型,1-正常新增 2-补签',
  assistant_code varchar(20)  COMMENT '营业员工号',
  assistant_name varchar(32)  COMMENT '营业员姓名',
  shop_no varchar(18)  COMMENT '门店编号',
  shop_name varchar(100)  COMMENT '门店名称',
  dependence_shop_no varchar(18)  COMMENT '从属店编号',
  dependence_shop_name varchar(100)  COMMENT '从属店名称',
  newest_dependence_shop_no varchar(18)  COMMENT '最新从属店编号',
  newest_dependence_shop_name varchar(100)  COMMENT '最新从属店名称',
  signin_type tinyint  COMMENT '考勤类型,1-上班 2-下班',
  signin_little_type tinyint  COMMENT '考勤细类,1-正常（上下班）、2-活动、3-午餐、4-晚餐',
  signin_time timestamp  COMMENT '登记时间',
  remark varchar(255)  COMMENT '备注',
  create_user_no varchar(20)  COMMENT '建档人',
  create_user_name varchar(32)  COMMENT '建档人姓名',
  create_time timestamp  COMMENT '建档时间',
  update_user_no varchar(20)  COMMENT '最后修改人',
  update_user_name varchar(32)  COMMENT '最后修改人姓名',
  update_time timestamp  COMMENT '最后修改时间',
  is_valid tinyint  COMMENT '是否有效,1-有效 0-无效',
  sharding_flag varchar(20)  COMMENT '分库字段',
  yw_update_time timestamp COMMENT 'dc定义业务时间',
  hive_create_time timestamp
) COMMENT '考勤登记' PARTITIONED BY (biz_date int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS TEXTFILE;
CREATE TABLE retail_pos_assistant_signin_cln LIKE retail_pos_assistant_signin;
