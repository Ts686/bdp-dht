package cn.wonhigh.dc.client.common.constans;

/**
 * 枚举集合
 *
 * @author wang.w
 */
public class CommonEnumCollection {

    /**
     * 任务类型对应的说明
     *
     * @author wang.w
     */
    public enum TaskTypeEnum {
        SQOOP_IMPORT(MessageConstant.SQOOP_IMPORT, "import"), SQOOP_EXPORT(MessageConstant.SQOOP_EXPORT,
                "export"), SQOOP_EVAL(MessageConstant.SQOOP_EVAL, "eval");

        private final Integer key;
        private final String value;

        TaskTypeEnum(Integer key, String value) {
            this.key = key;
            this.value = value;
        }

        /**
         * 根据传入的key，获得任务类型的value
         *
         * @param value
         * @return
         */
        public static String getCommand(Integer key) {
            if (key != null) {
                for (TaskTypeEnum taskType : values()) {
                    if (key.equals(taskType.key)) {
                        return taskType.value;
                    }
                }
            }
            return null;
        }
    }

    /**
     * 状态值集合
     *
     * @author wang.w
     */
    public enum CommonStatusEnum {
        OPEN_STATUS(1), CLOSE_STATUS(0);

        private final Integer value;

        CommonStatusEnum(Integer value) {
            this.value = value;
        }

        public Integer getValue() {
            return value;
        }
    }

    /**
     * 数据库类型集合
     *
     * @author wang.w
     */
    public enum DbTypeCollecEnum {
        MYSQL(0, "com.mysql.jdbc.Driver"), POSTGRESQL(1, "org.postgresql.Driver"), HIVE(2,
                "org.apache.hive.jdbc.HiveDriver"), ORACLE(3, "oracle.jdbc.OracleDriver"), SQLSERVER(4,
                "com.microsoft.jdbc.sqlserver.SQLServerDriver"), SYBASE(5,
                "com.sybase.jdbc3.jdbc.SybDriver"), SPARK_HIVE(6,
                "org.apache.hive.jdbc.HiveDriver"), DB2(7, "com.ibm.db2.jcc.DB2Driver");

        private final Integer value;

        private final String driverName;

        DbTypeCollecEnum(Integer value, String driverName) {
            this.value = value;
            this.driverName = driverName;
        }

        public Integer getValue() {
            return value;
        }

        public String getDriverName() {
            return driverName;
        }
    }

    /**
     * 名称前缀，后缀集合
     *
     * @author wang.w
     */
    public enum YW_Type_Enum {
        ALL_TYPE(""), RETAIL_TYPE("and t2.sharding_flag like 'U010101%'"), SPORT_TYPE(
                "and t2.sharding_flag like 'U010102%'");
        private final String value;

        YW_Type_Enum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * 名称前缀，后缀集合
     *
     * @author wang.w
     */
    public enum HiveDefinePartNameEnum {
        DB_NAME_SRC("dc_src."), DB_NAME_ODS("dc_ods."), DB_NAME_DW("dc_dw."), DB_NAME_DM("dc_dm."), DB_NAME_STAGING(
                "dc_staging."), RETAIL_POS_PREFIX("retail_pos"), RETAIL_MPS_PREFIX("retail_mps"), RETAIL_GMS_PREFIX(
                "retail_gms"), RETAIL_MDM_PREFIX("retail_mdm"), RETAIL_PMS_PREFIX(
                "retail_pms"), RETAIL_FMS_PREFIX(
                "retail_fms"), TMP_TABLE_NAME_SUBFIX("_tmp"), STAGE_TABLE_NAME_SUBFIX(
                "_stg"), CLN_TABLE_NAME_SUBFIX("_cln"), CLEANED_TABLE_NAME_SUBFIX(
                "_clnd"), THL_TABLE_NAME_SUBFIX("_thl"), CDC_TABLE_SUBFIX(
                "transaction_history_log_src"), PARTITION_DATE_NAME(
                "partition_date"), SRC_TABLE_NAME_SUBFIX(
                "_src"), ODS_TABLE_NAME_SUBFIX(
                "_ods"), DW_TABLE_NAME_SUBFIX(
                "_dw"), CK_TABLE_NAME_SUBFIX(
                "_ck"), CDC_TABLE_CAPTURE_TIME_COL(
                "capture_time"), CDC_TABLE_EVENT_TIME_COL(
                "dml_event_time"), CDC_TABLE_ID_COLUMN_NAME(
                "id_column_name"), CDC_TABLE_ID_COLUMN_VALUE(
                "id_column_value"), CDC_TABLE_PK_COL(
                "seq_no"), CDC_DB_NAMW(
                "db_name"), CDC_TABLE_NAMW(
                "table_name"), CDC_DML_TYPE_COL(
                "dml_type"), CDC_DML_UPT_TIME_VALUE(
                "update_timestamp_column_value"), EXPORT_SPORT_PERFIX(
                "_sp"), EXPORT_RETAIL_PERFIX(
                "_rt"), EXPORT_ALL_TABLE_NAME_SUBFIX(
                "_all"), HIS_TABLE_TABLE_NAME_SUBFIX(
                "_his");

        private final String value;

        HiveDefinePartNameEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * 数据库所支持的类型
     *
     * @author wang.w
     */
    public enum DbTypeEnum {
        TYPE_STRING("string"), TYPE_VARCHAR("varchar"), TYPE_CHAR("char"), TYPE_TINYINT("tinyint"), TYPE_SMALLINT(
                "smallint"), TYPE_INT("int"), TYPE_BIGINT("bigint"), TYPE_FLOAT("float"), TYPE_DOUBLE(
                "double"), TYPE_DECIMAL("decimal"), TYPE_BOOLEAN("boolean"), TYPE_REAL("real"), TYPE_BINARY(
                "binary"), TYPE_NUMBER("number"), TYPE_INTEGER("integer"), TYPE_CLOB("clob"), TYPE_BLOB(
                "blob"), TYPE_DATE(
                "date"), TYPE_TIME("time"), TYPE_TIMESTAMP("timestamp"), TYPE_VARCHAR2(
                "varchar2"), TYPE_NVARCHAR("nvarchar"), TYPE_LONGVARCHAR(
                "longvarchar"), TYPE_LONGNVARCHAR("longnvarchar");

        private final String value;

        DbTypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * 参数常量名称
     *
     * @author wang.w
     * @version 1.0.0
     * @date 2016-4-13 下午1:36:05
     * @copyright wonhigh.cn
     */
    public enum ParamNameEnum {
        TASK_ID("taskId"), GROUP_NAME("groupName"), TASK_NAME("taskName"), START_TIME("startTime"), END_TIME(
                "endTime"), SYS_NO("sysNo"), SYS_NAME("sysName"), TABLE_NAME("tableName");
        private final String value;

        ParamNameEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * 参数常量名称
     *
     * @author wang.w
     * @version 1.0.0
     * @date 2016-4-13 下午1:36:05
     * @copyright wonhigh.cn
     */
    public enum CDCTableColumnEnum {
        CDC_COLUMN_DB_NAME("db_name"), CDC_COLUMN_TABLE_NAME("table_name");
        private final String value;

        CDCTableColumnEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * 参数常量名称
     *
     * @author wang.w
     * @version 1.0.0
     * @date 2016-4-13 下午1:36:05
     * @copyright wonhigh.cn
     */
    public enum DateFormatStrEnum {
        JAVA_YYYY_MM_DD_HH_MM_SS("yyyy-MM-dd HH:mm:ss"), JAVA_YYYY_MM_DD("yyyy-MM-dd"), MYSQL_YYYY_MM_DD_HH_MM_SS(
                "%Y-%m-%d %H:%i:%s"), PARTITION_DATE_YYYY_MM_DD("yyyyMMdd"), PARTITION_DATE_YYYY_MM("yyyyMM");
        private final String value;

        DateFormatStrEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * 同步/清洗规则类型 0：orc清洗规则 1：parquet清洗规则
     *
     * @author wang.w
     * @version 1.10.0
     * @date 2017-6-27 上午11:48:51
     * @copyright wonhigh.cn
     */
    public enum SyncTypeEnum {
        SYNC_TYPE_0("0"), SYNC_TYPE_1("1"), SYNC_TYPE_2("2");

        private final String value;

        SyncTypeEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    /**
     * 分隔符
     *
     * @author wang.w
     * @version 1.11.0
     * @date 2017-7-27 上午11:48:51
     * @copyright wonhigh.cn
     */
    public enum SplitCharEnum {
        SPLIT_CHAR_0(","), SPLIT_CHAR_1("@"), SPLIT_CHAR_2(";");

        private final String value;

        SplitCharEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
