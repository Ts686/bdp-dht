package cn.wonhigh.dc.client.sqoop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * All rights reserved.
 *
 * @author Qiuzhuang.Lian
 *
 * This is an example on how to use sqoop API.
 */
public class SqoopApiExample {
  private static final Logger log = LoggerFactory.getLogger(SqoopApiExample.class);

  public static void main(String [] args) throws MaxHadoopJobRequestsException {
    final SqoopApi sqoopApi = SqoopApi.getSqoopApi();
    testSqoopApiExecuteSucceed(sqoopApi);
    testSqoopApiExecuteFailed(sqoopApi);
    // testMaxJobLimit(sqoopApi);
    System.exit(0);
  }

  private static void testMaxJobLimit(final SqoopApi sqoopApi) {
    ExecutorService service = Executors.newFixedThreadPool(4);
    try {
      final String[] importArgs = hiveImportArgs(
          "jdbc:mysql://172.17.210.134:8066/retail_mps",
          "retail_mps",
          "retail_mps",
          "select * from sale_assistant where $CONDITIONS",
          "/tmp/retail_mps/sale_assistant",
          "\\t",
          "\\\\N",
          "\\\\N",
          "/root/tmp/retail_mps/sale_assistant",
          "/root/tmp/retail_mps/sale_assistant",
          "default",
          "qiuzhuang_import",
          1);
      service.execute(new Runnable() {
        @Override
        public void run() {
          try {
            sqoopApi.execute("job1", importArgs);
          } catch (MaxHadoopJobRequestsException e) {
            log.error("error of MaxHadoopJobRequestsException", e);
          }
        }
      });

      final String[] exportArgs = hiveExportArgs(
          "jdbc:mysql://172.17.210.64:3306/sqoop_export_lqz",
          "root",
          "123456",
          "sale_assistant",
          "\\t",
          "\\\\N",
          "\\\\N",
          "id",
          "allowinsert",
          "/hive/warehouse/qiuzhuang_import",
          4
      );
      service.execute(new Runnable() {
        @Override
        public void run() {
          try {
            sqoopApi.execute("job2", exportArgs);
          } catch (MaxHadoopJobRequestsException e) {
            log.error("error of MaxHadoopJobRequestsException", e);
          }
        }
      });

      /*
      service.execute(new Runnable() {
        @Override
        public void run() {
          try {
            sqoopApi.execute(importArgs);
          } catch (MaxHadoopJobRequestsException e) {
            log.error("error of MaxHadoopJobRequestsException", e);
          }
        }
      });
      */
      service.awaitTermination(180, TimeUnit.SECONDS);
      service.shutdown();
    } catch (Exception e) {
      log.error("error in testMaxJobLimit example", e);
    }
    log.info(String.format("the current request is %d", sqoopApi.countRequests()));
  }


  private static void testSqoopApiExecuteSucceed(SqoopApi sqoopApi) throws MaxHadoopJobRequestsException {
    Map<String, String> paras = new HashMap<String, String>();
    paras.put("--connect", "jdbc:mysql://172.17.210.36:3307/retail_pms_replenish");
    paras.put("--username", "retail_pms_rep");
    paras.put("--password", "password");
    paras.put("--query", "select * from bill_grade_plan where $CONDITIONS");
    paras.put("--target-dir", "/tmp/retail_mps/bill_grade_plan");
    paras.put("--fields-terminated-by", "\\t");
    paras.put("--input-null-string", "\\\\N");
    paras.put("--input-null-non-string", "\\\\N");
    paras.put("-m", "1");
    paras.put("--hive-table", "bill_grade_plan_hive");
    paras.put("--hive-database", "default");
    String command = "import";
    List<String> options = new ArrayList<String>();
    options.add("--hive-import");
    JobExecutionResult jobExecutionResult = sqoopApi.execute("succeedJob", command, paras, options);
    log.warn(String.format("get succeedJob's execution result %s",
        (jobExecutionResult.isSucceed()? "OK" : "Failed")));
  }

  private static void testSqoopApiExecuteFailed(SqoopApi sqoopApi) throws MaxHadoopJobRequestsException {
    Map<String, String> paras = new HashMap<String, String>();
    paras.put("--connect", "jdbc:mysql://172.17.210.134:3306/retail_mps");
    paras.put("--username", "retail_mpsddd");
    paras.put("--password", "retail_mps");
    paras.put("--query", "select * from sale_assistant where $CONDITIONS");
    paras.put("--target-dir", "/tmp/retail_mps/sale_assistant");
    paras.put("--fields-terminated-by", "\\t");
    paras.put("--input-null-string", "\\\\N");
    paras.put("--input-null-non-string", "\\\\N");
    paras.put("-m", "1");
    paras.put("--hive-table", "wanglei");
    paras.put("--hive-database", "default");
    String command = "import";
    List<String> options = new ArrayList<String>();
    options.add("--hive-import");
    JobExecutionResult jobExecutionResult = sqoopApi.execute("failedJob", command, paras, options);
    log.warn(String.format("get failedJob's execution result %s, exception: %s, error message is: %s",
        (jobExecutionResult.isSucceed()? "OK" : "Failed"),
        jobExecutionResult.getException(),
        jobExecutionResult.getErrorMessage()));
  }

  private static final String[] hiveImportArgs(
      String url, String user, String password, String query,
      String targetDir, String fieldsBy, String inputNullString,
      String inputNullNotString, String binDir, String outDir,
      String importedDatabase, String importedTable, int m) {
    String[] importArgs =  {
        "import",
        "--connect", url,
        "--username", user,
        "--password", password,
        "--query", query,
        "--target-dir", targetDir,
        "--fields-terminated-by", fieldsBy,
        "--input-null-string", inputNullString,
        "--input-null-non-string", inputNullNotString,
        "--bindir", binDir,
        "--outdir", outDir,
        "-m", String.valueOf(m),
        "--hive-table", importedTable,
        "--hive-database", importedDatabase,
        "--hive-import"
    };
    return importArgs;
  }

  private static final String[] hiveExportArgs(
      String url, String user, String password, String targetTable,
      String fieldsBy, String nullString, String nullNotString,
      String updateByColumn, String updateMode, String exportDir, int m) {
    String[] exportArgs =  {
        "export",
        "--connect", url,
        "--username", user,
        "--password", password,
        "--table", targetTable,
        "--verbose", "-m", String.valueOf(m),
        "--fields-terminated-by", fieldsBy,
        "--null-string", nullString,
        "--null-non-string", nullNotString,
        "--update-key", updateByColumn,
        "--update-mode", updateMode,
        "--export-dir", exportDir,
    };
    return exportArgs;
  }
}
