package cn.wonhigh.dc.client.sqoop;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * All rights reserved.
 *
 * @author Qiuzhuang.Lian
 *
 * SqoopApi unit test class.
 */
public class TestSqoopApi {
  private static final Logger log = LoggerFactory.getLogger(TestSqoopApi.class);
  private static SqoopApi sqoopApi;

  @BeforeClass
  public static void initSqoopApi() {
    sqoopApi = SqoopApi.getSqoopApi();
  }

  @Before
  public void beforeEachTest() {
    log.info("This is executed before each test");
  }

  @After
  public void afterEachTest() {
    log.info("This is exceuted after each Test");
  }

  @Test
  public void testSqoopApiExecuteSucceed() {
    final String[] importArgs = hiveImportArgs(
        "jdbc:mysql://172.17.210.134:8066/retail_mps",
        "retail_mps",
        "retail_mps",
        "select * from sale_assistant where $CONDITIONS",
        "/tmp/retail_mps/sale_assistant" + UUID.randomUUID().toString(),
        "\\t",
        "\\\\N",
        "\\\\N",
        "/root/tmp/retail_mps/sale_assistant" + UUID.randomUUID().toString(),
        "/root/tmp/retail_mps/sale_assistant" + UUID.randomUUID().toString(),
        "default",
        "qiuzhuang_import",
        1);
    String jobIdPrefix = "sqoop-job-id-" + UUID.randomUUID().toString();
    try {
      JobExecutionResult result = sqoopApi.execute("import-" + jobIdPrefix, importArgs);
      Assert.assertTrue("sqoop import should succeed", result.getJobStatus() == JobStatus.Succeed);
    } catch (MaxHadoopJobRequestsException e) {
      log.error(e.getMessage());
    }
  }

  @Test
  public void testSqoopApiExecuteFailed() {
    final String[] importArgs = hiveImportArgs(
        "jdbc:mysql://172.17.210.134:8066/retail_mps",
        "retail_mps",
        "retail_mps---",
        "select * from sale_assistant where $CONDITIONS",
        "/tmp/retail_mps/sale_assistant" + UUID.randomUUID().toString(),
        "\\t",
        "\\\\N",
        "\\\\N",
        "/root/tmp/retail_mps/sale_assistant" + UUID.randomUUID().toString(),
        "/root/tmp/retail_mps/sale_assistant" + UUID.randomUUID().toString(),
        "default",
        "qiuzhuang_import",
        1);
    String jobIdPrefix = "sqoop-job-id-" + UUID.randomUUID().toString();
    try {
      JobExecutionResult result = sqoopApi.execute("import-" + jobIdPrefix, importArgs);
      Assert.assertTrue("sqoop import should fail", result.getJobStatus() == JobStatus.Failed);
      Assert.assertTrue("error message should not be null.", result.getErrorMessage().length() > 0);
    } catch (MaxHadoopJobRequestsException e) {
      log.error(e.getMessage());
    }
  }

  @Test
  public void testSqoopApiHadoopOptions() {
    String jobIdPrefix = "sqoop-job-id-" + UUID.randomUUID().toString();
    try {
      Map<String, String> hadoop = new HashMap<String, String>();
      hadoop.put("mapred.task.timeout", "40000");
      hadoop.put("mapred.map.max.attempts", "1");
      hadoop.put("mapred.reduce.max.attempts", "1");
      Map<String, String> paras = new HashMap<String, String>();
      paras.put("--export-dir", "/hive/warehouse/dc_ods.db/retail_gms_bill_receipt_dtl_cln_stg20150402105148");
      paras.put("--null-non-string", "\\\\N");
      paras.put("--fields-terminated-by", "\\t");
      paras.put("--null-string", "\\\\N");
      paras.put("--table", "retail_gms_bill_receipt");
      paras.put("--input-null-string", "\\\\N");
      paras.put("-m", "5");
      paras.put("--username", "usr_dc_ods");

      paras.put("--outdir", "/root/outdir");
      paras.put("--connect", "jdbc:postgresql://172.17.209.165:5432/dc_pg");
      paras.put("--password", "usr_dc_ods");
      paras.put("--input-null-non-string", "\\\\N");
      paras.put("--bindir", "/root/bindir");

      List<String> options = new ArrayList<String>();
      options.add("--verbose");

      JobExecutionResult result = sqoopApi.execute(
          "export-" + jobIdPrefix, "export", hadoop, paras, options);
      Assert.assertTrue("sqoop import should fail", result.getJobStatus() == JobStatus.Failed);
      Assert.assertTrue("error message should not be null.", result.getErrorMessage().length() > 0);
    } catch (MaxHadoopJobRequestsException e) {
      log.error(e.getMessage());
    }
  }

  @Test
  public void testSqoopConcurrency() {
    for (int i = 0; i < 1; i++) {
      int resultFailed = runConcurrentSqoopJob(sqoopApi, i, 6);
      Assert.assertTrue("Test failed with " + resultFailed + " tests cycle!", resultFailed == 0);
    }
  }

  private int runConcurrentSqoopJob(final SqoopApi sqoopApi,
                                    final int testCycle,
                                    final int maxJobs) {
    ExecutorService executorService = Executors.newFixedThreadPool(maxJobs);
    long start = System.currentTimeMillis();
    final Vector<JobExecutionResult> resultList = new Vector<JobExecutionResult>();
    final CountDownLatch latch = new CountDownLatch(maxJobs);
    int counter = 0;
    int resultFailed = 0;

    while ((counter++) < maxJobs) {
      final int currentJobId = counter;
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            final String[] listTableArgs = {
                "list-tables",
                "--connect", "jdbc:mysql://172.17.210.134:8066/retail_mps",
                "--username", "retail_mps",
                "--password", "retail_mps"
            };

            final String[] importArgs = hiveImportArgs(
                "jdbc:mysql://172.17.210.134:8066/retail_mps",
                "retail_mps",
                "retail_mps",
                "select * from sale_assistant where $CONDITIONS",
                "/tmp/retail_mps/sale_assistant" + UUID.randomUUID().toString(),
                "\\t",
                "\\\\N",
                "\\\\N",
                "/root/tmp/retail_mps/sale_assistant" + UUID.randomUUID().toString(),
                "/root/tmp/retail_mps/sale_assistant" + UUID.randomUUID().toString(),
                "default",
                "qiuzhuang_import",
                1);
            String jobIdPrefix = "sqoop-job-id-" + testCycle + "-" + currentJobId;
            resultList.add(sqoopApi.execute("listtable-" + jobIdPrefix, listTableArgs));
            //resultList.add(sqoopApi.execute("import-" + jobIdPrefix, importArgs));
          } catch (MaxHadoopJobRequestsException e) {
            log.error("error of MaxHadoopJobRequestsException", e);
          } finally {
            latch.countDown();
          }
        }
      });
    }
    try {
      latch.await();
    } catch (Throwable e) {
      log.error("error of InterruptedException", e);
    } finally {
      log.info(String.format("there are pending jobs of %d", sqoopApi.countRequests()));
      for (JobExecutionResult result: resultList) {
        if (!result.isSucceed()) {
          log.error(result.getSubmitJobId() + " failed:" + result.getErrorMessage());
          ++resultFailed;
        }
      }
      executorService.shutdown();
      long end = System.currentTimeMillis();
      long time = (end - start) / 1000;
      log.info("it takes time " + time + " seconds.");
    }
    return resultFailed;
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
}
