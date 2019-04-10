package cn.wonhigh.dc.client.sqoop;

import org.apache.hadoop.util.RunJar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * All rights reserved.
 *
 * @author Qiuzhuang.Lian
 * <p>
 * This is the exposed class of wonhigh sqoop API, it submits the jar into yarn cluster to
 * run the sqoop job via delegating to {@link cn.wonhigh.dc.client.sqoop.SqoopClient}
 * to invoke Sqoop API(unpublished). This API can control the concurrency of job submissions and
 * detects repeat job submission via its job ID. Also, it can maintain the job execution
 * result status so that DC client can decide next based on the execution result.
 * <p>
 * Since this invokes sqoop API directly there are some caveats when constructing pairs of parameter
 * key/value, namely for those escaped chars like '\t', '\\n' with sqoop commands. They should be mapped
 * to "\\t", "\\\\n". Here is an example for their usage,
 * <p>
 * <pre>
 *       Map<String, String> paras = new HashMap<String, String>();
 *       paras.put("--fields-terminated-by", "\\t");
 *       paras.put("--input-null-string", "\\\\N");
 *       paras.put("--input-null-non-string", "\\\\N");
 * </pre>
 */
public final class SqoopApi {

    private static final Logger log = LoggerFactory.getLogger(SqoopApi.class);

    private static final String HADOOP_OPTION_PREFIX = "-D";

    private static final SqoopConfigurable sqoopConfigurable = SqoopConfigurable.getInstance();
    /**
     * Very small sqoop job jar submitted to cluster!
     */
    private static final String sqoopJobJarPath =
            sqoopConfigurable.getString("sqoop.jobjar");

    private static final int MAX_SQOOP_JOBS = sqoopConfigurable.getInt("sqoop.job.request.max");

    private static final SqoopApi INSTANCE = new SqoopApi();

    private final SqoopClient sqoopClient;
    private final AtomicInteger sqoopJobCounter = new AtomicInteger(0);

    // DC submitted job status map
    private final ConcurrentMap<String, JobExecutionResult> jobStatusMap =
            new ConcurrentHashMap<String, JobExecutionResult>();

    // Mark as private to prevent from new instance with Singleton design
    private SqoopApi() {
        sqoopClient = new SqoopClient();
    }

    public static final SqoopApi getSqoopApi() {
        if (INSTANCE == null) {
            throw new RuntimeException("Can't create singleton for SqoopApi!");
        }
        return INSTANCE;
    }

    public void killHadoopJobs() {
        sqoopClient.killHadoopJobs();
    }

    /**
     * Count current submitted sqoop jobs
     */
    public final int countRequests() {
        return sqoopJobCounter.get();
    }

    public final ConcurrentMap<String, JobExecutionResult> getJobStatusMap() {
        return jobStatusMap;
    }

    /**
     * Execute sqoop command with <code>jobId</code> and <code>sqoopCommandArgs</> provided,
     * this is the only main entry to trigger sqoop job in programming with execution result
     * returned.
     * <p>
     * This API is thread-safe.
     */
    public final JobExecutionResult execute(String jobId, String[] sqoopCommandArgs)
            throws MaxHadoopJobRequestsException {
        // Check that the job had been submitted.
        JobExecutionResult oldResult = jobStatusMap.get(jobId);
        if (oldResult != null) {
            return handleRepeatJobSubmissions(jobId, oldResult);
        } else {
            int currentJobRunnings = sqoopJobCounter.get();
            log.info("sqoop并发任务计数器数量：" + currentJobRunnings);
            if (currentJobRunnings < MAX_SQOOP_JOBS) {
                JobExecutionResult initialJobStatus = new JobExecutionResult(jobId);
                JobExecutionResult newResultInMap = jobStatusMap.putIfAbsent(jobId, initialJobStatus);
                if (newResultInMap == null) {
                    try {
                        sqoopJobCounter.getAndIncrement();
                        log.info(String.format("Start to submit sqoop job of '%s' to hadoop!", jobId));
                        // sqoopApiArgs consists of "jobId, [sqoop command]"
                        String[] sqoopApiArgs = new String[sqoopCommandArgs.length + 1];
                        sqoopApiArgs[0] = jobId;
                        System.arraycopy(sqoopCommandArgs, 0, sqoopApiArgs, 1, sqoopCommandArgs.length);
                        /**
                         * For now, this is the blocking call, we will need to provide with
                         * the asyn. version as well in future.
                         */
                        submitMrJobjar(sqoopJobJarPath, SqoopApi.class, sqoopApiArgs);
                        JobExecutionResult executionResult = jobStatusMap.remove(jobId);
                        log.warn(String.format(
                                "Completed with running sqoop job of '%s' with execution result: %s",
                                jobId, (executionResult.isSucceed() ? "OK" : "Failed")));
                        return executionResult;
                    } finally {
                        sqoopJobCounter.getAndDecrement();
                    }
                } else {
                    // collision of new job submissions.
                    return handleRepeatJobSubmissions(jobId, newResultInMap);
                }
            } else {
                String maxErrorMsg = String.format(
                        "Exceed the upper limit of jobs of %d, currently having %d jobs running ",
                        MAX_SQOOP_JOBS, currentJobRunnings);
                log.error(maxErrorMsg);
                throw new MaxHadoopJobRequestsException(maxErrorMsg);
            }
        }
    }

    private JobExecutionResult handleRepeatJobSubmissions(String jobId, JobExecutionResult oldResult) {
        String message = String.format(
                "Job of '%s' has already been submitted with execution result %s",
                jobId, oldResult.getJobStatus().toString());
        log.error(message);
        JobExecutionResult repeatError = new JobExecutionResult(jobId);
        repeatError.setErrorMessage(message);
        repeatError.setJobStatus(JobStatus.Repeat);
        return repeatError;
    }

    /**
     * Execute sqoop command with job id, command, (key, value) pair and other options including
     * hadoop configurations.
     */
    public final JobExecutionResult execute(String jobId, String command,
                                            Map<String, String> paras,
                                            List<String> options)
            throws MaxHadoopJobRequestsException {

        return execute(jobId, command, Collections.EMPTY_MAP, paras, options);
    }

    public final JobExecutionResult execute(String jobId, String command,
                                            Map<String, String> paras,
                                            List<String> options, Map<String, String> sqoopProperties)
            throws MaxHadoopJobRequestsException {

        return execute(jobId, command, sqoopProperties, paras, options);
    }

    public final JobExecutionResult execute(String jobId, String command,
                                            Map<String, String> hadoopOptions,
                                            Map<String, String> paras,
                                            List<String> options)
            throws MaxHadoopJobRequestsException {
        List<String> args = new ArrayList<String>();
        args.add(command);

        /**
         * Firstly we need to append hadoop option with -D prefix. E.g.
         * mapred.task.timeout=600000
         * mapred.map.max.attempts=1
         * mapred.reduce.max.attempts=1
         */
        Set<String> hadoopKeys = hadoopOptions.keySet();
        for (String hadoopKey : hadoopKeys) {
            args.add(HADOOP_OPTION_PREFIX);
            args.add(hadoopKey + "=" + hadoopOptions.get(hadoopKey));
        }

        // Append sqoop's own key-value pairs
        Set<String> keys = paras.keySet();
        for (String key : keys) {
            args.add(key);
            args.add(paras.get(key));
        }

        for (String option : options) {
            args.add(option);
        }

        String[] sqoopCommandArgs = new String[args.size()];
        sqoopCommandArgs = args.toArray(sqoopCommandArgs);
        return execute(jobId, sqoopCommandArgs);
    }

    public static String encryptionPassWord(String message) throws Exception {
        try {
            if (message.contains("--password") && (message.contains("--query") || message.contains("--connect"))) {
                // int pwdInd = message.contains("--query") ? message.indexOf("--query") : message.indexOf("--connect");
                int pwdIndex = message.indexOf("--", message.indexOf("--password") + "--password".length());
                String tmp = message.substring(message.indexOf("--password") + "--password".length(), pwdIndex);
                message = message.replace(tmp, " ****** ");
            }
        } catch (Exception e) {
            log.warn(String.format("encryptionPassWord error '%s'", e.getMessage()));
        }
        return message;
    }

    private final void doLaunchSqoopJob(String[] sqoopApiArgs) {
        int launchResultCode = -1;
        /**
         * First arg should be jobId, take a look at
         * {@link #execute(String jobId, String[] sqoopCommandArgs)}
         */
        String jobId = sqoopApiArgs[0];
        JobExecutionResult result = jobStatusMap.get(jobId);
        if (result == null) {
            result = new JobExecutionResult(jobId);
        }
        String[] sqoopCommandArgs = new String[sqoopApiArgs.length - 1];
        try {
            System.arraycopy(sqoopApiArgs, 1, sqoopCommandArgs, 0, sqoopCommandArgs.length);
            String sqoopNotPwdStr = arrayToString(sqoopCommandArgs, " ");

            log.info(String.format("Sqoop job of '%s' with args: %s",
                    jobId, encryptionPassWord(sqoopNotPwdStr)));

            launchResultCode = sqoopClient.launchSqoopJob(sqoopCommandArgs);
        } catch (Exception e) {
            /**
             * With sqoop property Sqoop.SQOOP_RETHROW_PROPERTY enabled, we can catch all sqoop exceptions.
             * System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "1");
             */
            log.error(String.format("Error during launching sqoop job of '%s' via RunJar", jobId));
            log.error(String.format("Job of '%s' exception:%s", jobId, e.getClass()));
            log.error(String.format("Job of '%s' exception message:%s", jobId, e.getMessage()));
            result.setException(e);
            result.setErrorMessage(e.getMessage());
        } finally {
            // store the execution result to report back to DC client
            result.setJobStatus(
                    launchResultCode == 0 ? JobStatus.Succeed : JobStatus.Failed);
            jobStatusMap.put(jobId, result);
        }
    }

    /**
     * This is to submit MR job to Yarn cluster.
     */
    private void submitMrJobjar(String jobJar, Class mrJobMainClass, String[] sqoopApiArgs) {
        File file = new File(jobJar);
        if (file.exists() && file.isFile()) {
            String[] runJarArgs = {jobJar, mrJobMainClass.getName()};
            String[] allArgs = new String[runJarArgs.length + sqoopApiArgs.length];
            System.arraycopy(runJarArgs, 0, allArgs, 0, runJarArgs.length);
            System.arraycopy(sqoopApiArgs, 0, allArgs, runJarArgs.length, sqoopApiArgs.length);
            try {
                RunJar.main(allArgs);
            } catch (Throwable t) {
                log.error("Error in invoking RunJar.main", t);
            }
        } else {
            String noJobJarError = String.format(
                    "Couldn't find job jar of '%s', please put the sqoop job jar into this path: [%s]",
                    jobJar, jobJar);
            log.error(noJobJarError);
            throw new IllegalArgumentException(noJobJarError);
        }
    }

    public static String arrayToString(String[] arrayList, String splitChar) {
        StringBuffer sbBuffer = new StringBuffer();
        for (String str : arrayList) {
            sbBuffer.append(str);
            sbBuffer.append(splitChar);
        }
        if (sbBuffer.length() > 0) {
            sbBuffer.delete(sbBuffer.length() - 1, sbBuffer.length());
        }
        return sbBuffer.toString();
    }

    public static void main(String[] sqoopArgs) {
        SqoopApi sqoopApi = SqoopApi.getSqoopApi();
        sqoopApi.doLaunchSqoopJob(sqoopArgs);
    }
}
