package cn.wonhigh.dc.client.sqoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.sqoop.Sqoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;

/**
 * All rights reserved.
 *
 * @author Qiuzhuang.Lian
 *
 * Sqoop MR client, this class is just for internal usage, thus marked as package scope
 */
class SqoopClient {
  private static final Logger log = LoggerFactory.getLogger(SqoopClient.class);

  private final Configuration configuration = new Configuration();

  protected SqoopClient() {
    /* lets test one of hadoop conf files to see if they are there anyway! */
    log.warn("Please do make sure that all hadoop configuration files in the classpath.");
    URL url = SqoopClient.class.getClassLoader().getResource("core-site.xml");
    if (url != null) {
      log.warn(String.format("Hadoop core-site.xml in location: %s", url.toString()));
    } else {
      throw new IllegalArgumentException("Hadoop configuration files are not in classpath, " +
          "you could export CLASSPATH_PREFIX=xxx so that application can pick it up and add into classpath.");

    }
    configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    configuration.set("mapreduce.framework.name", "yarn");

    configuration.addResource("core-site.xml");
    configuration.addResource("hdfs-site.xml");
    configuration.addResource("yarn-site.xml");
    configuration.addResource("yarn-site.xml");
  }

  public void killHadoopJobs() {
    int numRetries = 3;
    while ((numRetries--) > 0) {
      Configuration yarnConf = new YarnConfiguration(configuration);
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(yarnConf);
      yarnClient.start();
      try {
        List<ApplicationReport> applicationReports = yarnClient.getApplications();
        int size = applicationReports.size();
        if (size > 0) {
          log.info(String.format("Killing %d running hadoop jobs...", size));
        }
        for (ApplicationReport report : applicationReports) {
          yarnClient.killApplication(report.getApplicationId());
        }
        log.info(String.format("Done with killing %d running hadoop jobs!", size));
        return;
      } catch (Exception e) {
        log.error("Can't kill hadoop jobs.", e);
      } finally {
        yarnClient.stop();
      }
    }
  }

  public int launchSqoopJob(String[] commandArgs) {
    log.info("Start to run sqoop hadoop job.");
    System.setProperty(Sqoop.SQOOP_RETHROW_PROPERTY, "1");
    int resultCode = Sqoop.runTool(commandArgs, configuration);
    if (resultCode == 0) {
      log.info("Sqoop job runs to completion without error.");
    } else {
      log.error("Sqoop job execution failed, please check log.");
    }
    return resultCode;
  }
}
