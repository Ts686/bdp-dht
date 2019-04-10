package cn.wonhigh.dc.client.sqoop;

import java.io.Serializable;

/**
 * All rights reserved.
 *
 * @author Qiuzhuang.Lian
 */
public class JobExecutionResult implements Serializable {
  private final String submitJobId;
  private JobStatus jobStatus;
  private String errorMessage;
  private Throwable exception;

  public JobExecutionResult(final String submitJobId) {
    this.submitJobId = submitJobId;
    this.jobStatus = JobStatus.Submit;
  }

  public String getSubmitJobId() {
    return submitJobId;
  }

  public JobStatus getJobStatus() {
    return jobStatus;
  }

  public void setJobStatus(JobStatus jobStatus) {
    this.jobStatus = jobStatus;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public Throwable getException() {
    return exception;
  }

  public void setException(Throwable exception) {
    this.exception = exception;
  }

  public boolean isSucceed() {
    return jobStatus == JobStatus.Succeed;
  }
}
