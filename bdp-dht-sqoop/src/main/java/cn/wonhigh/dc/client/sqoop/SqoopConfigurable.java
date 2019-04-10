package cn.wonhigh.dc.client.sqoop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * All rights reserved.
 *
 * @author Qiuzhuang.Lian
 *
 * SqoopApi configrations
 */
public class SqoopConfigurable {
  private static final Logger log = LoggerFactory.getLogger(SqoopConfigurable.class);

  /**
   * Use lazy thread-safe singleton pattern
   * https://en.wikipedia.org/wiki/Singleton_pattern
   */
  private static class SqoopConfigurableHolder {
    private static SqoopConfigurable HOLDER = new SqoopConfigurable();
  }

  public static final String CONFIGURATION_PATH =
      "/etc/wonhighconf/dc/client/sqoopapi.properties";

  private final Properties props = new Properties();

  private SqoopConfigurable() {
    InputStream in;
    try {
      in = new FileInputStream(new File(CONFIGURATION_PATH));
      log.warn(String.format("Use SqoopApi configuration file of '%s'", CONFIGURATION_PATH));
    } catch (FileNotFoundException fileNotFoundException) {
      in = SqoopConfigurable.class.getClassLoader().
          getResourceAsStream("sqoopapi.properties");
      log.warn("Use SqoopApi default configuration file in jar!");
    }

    try {
      if (in == null) {
        throw new Exception("Can't find SqoopApi configuration file.");
      }
      InputStreamReader utfInputStreamReader = new InputStreamReader(in, "UTF-8");
      props.load(utfInputStreamReader);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
      }
    }
  }

  public static SqoopConfigurable getInstance() {
    return SqoopConfigurableHolder.HOLDER;
  }

  public String getString(String key) {
    return props.getProperty(key);
  }

  public int getInt(String key) {
    String value = getString(key);
    return Integer.parseInt(value);
  }

  public boolean getBoolean(String key) {
    String value = getString(key);
    return Boolean.parseBoolean(value);
  }

  public boolean getBoolean(String key, boolean defaultVal) {
    String value = getString(key);
    if (value != null && value.length() > 0) {
      return Boolean.parseBoolean(value);
    } else {
      return defaultVal;
    }
  }

  public static void main(String[] args) {
    SqoopConfigurable.getInstance().getString("sqoop.jobjar");
  }
}
