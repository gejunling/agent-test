package com.dp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ConfigProperty {

  private static Properties properties = new Properties();

  public static final String SOURCE_EXEC_DIR = "source.exec.dir";

  static {
    try {
      BufferedReader bufferedReader = new BufferedReader(new FileReader("./conf/test.properties"));
      properties.load(bufferedReader);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String get(String key) {
    return properties.getProperty(key);
  }
}
