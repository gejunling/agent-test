package com.dp;

import com.alibaba.druid.pool.DruidDataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OracleConnect {
  private static Logger logger = LogManager.getLogger(OracleConnect.class);

  private DataSource dataSource;

  public OracleConnect() {

    try {
      Properties properties = new Properties();
      BufferedReader bufferedReader =
          new BufferedReader(
              new FileReader(
                  ConfigProperty.get(ConfigProperty.SOURCE_EXEC_DIR) + "/conf/export.conf"));
      properties.load(bufferedReader);
      String login = properties.getProperty("src_login");
      String substring1 = login.substring(0, login.indexOf("@"));
      String substring2 = login.substring(login.indexOf("@"));

      String[] userAndPwd = substring1.split("/");

      DruidDataSource dataSource = new DruidDataSource();

      String dbURL = String.format("jdbc:oracle:thin:%s", substring2);
      dataSource.setUrl(dbURL);
      dataSource.setDriverClassName("oracle.jdbc.OracleDriver"); // 这个可以缺省的，会根据url自动识别
      dataSource.setUsername("DP_CI");
      dataSource.setPassword("datapipeline123");

      dataSource.setInitialSize(10);
      dataSource.setMaxActive(30);
      dataSource.setMinIdle(10);
      dataSource.setMaxWait(2000);
      this.dataSource = dataSource;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  public long getMaxSequence() throws SQLException {

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet rs = statement.executeQuery("select max(SEQUENCE#) from v$log")) {
        if (rs.next()) {
          return rs.getLong(1);
        }
        throw new RuntimeException("max sequence is null, stop test");
      }
    }
  }

  public void addSomeData() throws IOException {

    File file = new File("data.sql");

    String script_location = "@" + file.getAbsolutePath(); // ORACLE
    ProcessBuilder processBuilder =
        new ProcessBuilder(
            "sqlplus", "DP_CI/datapipeline123@ora-app:1521/ORCL", script_location); // ORACLE
    processBuilder.redirectErrorStream(true);
    Process process = processBuilder.start();
    BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String currentLine = null;
    while ((currentLine = in.readLine()) != null) {
      System.out.println(" " + currentLine);
      if (currentLine.contains("successfully")) {
        break;
      }
    }

    System.out.println();
  }

  public void switchLogfile() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("alter system switch logfile");
    }
  }

  public static void main(String[] args) {
    String s = "fzsge/fzs1@ora-app:1521/ORCL";
    String substring1 = s.substring(0, s.indexOf("@"));
    String substring2 = s.substring(s.indexOf("@"));

    String[] userAndPwd = substring1.split("/");

    //    String dbURL = "jdbc:oracle:thin:@47.94.83.111:1521:orcl";

    String.format("jdbc:oracle:thin:%s", substring2);
  }
}
