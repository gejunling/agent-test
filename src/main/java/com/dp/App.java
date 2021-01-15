package com.dp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.net.www.http.HttpClient;

public class App {
  private static Logger logger = LogManager.getLogger(App.class.getName());

  /*
  0、源备端停止同步，并清理一下
  1. 查询当前最大的sequence，记为 start sequence
  2。执行加数据的脚本
  3。再次查询当前最大的sequence, 记为end sequence
  4。执行切换日志
  5。把start sequence 到 end sequence，对应的日志文件，copy到指定目录
  6. 修改源端run/save/1/cfg.loginfo,
  7、启动源端同步
   */
  public static void main(String[] args) throws Exception {
    System.out.println("--------------> agent ci start <--------------");

    Properties properties = new Properties();
    BufferedReader bufferedReader =
        new BufferedReader(
            new FileReader(
                ConfigProperty.get(ConfigProperty.SOURCE_EXEC_DIR) + "/conf/send1.conf"));
    properties.load(bufferedReader);
    String tgtIp = properties.getProperty("tgt_ip");
    String tgtWebPort = properties.getProperty("tgt_web_port");

    System.out.println("tgt_id: " + tgtIp);
    System.out.println("tgt_web_port: " + tgtWebPort);

    stopAndCleanSource();

    stopAndCleanSink(tgtIp, tgtWebPort);

    OracleConnect oracleConnect = new OracleConnect();
    long startSequence = oracleConnect.getMaxSequence();
    System.out.println("--> get max sequence as start sequence: " + startSequence);

    System.out.println("--> exec add data sql");
    oracleConnect.addSomeData();

    long endSequence = oracleConnect.getMaxSequence();
    System.out.println("--> get max sequence as end sequence: " + endSequence);

    System.out.println("--> alter system switch logfile");
    oracleConnect.switchLogfile();
    oracleConnect.switchLogfile();

    System.out.println("--> copy logfile");
    copyLogfile(oracleConnect, startSequence, endSequence);

    System.out.println("--> modify save/1/cfg.loginfo");
    createCfgLoginfo(startSequence);

    System.out.println("-->  start source");

    startSourceAndSink(tgtIp, tgtWebPort);

    // System.out.println("-->  start sink");

    System.out.println("--------------> agent ci end <--------------");
  }

  private static void startSourceAndSink(String tgtIp, String tgtPort) throws Exception {
    // start source
    Runtime runtime = Runtime.getRuntime();
    String fzsStart = ConfigProperty.get(ConfigProperty.SOURCE_EXEC_DIR) + "/run/fzsstart";
    Process p = runtime.exec(fzsStart, null, new File("/home/fzsge/run"));
    p.waitFor();
    System.out.println("--> " + fzsStart);

    // start sink
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet httpGet = new HttpGet(String.format("http://%s:%s/fzsstart", tgtIp, tgtPort));
      try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
        if (response.getStatusLine().getStatusCode() == 200) {
          HttpEntity entity = response.getEntity();
          String string = EntityUtils.toString(entity, "utf-8");
          System.out.println(string);
        }
      }
    }
  }

  private static void createCfgLoginfo(long startSequence) throws IOException {

    String dir = ConfigProperty.get(ConfigProperty.SOURCE_EXEC_DIR);
    File saveDir = new File(dir + "/run/save/1");
    if (!saveDir.exists()) {
      saveDir.mkdirs();
    }

    try (FileWriter fileWriter = new FileWriter(saveDir.getAbsoluteFile() + "/cfg.loginfo")) {
      fileWriter.write("#TH# SEQ# BLK# BLK-OFF#");
      fileWriter.write(System.getProperty("line.separator"));
      fileWriter.write(String.format("1 %s 0 0", startSequence));
      fileWriter.write(System.getProperty("line.separator"));
    }
  }

  private static void copyLogfile(OracleConnect oracleConnect, long startSequence, long endSequence)
      throws Exception {
    String sql =
        String.format(
            "select THREAD#, SEQUENCE#, NAME from V$ARCHIVED_LOG where NAME is not null and SEQUENCE# >= %s and SEQUENCE# <= %s ",
            startSequence, endSequence);
    List<Logfile> logfiles = new ArrayList<>();
    try (Connection connection = oracleConnect.getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet rs = statement.executeQuery(sql)) {
        while (rs.next()) {
          long sequence = rs.getLong("SEQUENCE#");
          Logfile logfile = new Logfile();
          logfile.setThread(rs.getString("THREAD#"));
          logfile.setSequence(rs.getString("SEQUENCE#"));
          logfile.setName(rs.getString("NAME"));
          logfiles.add(logfile);
        }
      }
    }

    File archDir = new File(ConfigProperty.get(ConfigProperty.SOURCE_EXEC_DIR) + "/run/arch");
    if (!archDir.exists()) {
      archDir.mkdir();
    }

    for (Logfile logfile : logfiles) {
      System.out.println(logfile);
      Path source = Paths.get(logfile.getName());
      Path dest =
          Paths.get(
              archDir.getAbsolutePath()
                  + "/"
                  + String.format("arc_%s_%s", logfile.getThread(), logfile.getSequence()));
      System.out.println("copy " + source.getRoot() + " to " + dest.getRoot());
      Files.copy(source, dest);
    }
    // System.out.println(logfiles.size());
    // System.out.println(startSequence);
    // System.out.println(endSequence);
  }

  private static void stopAndCleanSource() throws Exception {
    Runtime runtime = Runtime.getRuntime();
    String fzsStop = ConfigProperty.get(ConfigProperty.SOURCE_EXEC_DIR) + "/run/fzsterm";
    Process p = runtime.exec(fzsStop, null, new File("/home/fzsge/run"));
    p.waitFor();
    System.out.println("--> " + fzsStop);

    TimeUnit.SECONDS.sleep(1);

    String fzsClean = ConfigProperty.get(ConfigProperty.SOURCE_EXEC_DIR) + "/run/fzsclean";
    p = runtime.exec(fzsClean, null, new File("/home/fzsge/run"));

    p.waitFor();
    System.out.println("--> " + fzsClean);
  }

  private static void stopAndCleanSink(String tgtIp, String tgtPort) throws Exception {

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet httpGet = new HttpGet(String.format("http://%s:%s/fzsstop", tgtIp, tgtPort));
      try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
        if (response.getStatusLine().getStatusCode() == 200) {
          HttpEntity entity = response.getEntity();
          String string = EntityUtils.toString(entity, "utf-8");
          System.out.println(string);
        }
      }
    }
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet httpGet = new HttpGet(String.format("http://%s:%s/fzsclean", tgtIp, tgtPort));
      try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
        if (response.getStatusLine().getStatusCode() == 200) {
          HttpEntity entity = response.getEntity();
          String string = EntityUtils.toString(entity, "utf-8");
          System.out.println(string);
        }
      }
    }
  }
}
