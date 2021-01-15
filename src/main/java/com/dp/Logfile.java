package com.dp;

public class Logfile {
  private String thread;
  private String sequence;
  private String name;

  public String getThread() {
    return thread;
  }

  public void setThread(String thread) {
    this.thread = thread;
  }

  public String getSequence() {
    return sequence;
  }

  public void setSequence(String sequence) {
    this.sequence = sequence;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return "Logfile{"
        + "thread='"
        + thread
        + '\''
        + ", sequence='"
        + sequence
        + '\''
        + ", name='"
        + name
        + '\''
        + '}';
  }
}
