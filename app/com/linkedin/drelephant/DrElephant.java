package com.linkedin.drelephant;

import java.io.IOException;


public class DrElephant extends Thread {
  private ElephantRunner elephant;

  public DrElephant() throws IOException {
    elephant = new ElephantRunner();
  }

  @Override
  public void run() {
    elephant.run();
  }

  public void kill() {
    if (elephant != null) {
      elephant.kill();
    }
  }
}
