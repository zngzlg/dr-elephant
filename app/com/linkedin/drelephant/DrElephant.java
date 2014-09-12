package com.linkedin.drelephant;

import java.io.IOException;


public class DrElephant extends Thread {
  private ElephantRunner _elephant;

  public DrElephant() throws IOException {
    _elephant = new ElephantRunner();
  }

  @Override
  public void run() {
    _elephant.run();
  }

  public void kill() {
    if (_elephant != null) {
      _elephant.kill();
    }
  }
}
