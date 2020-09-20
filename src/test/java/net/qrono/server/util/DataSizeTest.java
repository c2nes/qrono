package net.qrono.server.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DataSizeTest {
  @Test
  public void fromString_noUnit() {
    assertEquals(DataSize.fromBytes(1234), DataSize.fromString("1234"));
  }

  @Test
  public void fromString_B() {
    assertEquals(DataSize.fromBytes(1234), DataSize.fromString("1234B"));
  }

  @Test
  public void fromString_B_withWhitespace() {
    assertEquals(DataSize.fromBytes(1234), DataSize.fromString("1234 B"));
  }

  @Test
  public void fromString_kB() {
    assertEquals(DataSize.fromBytes(1234 * 1024), DataSize.fromString("1234kB"));
  }
}