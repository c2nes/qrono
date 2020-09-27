package net.qrono.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PersistentIdGeneratorTest {
  @Rule
  public TemporaryFolder dir = new TemporaryFolder();

  @Test
  public void testGenerate() throws IOException {
    var statePath = dir.newFolder().toPath().resolve("state");
    var generator = new PersistentIdGenerator(statePath, 1_000, 500);
    generator.startAsync().awaitRunning();
    try {
      var id1 = generator.generateId();
      var id2 = generator.generateId();
      assertThat(id1).isGreaterThan(0);
      assertThat(id2).isGreaterThan(id1);
    } finally {
      generator.stopAsync().awaitTerminated();
    }
  }

  @Test
  public void testRenewal() throws IOException {
    var statePath = dir.newFolder().toPath().resolve("state");
    var generator = new PersistentIdGenerator(statePath, 1_000, 500);
    generator.startAsync().awaitRunning();
    try {
      var last = 0L;
      for (int i = 0; i < 100_000; i++) {
        var id = generator.generateId();
        assertThat(id).isGreaterThan(last);
        last = id;
      }
    } finally {
      generator.stopAsync().awaitTerminated();
    }
  }

  @Test
  public void testRenewalPersisted() throws IOException {
    var statePath = dir.newFolder().toPath().resolve("state");
    var last = 0L;

    // Generate 100,000 IDs and then tear down the generator.
    var generator = new PersistentIdGenerator(statePath, 1_000, 500);
    generator.startAsync().awaitRunning();
    try {
      for (int i = 0; i < 100_000; i++) {
        last = generator.generateId();
      }
    } finally {
      generator.stopAsync().awaitTerminated();
    }

    // Restart the generator and ensure the next ID is greater than the last ID returned
    // before tearing down the generator.
    generator = new PersistentIdGenerator(statePath, 1_000, 500);
    generator.startAsync().awaitRunning();
    try {
      var id = generator.generateId();
      assertThat(id).isGreaterThan(last);
    } finally {
      generator.stopAsync().awaitTerminated();
    }
  }
}