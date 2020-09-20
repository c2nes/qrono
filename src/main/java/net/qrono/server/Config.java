package net.qrono.server;

import com.google.common.net.HostAndPort;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

@SuppressWarnings("UnstableApiUsage")
public class Config {
  HostAndPort netListenResp = HostAndPort.fromString("[::]:16379");
  HostAndPort netListenHttp = HostAndPort.fromString("[::]:16380");
  HostAndPort netListenGrpc = HostAndPort.fromString("[::]:16381");

  Path dataRoot = Path.of("/var/lib/qrono");
  Path dataQueuesDir = Path.of("queues");
  Path dataWorkingSetDir = Path.of("working");
  int dataWorkingSetMappedFileSize = 1 << 30;

  void load(String resourceName) throws IOException {
    Properties props = new Properties();
    var classLoader = Config.class.getClassLoader();
    try (var in = classLoader.getResourceAsStream(resourceName)) {
      if (in != null) {
        props.load(in);
      }
    }

    String p;

    p = props.getProperty("net.listen.resp");
    if (p != null) {
      netListenResp = HostAndPort.fromString(p);
    }

    p = props.getProperty("net.listen.http");
    if (p != null) {
      netListenHttp = HostAndPort.fromString(p);
    }

    p = props.getProperty("net.listen.grpc");
    if (p != null) {
      netListenGrpc = HostAndPort.fromString(p);
    }

    p = props.getProperty("data.root");
    if (p != null) {
      dataRoot = Path.of(p);
    }

    p = props.getProperty("data.queues.dir");
    if (p != null) {
      dataQueuesDir = Path.of(p);
    }

    p = props.getProperty("data.workingSet.dir");
    if (p != null) {
      dataWorkingSetDir = Path.of(p);
    }

    p = props.getProperty("data.workingSet.mappedFileSize");
    if (p != null) {
      dataWorkingSetMappedFileSize = Integer.parseInt(p);
    }
  }
}
