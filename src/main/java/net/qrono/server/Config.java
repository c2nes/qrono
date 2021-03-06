package net.qrono.server;

import static com.google.common.base.Strings.emptyToNull;

import com.google.common.net.HostAndPort;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import net.qrono.server.util.DataSize;
import org.immutables.value.Value;

@Value.Immutable
@SuppressWarnings("UnstableApiUsage")
public interface Config {
  @Option("qrono.net.resp.listen")
  HostAndPort netRespListen();

  @Option("qrono.net.http.listen")
  HostAndPort netHttpListen();

  @Option("qrono.net.http.gatewayPath")
  Optional<Path> netHttpGatewayPath();

  @Option("qrono.net.grpc.listen")
  HostAndPort netGrpcListen();

  @Option("qrono.net.metrics.listen")
  HostAndPort netMetricsListen();

  @Option("qrono.data.root")
  Path dataRoot();

  @Option("qrono.data.queues.dir")
  Path dataQueuesDir();

  @Option("qrono.data.workingSet.dir")
  Path dataWorkingSetDir();

  @Option("qrono.data.workingSet.mappedFileSize")
  DataSize dataWorkingSetMappedFileSize();

  @Option("qrono.segmentFlushThreshold")
  DataSize segmentFlushThreshold();

  static Config load() throws IOException {
    var classLoader = Config.class.getClassLoader();
    var properties = loadProperties(classLoader);

    InvocationHandler handler = (proxy, method, args) -> {
      var option = method.getAnnotation(Option.class);
      if (option != null) {
        var valueName = option.value();
        var valueStr = readStringOption(properties, valueName);
        var type = method.getReturnType();

        if (Optional.class.equals(type)) {
          if (valueStr == null) {
            return Optional.empty();
          }

          var optionalType = (Class<?>) ((ParameterizedType) method.getGenericReturnType())
              .getActualTypeArguments()[0];

          return Optional.of(parseOption(optionalType, valueStr));
        }

        return valueStr == null ? null : parseOption(type, valueStr);
      }

      throw new UnsupportedOperationException();
    };

    // Use a temporary proxy to initialize the ImmutableConfig
    return ImmutableConfig.copyOf(
        (Config) Proxy.newProxyInstance(classLoader, new Class<?>[]{Config.class}, handler));
  }

  private static Properties loadProperties(ClassLoader classLoader) throws IOException {
    var defaults = new Properties();
    try (var in = classLoader.getResourceAsStream("qrono-defaults.properties")) {
      if (in != null) {
        defaults.load(in);
      }
    }

    var properties = new Properties(defaults);
    try (var in = classLoader.getResourceAsStream("qrono.properties")) {
      if (in != null) {
        properties.load(in);
      }
    }

    return properties;
  }

  private static String readStringOption(Properties properties, String name) {
    var sysPropValue = emptyToNull(System.getProperty(name));
    if (sysPropValue != null) {
      return sysPropValue;
    }

    var envVarName = name
        .replaceAll("([a-z])([A-Z])", "$1_$2")
        .replace('.', '_')
        .toUpperCase();

    var value = emptyToNull(System.getenv(envVarName));
    if (value != null) {
      return value;
    }

    return emptyToNull(properties.getProperty(name));
  }

  private static Object parseOption(Class<?> type, String value) {
    if (type.isAssignableFrom(HostAndPort.class)) {
      return HostAndPort.fromString(value);
    }

    if (type.isAssignableFrom(Path.class)) {
      return Path.of(value);
    }

    if (type.isAssignableFrom(DataSize.class)) {
      return DataSize.fromString(value);
    }

    throw new IllegalArgumentException("unsupported type: " + type);
  }

  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Option {
    String value();
  }
}
