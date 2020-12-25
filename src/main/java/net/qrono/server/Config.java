package net.qrono.server;

import static com.google.common.base.Strings.emptyToNull;

import com.google.common.net.HostAndPort;
import com.google.common.reflect.TypeToken;
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
  @Option("net.resp.listen")
  HostAndPort netRespListen();

  @Option("net.http.listen")
  HostAndPort netHttpListen();

  @Option("net.http.gatewayPath")
  Optional<Path> netHttpGatewayPath();

  @Option("net.grpc.listen")
  HostAndPort netGrpcListen();

  @Option("data.root")
  Path dataRoot();

  @Option("data.queues.dir")
  Path dataQueuesDir();

  @Option("data.workingSet.dir")
  Path dataWorkingSetDir();

  @Option("data.workingSet.mappedFileSize")
  DataSize dataWorkingSetMappedFileSize();

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
