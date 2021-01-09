package net.qrono.server.exceptions;

/**
 * Generic Qrono related exception.
 */
public class QronoException extends RuntimeException {
  public QronoException() {
  }

  public QronoException(String message) {
    super(message);
  }

  public QronoException(String message, Throwable cause) {
    super(message, cause);
  }

  public QronoException(Throwable cause) {
    super(cause);
  }
}
