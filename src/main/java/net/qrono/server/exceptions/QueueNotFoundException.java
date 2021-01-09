package net.qrono.server.exceptions;

/**
 * Thrown when a queue must already exist, but is not found.
 */
public class QueueNotFoundException extends QronoException {
  public QueueNotFoundException() {
    super("queue not found");
  }

  public QueueNotFoundException(String message) {
    super(message);
  }

  public QueueNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public QueueNotFoundException(Throwable cause) {
    super(cause);
  }
}
