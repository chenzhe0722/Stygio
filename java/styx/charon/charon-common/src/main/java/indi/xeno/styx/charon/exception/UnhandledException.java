package indi.xeno.styx.charon.exception;

public class UnhandledException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public UnhandledException() {
    super();
  }

  public UnhandledException(String msg) {
    super(msg);
  }

  public UnhandledException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public UnhandledException(Throwable cause) {
    super(cause);
  }

  public UnhandledException(String msg, Throwable cause, boolean suppress, boolean traceable) {
    super(msg, cause, suppress, traceable);
  }
}
