package indi.xeno.styx.erebos.app;

public abstract class CommandApp {

  public static void run(CommandApp app, String[] args) {
    CommandOptions opts = new CommandOptions();
    app.initCommand(opts);
    app.run(opts.parse(args));
  }

  protected abstract void initCommand(CommandOptions opts);

  protected abstract void run(CommandArguments args);
}
