package indi.xeno.styx.charon.app;

public abstract class CommandApp {

  public static void run(CommandApp app, String[] args) {
    CommandOptions opts = app.initCommand(new CommandOptions());
    app.run(opts.parse(args));
  }

  protected abstract CommandOptions initCommand(CommandOptions opts);

  protected abstract void run(CommandArguments args);
}
