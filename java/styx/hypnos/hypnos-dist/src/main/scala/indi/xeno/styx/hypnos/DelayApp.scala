package indi.xeno.styx.hypnos

import indi.xeno.styx.hypnos.app.{CommandArguments, CommandOptions, FlinkApp}
import indi.xeno.styx.hypnos.app.FlinkApp.run

object DelayApp {

  def main(args: Array[String]): Unit = {
    run(new DelayApp(), args)
  }
}

private class DelayApp() extends FlinkApp() {

  protected final def initCommand(opts: CommandOptions): Unit = {

  }

  protected final def run(args: CommandArguments): Unit = {

  }
}
