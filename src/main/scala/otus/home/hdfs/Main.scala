package otus.home.hdfs

import utils.HdfsUtils

object Main extends App {
  val pathSource = "/sample_data/stage"
  val pathTarget = "/sample_data/ods"

  HdfsUtils.writeFiles(pathSource, pathTarget, ".csv")
  println("done")

}

