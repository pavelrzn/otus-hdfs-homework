package otus.home.hdfs.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.hadoop.io.IOUtils


object HdfsUtils {

  private val conf = new Configuration()
  private val hdfsCoreSitePath = new HPath("core-site.xml")
  private val hdfsHDFSSitePath = new HPath("hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)

  def writeFiles(sourceDir: String, targetDir: String, ext: String): Unit = {
    val dirsList = getDirs(sourceDir)

    dirsList.foreach(dir => {
      val files = getFilesList(dir, ext)
      val destination = targetDir + "/" + dir.getName

      createFolder(destination)

      val targetFileOption = if (files.nonEmpty) Some(new HPath(destination + "/" + files.head.getName)) else None

      targetFileOption.foreach( targetFile => {
        val outStream = fileSystem.create(targetFile)

        try {
          files.foreach(sourceFile => {
            val inStream = fileSystem.open(sourceFile)
            try {
              println(s"trying to add ${sourceFile.getName} to ${targetFile.getName}")
              IOUtils.copyBytes(inStream, outStream, conf, false)
            } finally {inStream.close()}
          })
        } finally {outStream.close()}
      }
      )
    })
  }

  private def getDirs(path: String): Array[HPath] =
    fileSystem.listStatus(new HPath(path))
      .filter(_.isDirectory).map(_.getPath)

  private def getFilesList(path: HPath, extension: String): List[HPath] = {
    fileSystem.listStatus(path)
      .filter(fs => fs.isFile && fs.getPath.getName.endsWith(extension))
      .map(_.getPath).toList
  }

  private def createFolder(fPath: String): Unit = {
    val path = new HPath(fPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

}