package org.livy.example

import java.io.{File, FileNotFoundException}
import java.net.URI

import org.apache.livy.LivyClientBuilder
import org.apache.livy.scalaapi.LivyScalaClient
import org.apache.livy.scalaapi._
import org.apache.spark.storage.StorageLevel

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object WordCountApp {
  var scalaClient: LivyScalaClient = _

  def init(url: String): Unit = {
    scalaClient = new LivyClientBuilder(false).setURI(new URI(url)).build().asScalaClient
  }

  @throws(classOf[FileNotFoundException])
  def uploadRelevantJarsForJobExecution(): Unit = {
    val exampleAppJarPath = getSourcePath(this)
    val scalaApiJarPath = getSourcePath(scalaClient)
    uploadJar(exampleAppJarPath)
    uploadJar(scalaApiJarPath)
  }

  @throws(classOf[FileNotFoundException])
  private def getSourcePath(obj: Object): String = {
    val source = obj.getClass.getProtectionDomain.getCodeSource
    if (source != null && source.getLocation.getPath != "") {
      source.getLocation.getPath
    } else {
      throw new FileNotFoundException(s"Jar containing ${obj.getClass.getName} not found.")
    }
  }

  private def uploadJar(path: String): Unit = {
    val file = new File(path)
    val uploadJarFuture = scalaClient.uploadJar(file)
    Await.result(uploadJarFuture, 40 second) match {
      case null => println("Successfully uploaded " + file.getName)
    }
  }


  def processStreamingWordCount(
                                 host: String,
                                 port: Int,
                                 outputPath: String): ScalaJobHandle[Unit] = {
    scalaClient.submit { context =>
      context.createStreamingContext(15000)
      val ssc = context.streamingctx
      val sqlctx = context.sqlctx
      val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)
      val words = lines.filter(filterEmptyContent(_)).flatMap(tokenize(_))
      words.print()
      words.foreachRDD { rdd =>
        import sqlctx.implicits._
        val df = rdd.toDF("word")
        df.write.mode("append").json(outputPath)
      }
      ssc.start()
      ssc.awaitTerminationOrTimeout(12000)
      ssc.stop(false, true)
    }
  }

  def getWordWithMostCount(inputPath: String): ScalaJobHandle[String] = {
    scalaClient.submit { context =>
      val sqlctx = context.sqlctx
      val rdd = sqlctx.read.json(inputPath)
      rdd.registerTempTable("words")
      val result = sqlctx.sql("select word, count(word) as word_count from words " +
        "group by word order by word_count desc limit 1")
      result.first().toString()
    }
  }

  private def filterEmptyContent(text: String): Boolean = {
    text != null && !text.isEmpty
  }

  private def tokenize(text: String): Array[String] = {
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }

  private def stopClient(): Unit = {
    if (scalaClient != null) {
      scalaClient.stop(true)
      scalaClient = null;
    }
  }


  def main(args: Array[String]): Unit = {
    var socketStreamHost: String = "localhost"
    var socketStreamPort: Int = 8086
    var url = ""
    var outputFilePath = ""

    def parseOptionalArg(arg: String): Unit = {
      val Array(argKey, argValue) = arg.split("=")
      argKey match {
        case "host" => socketStreamHost = argValue
        case "port" => socketStreamPort = argValue.toInt
        case _ => throw new IllegalArgumentException("Invalid key for optional arguments")
      }
    }

    require(args.length >= 2 && args.length <= 4)
    url = args(0)
    outputFilePath = args(1)
    args.slice(2, args.length).foreach(parseOptionalArg)
    
    try {
      init(url)
      uploadRelevantJarsForJobExecution()
      println("Calling processStreamingWordCount")
      val handle1 = processStreamingWordCount(socketStreamHost, socketStreamPort, outputFilePath)
      Await.result(handle1, 100 second)
      println("Calling getWordWithMostCount")
      val handle = getWordWithMostCount(outputFilePath)
      println("Word with max count::" + Await.result(handle, 100 second))
    } finally {
      stopClient()
    }
  }
}
