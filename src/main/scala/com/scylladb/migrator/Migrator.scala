package com.scylladb.migrator

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.datastax.spark.connector.rdd.partitioner.{
  CassandraPartition,
  CqlTokenRange
}
import com.datastax.spark.connector.rdd.partitioner.dht.Token
import com.datastax.spark.connector.writer._
import com.scylladb.migrator.config._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import sun.misc.{Signal, SignalHandler}

import scala.util.control.NonFatal

object Migrator {
  val log = LogManager.getLogger("com.scylladb.migrator")

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("scylla-migrator")
      .config("spark.task.maxFailures", "1024")
      .config("spark.stage.maxConsecutiveAttempts", "60")
      .getOrCreate
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(5))

    Logger.getRootLogger.setLevel(Level.WARN)
    log.setLevel(Level.INFO)
    Logger
      .getLogger("org.apache.spark.scheduler.TaskSetManager")
      .setLevel(Level.WARN)
    Logger
      .getLogger("com.datastax.spark.connector.cql.CassandraConnector")
      .setLevel(Level.WARN)

    val migratorConfig =
      MigratorConfig.loadFrom(spark.conf.get("spark.scylla.config"))

    log.info(s"Loaded config: ${migratorConfig}")

    val scheduler = new ScheduledThreadPoolExecutor(1)

    val sourceDF =
      migratorConfig.source match {
        case cassandraSource: SourceSettings.Cassandra =>
          readers.Cassandra.readDataframe(spark,
                                          cassandraSource,
                                          cassandraSource.preserveTimestamps)
      }

    log.info("Created source dataframe; resulting schema:")
    sourceDF.dataFrame.printSchema()

    log.info(
      "We need to transfer: " + sourceDF.dataFrame.rdd.getNumPartitions + " partitions in total")

    if (migratorConfig.source.isInstanceOf[SourceSettings.Cassandra]) {
      val partitions = sourceDF.dataFrame.rdd.partitions
      val cassandraPartitions = partitions.map(p => {
        p.asInstanceOf[CassandraPartition[_, _]]
      })
      var allTokenRanges = Set[(Token[_], Token[_])]()
      cassandraPartitions.foreach(p => {
        p.tokenRanges
          .asInstanceOf[Vector[CqlTokenRange[_, _]]]
          .foreach(tr => {
            val range =
              Set(
                (tr.range.start.asInstanceOf[Token[_]],
                 tr.range.end.asInstanceOf[Token[_]]))
            allTokenRanges = allTokenRanges ++ range
          })

      })

      log.info(
        "All token ranges extracted from partitions size:" + allTokenRanges.size)

      if (migratorConfig.skipTokenRanges != None) {
        log.info(
          "Savepoints array defined, size of the array: " + migratorConfig.skipTokenRanges.size)

        val diff = allTokenRanges.diff(migratorConfig.skipTokenRanges)
        log.info(
          "Diff ... total diff of full ranges to savepoints is: " + diff.size)
        log.debug("Dump of the missing tokens: ")
        log.debug(diff)
      }
    }

    log.info("Starting write...")

    try {
      migratorConfig.target match {
        case target: TargetSettings.Scylla =>
          writers.Scylla.writeDataframe(target,
                                        migratorConfig.renames,
                                        sourceDF.dataFrame,
                                        sourceDF.timestampColumns)
      }
    } catch {
      case NonFatal(e) => // Catching everything on purpose to try and dump the accumulator state
        log.error(
          "Caught error while writing the DataFrame. Will create a savepoint before exiting",
          e)
    } finally {
      scheduler.shutdown()
      spark.stop()
    }
  }

  def savepointFilename(path: String): String =
    s"${path}/savepoint_${System.currentTimeMillis / 1000}.yaml"

  def dumpAccumulatorState(config: MigratorConfig, reason: String): Unit = {
    val filename =
      Paths.get(savepointFilename(config.savepoints.path)).normalize
    val modifiedConfig = config.copy(
      skipTokenRanges = config.skipTokenRanges
    )

    Files.write(filename,
                modifiedConfig.render.getBytes(StandardCharsets.UTF_8))
  }
}
