package io.delta.standalone.internal

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.expressions.{And, EqualTo, GreaterThanOrEqual, Literal}
import io.delta.standalone.{DeltaLog, Operation}
import io.delta.standalone.types.{IntegerType, StringType, StructField, StructType}

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata}
import io.delta.standalone.internal.util.TestUtils._

class DataSkippingSuite extends FunSuite {
  private val op = new Operation(Operation.Name.WRITE)

  private val partitionSchema = new StructType(Array(
    new StructField("col1", new StringType(), true),
    new StructField("col2", new StringType(), true)
  ))

  private val schema = new StructType(Array(
    new StructField("col1", new StringType(), true),
    new StructField("col2", new StringType(), true),
    new StructField("col3", new StringType(), true),
    new StructField("col4", new StringType(), true)
  ))

  val metadata: Metadata = Metadata(
    partitionColumns = partitionSchema.getFieldNames, schemaString = schema.toJson)


  private val files = (1 to 9).map { i =>
    val partitionValues = Map("col1" -> (i % 3).toString, "col2" -> (i % 2).toString)
    val columnStats = "\"{\\\"numRecords\\\":1,\\\"minValues\\\":{\\\"col3\\\":\\\"" + i.toString +
      "\\\",\\\"col4\\\":\\\"" + (i*2).toString + "\\\"}," +
      "\\\"maxValues\\\":{\\\"col3\\\":\\\"" + i.toString + "\\\",\\\"col4\\\":\\\"" +
      (i*2).toString + "\\\"},\\\"nullCount\\\":{\\\"col3\\\":0,\\\"col4\\\":0}}\""
    AddFile(i.toString, partitionValues, 1L, 1L, dataChange = true, stats = columnStats)
  }

  private val metadataConjunct = new GreaterThanOrEqual(schema.column("col1"), Literal.of("0"))
  private val dataConjunct = new EqualTo(schema.column("col3"), Literal.of("5"))

  def withLog(actions: Seq[Action])(test: DeltaLog => Unit): Unit = {
    withTempDir { dir =>
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.startTransaction().commit(metadata :: Nil, op, "engineInfo")
      log.startTransaction().commit(actions, op, "engineInfo")

      test(log)
    }
  }
  withLog(files) { log =>
    val scan = log.update().scan(new And(dataConjunct, metadataConjunct))
    print(scan.getPushedPredicate.get().toString)
    val iter = scan.getFiles
    while (iter.hasNext) {
      print(iter.next().getStats + "\n")
    }
  }
}
