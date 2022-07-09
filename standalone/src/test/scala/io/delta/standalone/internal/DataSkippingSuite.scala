/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone.internal

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.{DeltaLog, Operation}
import io.delta.standalone.expressions.{And, EqualTo, GreaterThanOrEqual, Literal}
import io.delta.standalone.types.{IntegerType, LongType, StringType, StructField, StructType}

import io.delta.standalone.internal.actions.{Action, AddFile, Metadata}
import io.delta.standalone.internal.util.TestUtils._

class DataSkippingSuite extends FunSuite {
  private val op = new Operation(Operation.Name.WRITE)

  private val partitionSchema = new StructType(Array(
    new StructField("col1", new IntegerType(), true),
    new StructField("col2", new IntegerType(), true)
  ))

  private val schema = new StructType(Array(
    new StructField("col1", new IntegerType(), true),
    new StructField("col2", new IntegerType(), true),
    new StructField("col3", new IntegerType(), true),
    new StructField("col4", new IntegerType(), true)
  ))

  val metadata: Metadata = Metadata(
    partitionColumns = partitionSchema.getFieldNames, schemaString = schema.toJson)


  private val files = (1 to 9).map { i =>
    val partitionValues = Map("col1" -> (i % 3).toString, "col2" -> (i % 2).toString)
    val columnStats = "\"{\\\"numRecords\\\":1,\\\"minValues\\\":{\\\"col3\\\":" + i.toString +
      ",\\\"col4\\\":" + (i*2).toString + "}," +
      "\\\"maxValues\\\":{\\\"col3\\\":" + i.toString + ",\\\"col4\\\":" +
      (i*2).toString + "},\\\"nullCount\\\":{\\\"col3\\\":0,\\\"col4\\\":0}}\""
    AddFile(i.toString, partitionValues, 1L, 1L, dataChange = true, stats = columnStats)
  }
  // all types can be automatically parsed here

  private val metadataConjunct = new GreaterThanOrEqual(schema.column("col1"), Literal.of(0))
  private val dataConjunct = new EqualTo(schema.column("col3"), Literal.of(5))

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

  test("integration test using spark-built delta table") {
    //
  }
}
