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

package io.delta.standalone.internal.scan

import java.util.Optional

import scala.collection.mutable

import io.delta.standalone.expressions.{And, Column, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or}
import io.delta.standalone.types.{DataType, LongType, StructType}

import io.delta.standalone.internal.actions.{AddFile, MemoryOptimizedLogReplay}
import io.delta.standalone.internal.data.{ColumnStatsRowRecord, PartitionRowRecord}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.{JsonUtils, PartitionUtils}

private [internal] case class ColumnStatsPredicate(
    expr: Expression,
    referencedStats: Set[Column])

private[internal] case class ColumnNode(
    var dataType: DataType = new StructType(),
    columnPath: Seq[String] = Nil,
    nestedColumns: mutable.Map[String, ColumnNode]
      = mutable.Map.empty[String, ColumnNode],
    statsValue: mutable.Map[String, String] = mutable.Map.empty[String, String])

/**
 * An implementation of [[io.delta.standalone.DeltaScan]] that filters files and only returns
 * those that match the [[getPushedPredicate]].
 *
 * If the pushed predicate is empty, then all files are returned.
 */
final private[internal] class FilteredDeltaScanImpl(
    replay: MemoryOptimizedLogReplay,
    expr: Expression,
    partitionSchema: StructType,
    tableSchema: StructType) extends DeltaScanImpl(replay) {
  /* The total number of records in the file. */
  final val NUM_RECORDS = "numRecords"
  /* The smallest (possibly truncated) value for a column. */
  final val MIN = "minValues"
  /* The largest (possibly truncated) value for a column. */
  final val MAX = "maxValues"
  /* The number of null values present for a column. */
  final val NULL_COUNT = "nullCount"

  private val partitionColumns = partitionSchema.getFieldNames.toSeq

  private val (metadataConjunction, dataConjunction) =
    PartitionUtils.splitMetadataAndDataPredicates(expr, partitionColumns)

  private var statsStore = ColumnNode()

  override protected def accept(addFile: AddFile): Boolean = {
    if (metadataConjunction.isEmpty && dataConjunction.isEmpty) return true

    // filter the Delta Scan files by partition values
    val partitionRowRecord = new PartitionRowRecord(partitionSchema, addFile.partitionValues)
    val result = metadataConjunction.get.eval(partitionRowRecord).asInstanceOf[Boolean]

    // recursively construct all column stats by ColumnStatsInternal &&
    // find values in the StructType in ColumnStatsRowRecord
    JsonUtils.fromJson[Map[String, String]](addFile.stats) foreach { stats =>
      statsStore = parseColumnStats(
        tableSchema,
        statsStr = stats._2,
        statsType = stats._1,
        statsStore)
    }

    // construct column evaluator
    val dataRowRecord = new ColumnStatsRowRecord(tableSchema, statsStore)

    // make filters by column stats and data conjunction in query
    val columnStatsPredicate = constructDataFilters(dataConjunction.get).getOrElse {
      return result
    }
    val finalColumnStatsPredicate = new And(columnStatsPredicate.expr,
      verifyStatsForFilter(columnStatsPredicate.referencedStats))

    val columnStatsFilterResult = finalColumnStatsPredicate
      .eval(dataRowRecord)
      .asInstanceOf[Boolean]

    result && columnStatsFilterResult
  }

  override def getInputPredicate: Optional[Expression] = Optional.of(expr)

  override def getPushedPredicate: Optional[Expression] =
    Optional.ofNullable(metadataConjunction.orNull)

  override def getResidualPredicate: Optional[Expression] =
    Optional.ofNullable(dataConjunction.orNull)

  // TODO the same as `parseAttributeName` in
  //  `org/apache/spark/sql/catalyst/analysis/unresolved.scala`
  /*
  private def parseAndValidateColumn(name: String): Seq[String] = {
    val e = DeltaErrors.invalidColumnName("exprStr")
    val nameParts = mutable.ArrayBuffer.empty[String]
    val tmp = mutable.ArrayBuffer.empty[Char]
    var inBacktick = false
    var i = 0
    while (i < name.length) {
      val char = name(i)
      if (inBacktick) {
        if (char == '`') {
          if (i + 1 < name.length && name(i + 1) == '`') {
            tmp += '`'
            i += 1
          } else {
            inBacktick = false
            if (i + 1 < name.length && name(i + 1) != '.') throw e
          }
        } else {
          tmp += char
        }
      } else {
        if (char == '`') {
          if (tmp.nonEmpty) throw e
          inBacktick = true
        } else if (char == '.') {
          if (name(i - 1) == '.' || i == name.length - 1) throw e
          nameParts += tmp.mkString
          tmp.clear()
        } else {
          tmp += char
        }
      }
      i += 1
    }
    if (inBacktick) throw e
    nameParts += tmp.mkString
    nameParts
  }
   */

  private def findColumnNode(path: Seq[String]): Option[ColumnNode] = {
    var curSchema: Option[ColumnNode] = Some(statsStore)
    path foreach { x =>
      if (curSchema.isEmpty || !curSchema.get.dataType.isInstanceOf[StructType]) {
        return None
      }
      curSchema = curSchema.get.nestedColumns.get(x)
    }
    curSchema
  }

  /*
  private def findColumnStatsInBatch(path: Seq[String], statTypes: Seq[String]):
  Option[Seq[(DataType, String)]] = {
    val columnNode = findColumnNode(path).getOrElse {
      return None
    }
    val columnStatsExtractor = columnNode.statsValue.getOrElse(_, return None)
    Some(statTypes.map {
      case MAX => (columnNode.dataType, columnStatsExtractor(MAX))
      case MIN => (columnNode.dataType, columnStatsExtractor(MIN))
      case NULL_COUNT => (LongType, columnStatsExtractor(NULL_COUNT))
      case NUM_RECORDS => (LongType, statsStore.statsValue.getOrElse(NUM_RECORDS, return None))
    })
  }
   */

  def parseColumnStats(
      tableSchema: DataType,
      statsStr: String,
      statsType: String,
      structuredStats: ColumnNode): ColumnNode = tableSchema match {
    case structNode: StructType =>
      val newStats = structuredStats
      structNode.getFields.foreach { fieldName =>
        val jsonMapper = JsonUtils.fromJson[Map[String, String]](statsStr)
        val originSubStatsStr = jsonMapper.getOrElse(fieldName.getName, null)
        if (originSubStatsStr == null) {
          throw DeltaErrors.nullValueFoundForNonNullSchemaField(fieldName.getName, structNode)
        }

        val originSubStats = newStats.nestedColumns
          .getOrElseUpdate(fieldName.getName, ColumnNode())
        newStats.columnPath :+ fieldName.getName
        val newSubStats = parseColumnStats(
          fieldName.getDataType,
          originSubStatsStr,
          statsType,
          originSubStats
        )

        // TODO make this by shallow copy
        newStats.nestedColumns += (fieldName.getName -> newSubStats)
      }
      newStats
    case dataNode: DataType =>
      val newStats = structuredStats
      newStats.dataType = dataNode
      newStats.statsValue += (statsType -> statsStr)
      newStats
  }

  private def constructDataFilters(expression: Expression):
    Option[ColumnStatsPredicate] = expression match {
    case and: And =>
      val e1 = constructDataFilters(and.getLeft)
      val e2 = constructDataFilters(and.getRight)
      if (e1.isDefined && e2.isDefined) {
        Some(ColumnStatsPredicate(
          new And(e1.get.expr, e2.get.expr),
          e1.get.referencedStats ++ e2.get.referencedStats))
      } else if (e1.isDefined) {
        e1
      } else {
        e2  // possibly None
      }

    case or: Or =>
      val e1 = constructDataFilters(or.getLeft)
      val e2 = constructDataFilters(or.getRight)
      if (e1.isDefined && e2.isDefined) {
        Some(ColumnStatsPredicate(
          new Or(e1.get.expr, e2.get.expr),
          e1.get.referencedStats ++ e2.get.referencedStats))
      } else {
        None
      }

    case isNull: IsNull => isNull.children().get(0) match {
      case child: Column =>
        val columnPath = child.getName
        val nullCount = new Column(columnPath :+ NULL_COUNT, new LongType())

        Some(ColumnStatsPredicate(
          new GreaterThan(nullCount, Literal.of(0)),
          Set(nullCount)))

      case _ => None
    }

    case isNotNull: IsNotNull => isNotNull.children().get(0) match {
      case child: Column =>
        val columnPath = child.getName
        val nullCount = new Column(columnPath :+ NULL_COUNT, new LongType())
        val numRecords = new Column(NUM_RECORDS, new LongType())

        Some(ColumnStatsPredicate(
          new LessThan(nullCount, numRecords),
          Set(nullCount, numRecords)))

      case _ => None
    }

    case eq: EqualTo => (eq.getLeft, eq.getRight) match {
      case (e1: Column, e2: Literal) =>
        val columnPath = e1.getName
        val columnNode = findColumnNode(columnPath).getOrElse(return None)
        val minColumn = new Column(columnPath :+ MIN, columnNode.dataType)
        val maxColumn = new Column(columnPath :+ MAX, columnNode.dataType)

        Some(ColumnStatsPredicate(
          new And(new LessThanOrEqual(minColumn, e2),
            new GreaterThanOrEqual(maxColumn, e2)),
          Set(minColumn, maxColumn)))

      case (_: Literal, _: Literal) => Some(ColumnStatsPredicate(eq, Set()))
      case (e1: Literal, e2: Column) => constructDataFilters(new EqualTo(e2, e1))
      case (e1: Column, e2: Column) =>
        val e1Path = e1.getName
        val e1Node = findColumnNode(e1Path).getOrElse(return None)
        val e1Min = new Column(e1Path :+ MIN, e1Node.dataType)
        val e1Max = new Column(e1Path :+ MAX, e1Node.dataType)
        val e2Path = e2.getName
        val e2Node = findColumnNode(e2Path).getOrElse(return None)
        val e2Min = new Column(e2Path :+ MIN, e2Node.dataType)
        val e2Max = new Column(e2Path :+ MAX, e2Node.dataType)

        Some(ColumnStatsPredicate(
          new And(new GreaterThanOrEqual(e1Max, e2Min),
            new LessThanOrEqual(e1Min, e2Max)),
          Set(e1Min, e1Max, e2Min, e2Max)))
      case _ => None
    }

    // TODO refactor all these into 3 types: binary operation, binary comparison
    //  and unary expression
    case lt: LessThan => (lt.getLeft, lt.getRight) match {
      case (Column, Literal) =>
        val e1 = lt.getLeft
        val e2 = lt.getRight

        val columnPath = e1.asInstanceOf[Column].getName
        val columnNode = findColumnNode(columnPath).getOrElse(return None)
        val minColumn = new Column(columnPath :+ MIN, columnNode.dataType)

        Some(ColumnStatsPredicate(
          new LessThan(minColumn, e2),
          Set(minColumn)))

      case (Literal, Literal) => Some(ColumnStatsPredicate(lt, Set()))
      case (Literal, Column) => constructDataFilters(new GreaterThanOrEqual(lt.getRight, lt.getLeft))
      case _ => None
    }

    case leq: LessThanOrEqual => (leq.getLeft, leq.getRight) match {
      case (Column, Literal) =>
        val e1 = leq.getLeft
        val e2 = leq.getRight

        val columnPath = e1.asInstanceOf[Column].getName
        val columnNode = findColumnNode(columnPath).getOrElse(return None)
        val minColumn = new Column(columnPath :+ MIN, columnNode.dataType)

        Some(ColumnStatsPredicate(
          new LessThanOrEqual(minColumn, e2),
          Set(minColumn)))

      case (Literal, Literal) => Some(ColumnStatsPredicate(leq, Set()))
      case (Literal, Column) => constructDataFilters(new GreaterThan(leq.getRight, leq.getLeft))
      case _ => None
    }

    case gt: GreaterThan => (gt.getLeft, gt.getRight) match {
      case (Column, Literal) =>
        val e1 = gt.getLeft
        val e2 = gt.getRight

        val columnPath = e1.asInstanceOf[Column].getName
        val columnNode = findColumnNode(columnPath).getOrElse(return None)
        val maxColumn = new Column(columnPath :+ MAX, columnNode.dataType)

        Some(ColumnStatsPredicate(
          new GreaterThan(maxColumn, e2),
          Set(maxColumn)))

      case (Literal, Literal) => Some(ColumnStatsPredicate(gt, Set()))
      case (Literal, Column) => constructDataFilters(new LessThanOrEqual(gt.getRight, gt.getLeft))
      case _ => None
    }

    case geq: LessThanOrEqual => (geq.getLeft, geq.getRight) match {
      case (Column, Literal) =>
        val e1 = geq.getLeft
        val e2 = geq.getRight

        val columnPath = e1.asInstanceOf[Column].getName
        val columnNode = findColumnNode(columnPath).getOrElse(return None)
        val maxColumn = new Column(columnPath :+ MAX, columnNode.dataType)

        Some(ColumnStatsPredicate(
          new GreaterThanOrEqual(maxColumn, e2),
          Set(maxColumn)))

      case (Literal, Literal) => Some(ColumnStatsPredicate(geq, Set()))
      case (Literal, Column) => constructDataFilters(new LessThan(geq.getRight, geq.getLeft))
      case _ => None
    }

    // TODO ordering package is needed in Standalone to sort IN-list
    case in: In => None
    /*
      import scala.collection.convert.ImplicitConversions._
      val list = in.children()
      if (list.size() <= 0) return Some(ColumnStatsPredicate(Literal.False, Set()))
      list.toArray[Expression].reduceLeft(_ min _)
     */

    case not: Not => not.children().get(0) match {
      case and: And =>
        constructDataFilters(new Or(new Not(and.getLeft), new Not(and.getRight)))
      case or: Or =>
        constructDataFilters(new And(new Not(or.getLeft), new Not(or.getRight)))
      case isNull: IsNull =>
        constructDataFilters(new IsNotNull(isNull.children().get(0)))
      case isNotNull: IsNotNull =>
        constructDataFilters(new IsNull(isNotNull.children().get(0)))
      case eq: EqualTo => (eq.getLeft, eq.getRight) match {
        case (Column, Literal) =>
          val e1 = eq.getLeft
          val e2 = eq.getRight

          val columnPath = e1.asInstanceOf[Column].getName
          val columnNode = findColumnNode(columnPath).getOrElse(return None)
          val minColumn = new Column(columnPath :+ MIN, columnNode.dataType)
          val maxColumn = new Column(columnPath :+ MAX, columnNode.dataType)

          Some(ColumnStatsPredicate(
            new Or(new LessThan(minColumn, e2),
              new GreaterThan(maxColumn, e2)),
            Set(columnPath :+ MIN, columnPath :+ MAX)))

        case (Literal, Literal) => Some(ColumnStatsPredicate(new Not(eq), Set()))
        case (Literal, Column) => constructDataFilters(new Not(new EqualTo(eq.getRight, eq.getLeft)))
        case _ => None
      }
      case lt: LessThan =>
        constructDataFilters(new GreaterThanOrEqual(lt.getLeft, lt.getRight))
      case leq: LessThanOrEqual =>
        constructDataFilters(new GreaterThan(leq.getLeft, leq.getRight))
      case gt: GreaterThan =>
        constructDataFilters(new LessThanOrEqual(gt.getLeft, gt.getRight))
      case geq: GreaterThanOrEqual =>
        constructDataFilters(new LessThan(geq.getLeft, geq.getRight))
      case in: In => None
      // TODO add ordering support to Standalone
    }

    // Unknown expression type... can't use it for data skipping.
    case _ => None
  }

  private def verifyStatsForFilter(
      referencedStats: Set[Column]): Expression = {
    referencedStats.filter(_.getName.length <= 0).map { refStats =>
      refStats.getName.last match {
        case MAX | MIN =>
          val nullCount = new Column(refStats.getName.dropRight(1), new LongType())
          val numRecords = new Column(NUM_RECORDS, new LongType())
          new Or(new IsNotNull(refStats), new EqualTo(nullCount, numRecords))

        case _ => new IsNotNull(refStats)
      }
    }.reduceLeft(new And(_, _))
  }
}
