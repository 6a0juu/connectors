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

import com.fasterxml.jackson.databind.JsonNode

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
    parseColumnStats(tableSchema, addFile)

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

  /** Get column stats from the given path */
  def findColumnNode(path: Seq[String]): Option[ColumnNode] = {
    var curSchema: Option[ColumnNode] = Some(statsStore)
    path foreach { x =>
      if (curSchema.isEmpty || !curSchema.get.dataType.isInstanceOf[StructType]) {
        return None
      }
      curSchema = curSchema.get.nestedColumns.get(x)
    }
    curSchema
  }

  /** Parse the entire [[AddFile.stats]] string based on table schema */
  def parseColumnStats(
      tableSchema: DataType,
      addFile: AddFile): Unit = {
    // Reset the store for current AddFile
    statsStore = ColumnNode()

    JsonUtils.fromJson[Map[String, JsonNode]](addFile.stats) foreach { stats =>
      if (!stats._2.isObject) {
        // This is an table-level stats, add to root node directly
        statsStore.statsValue += (stats._1 -> stats._2.asText)
      } else {
        // Update the store in the every stats type sequence
        statsStore = parseColumnStats(
          tableSchema,
          statsNode = stats._2,
          statsType = stats._1,
          statsStore)
      }
    }
  }

  /** Parse the column stats recursively based on table schema */
  def parseColumnStats(
      tableSchema: DataType,
      statsNode: JsonNode,
      statsType: String,
      structuredStats: ColumnNode): ColumnNode = tableSchema match {
    case structNode: StructType =>
      // This is a structured column, iterate through all the nested columns and parse them
      // recursively.

      // Get the previous stored column stats for this column, if there are.
      val originStats = structuredStats
      structNode.getFields.foreach { fieldName =>
        val originSubStatsNode = statsNode.get(fieldName.getName)
        if (originSubStatsNode != null) {
          // If there is not such sub column in table schema, skip it.

          // Get the previous stored column stats for this sub column, if there are.
          val originSubStats = originStats.nestedColumns
            .getOrElseUpdate(fieldName.getName,
              ColumnNode(columnPath = originStats.columnPath :+ fieldName.getName))

          // Parse and update the nested column nodes to the current column node.
          val newSubStats = parseColumnStats(
            fieldName.getDataType,
            originSubStatsNode,
            statsType,
            originSubStats)
          originStats.nestedColumns += (fieldName.getName -> newSubStats)
        }
      }
      originStats

    case dataNode: DataType =>
      // This is a leaf column (data column), parse the corresponding stats value in string format.
      ColumnNode(
        dataNode,
        structuredStats.columnPath,
        structuredStats.nestedColumns,
        structuredStats.statsValue + (statsType -> statsNode.asText))
  }

  def constructDataFilters(expression: Expression):
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
      case (e1: Column, e2: Literal) =>
        val columnPath = e1.getName
        val columnNode = findColumnNode(columnPath).getOrElse(return None)
        val minColumn = new Column(columnPath :+ MIN, columnNode.dataType)

        Some(ColumnStatsPredicate(
          new LessThan(minColumn, e2),
          Set(minColumn)))
      case (_: Literal, _: Literal) => Some(ColumnStatsPredicate(lt, Set()))
      case (e1: Literal, e2: Column) => constructDataFilters(
        new GreaterThanOrEqual(e2, e1))
      case (e1: Column, e2: Column) =>
        val e1Path = e1.getName
        val e1Node = findColumnNode(e1Path).getOrElse(return None)
        val e1Min = new Column(e1Path :+ MIN, e1Node.dataType)
        val e2Path = e2.getName
        val e2Node = findColumnNode(e2Path).getOrElse(return None)
        val e2Max = new Column(e2Path :+ MAX, e2Node.dataType)

        Some(ColumnStatsPredicate(new LessThan(e1Min, e2Max), Set(e1Min, e2Max)))
      case _ => None
    }

    case leq: LessThanOrEqual => (leq.getLeft, leq.getRight) match {
      case (e1: Column, e2: Literal) =>
        val columnPath = e1.getName
        val columnNode = findColumnNode(columnPath).getOrElse(return None)
        val minColumn = new Column(columnPath :+ MIN, columnNode.dataType)

        Some(ColumnStatsPredicate(
          new LessThanOrEqual(minColumn, e2),
          Set(minColumn)))

      case (_: Literal, _: Literal) => Some(ColumnStatsPredicate(leq, Set()))
      case (e1: Literal, e2: Column) => constructDataFilters(new GreaterThan(e2, e1))
      case (e1: Column, e2: Column) =>
        val e1Path = e1.getName
        val e1Node = findColumnNode(e1Path).getOrElse(return None)
        val e1Min = new Column(e1Path :+ MIN, e1Node.dataType)
        val e2Path = e2.getName
        val e2Node = findColumnNode(e2Path).getOrElse(return None)
        val e2Max = new Column(e2Path :+ MAX, e2Node.dataType)

        Some(ColumnStatsPredicate(new LessThanOrEqual(e1Min, e2Max), Set(e1Min, e2Max)))
      case _ => None
    }

    case gt: GreaterThan => (gt.getLeft, gt.getRight) match {
      case (e1: Column, e2: Literal) =>
        val columnPath = e1.getName
        val columnNode = findColumnNode(columnPath).getOrElse(return None)
        val maxColumn = new Column(columnPath :+ MAX, columnNode.dataType)

        Some(ColumnStatsPredicate(
          new GreaterThan(maxColumn, e2),
          Set(maxColumn)))
      case (_: Literal, _: Literal) => Some(ColumnStatsPredicate(gt, Set()))
      case (Literal, Column) => constructDataFilters(new LessThanOrEqual(gt.getRight, gt.getLeft))
      case (e1: Column, e2: Column) =>
        val e1Path = e1.getName
        val e1Node = findColumnNode(e1Path).getOrElse(return None)
        val e1Max = new Column(e1Path :+ MAX, e1Node.dataType)
        val e2Path = e2.getName
        val e2Node = findColumnNode(e2Path).getOrElse(return None)
        val e2Min = new Column(e2Path :+ MIN, e2Node.dataType)

        Some(ColumnStatsPredicate(new GreaterThan(e1Max, e2Min), Set(e1Max, e2Min)))
      case _ => None
    }

    case geq: LessThanOrEqual => (geq.getLeft, geq.getRight) match {
      case (e1: Column, e2: Literal) =>
        val columnPath = e1.getName
        val columnNode = findColumnNode(columnPath).getOrElse(return None)
        val maxColumn = new Column(columnPath :+ MAX, columnNode.dataType)

        Some(ColumnStatsPredicate(
          new GreaterThanOrEqual(maxColumn, e2),
          Set(maxColumn)))
      case (_: Literal, _: Literal) => Some(ColumnStatsPredicate(geq, Set()))
      case (e1: Literal, e2: Column) => constructDataFilters(new LessThan(e2, e1))
      case (e1: Column, e2: Column) =>
        val e1Path = e1.getName
        val e1Node = findColumnNode(e1Path).getOrElse(return None)
        val e1Max = new Column(e1Path :+ MAX, e1Node.dataType)
        val e2Path = e2.getName
        val e2Node = findColumnNode(e2Path).getOrElse(return None)
        val e2Min = new Column(e2Path :+ MIN, e2Node.dataType)

        Some(ColumnStatsPredicate(new GreaterThanOrEqual(e1Max, e2Min), Set(e1Max, e2Min)))
      case _ => None
    }

    // TODO ordering package is needed in Standalone to sort IN-list
    /*
    case in: In => None
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
        case (Literal, Column) => constructDataFilters(
          new Not(new EqualTo(eq.getRight, eq.getLeft)))
        case ()
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
