/*
 * Experimental
 */

package org.apache.spark.sql

import org.scalatest._
import Matchers._
import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.CodeGeneration
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Add, ExpressionEvalHelper, Literal}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.execution.{WholeStageCodegenExec, debug}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext

class TestCodegen extends CodeGeneration {
  override def context: CodegenContext = {
    Tracker.testCount += 1
    new CodegenContext
  }
}

object Tracker {
  var testCount = 0
}

/**
 * Additional tests for code generation.
 */
class CodeGenerationSpecialCasesSuite extends SparkFunSuite  with SharedSQLContext {
  import testImplicits._
  
//  def altDumpUdf(df: DataFrame): String = {
//    val plan = df.queryExecution.executedPlan
//    execution.debug.codegenString(plan)
//  }
  
  def dumpUdf(df: DataFrame): String = {
    val plan = df.queryExecution.executedPlan
    plan match {
      case wsce: WholeStageCodegenExec => CodeFormatter.format(wsce.doCodeGen()._2)
      case bs => "???"
    }
  }
  
  test("sf#1") {
    sparkConf.set("spark.sql.codegen.factory", "org.apache.spark.sql.TestCodegen")
    SparkEnv.get.conf.set("spark.sql.codegen.factory", "org.apache.spark.sql.TestCodegen")
    
    val ua = udf((i: Int) => Array(i))
    val u = udf((i: Int) => i)
    val value1: RDD[Int] = spark.sparkContext.parallelize(Seq(42))
//    val value1 = spark.createDataset(1 to 5)
    
    val df0 = value1.toDF("i")
    
    val udfs: Seq[Column] = Seq(new ColumnName("*")) ++ (0 until 95).map(_ => ua(u(Column("i"))))
    val df = df0.select(udfs: _*)
    val code = dumpUdf(df)  // .split("\\n").toList
//    val code = altDumpUdf(df).split("\\n").toList.drop(7)
//    for ((a,b) <- code zip code0) { a shouldBe b }
//    code shouldBe code1
    // scalastyle:off println println(...)
    Tracker.testCount shouldBe 1
    println(code)
    // scalastyle:on
    Succeeded
  }

}
