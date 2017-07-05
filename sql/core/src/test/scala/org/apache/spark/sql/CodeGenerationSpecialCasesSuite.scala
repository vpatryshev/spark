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

import scala.collection.mutable

class TestCodegen extends CodeGeneration {
  override def context: CodegenContext = {
    Tracker.testCount += 1
    new CodegenContext {
      override def addReferenceObj(obj: Any): String = {
        val idx = references.length
        if (!references.contains(obj)) references += obj
        val clsName = obj.getClass.getName
        s"(($clsName) references[$idx])"
      }

      private val objectCache: mutable.Map[(String, Any), String] =
        new mutable.HashMap[(String, Any), String]()
      
      override def addReferenceObj(name: String, obj: Any, className: String = null): String = {
        objectCache.getOrElseUpdate((name, obj), super.addReferenceObj(name, obj, className))
      }

      private val udfCache: mutable.Map[(String, String), String] =
        new mutable.HashMap[(String, String), String]()

      override def addNewTerm(javaType: String, name: String, initCode: String => String): String =
      {
        val key = (name, initCode("`variable name`"))
        udfCache.getOrElseUpdate(key, super.addNewTerm(javaType, name, initCode))
      }
    }
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
