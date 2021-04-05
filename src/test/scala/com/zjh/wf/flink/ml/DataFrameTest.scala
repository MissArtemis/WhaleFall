package com.zjh.wf.flink.ml

import com.alibaba.alink.operator.batch.BatchOperator
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp
import org.scalatest.funsuite.AnyFunSuite

class DataFrameTest extends AnyFunSuite{
  test("CSVBATCH") {
    val filePath = "http://alink-dataset.cn-hangzhou.oss.aliyun-inc.com/csv/iris.csv"
    val schema = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"
    val csvSource = new CsvSourceBatchOp().setFilePath(filePath).setSchemaStr(schema).setFieldDelimiter(",")
    csvSource.print()
    csvSource.initializeDataSource()
  }

}
