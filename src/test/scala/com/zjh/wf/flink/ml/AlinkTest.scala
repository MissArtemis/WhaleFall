package com.zjh.wf.flink.ml

import org.scalatest.funsuite.AnyFunSuite
import com.alibaba.alink.operator.batch.BatchOperator
import com.alibaba.alink.operator.batch.dataproc.SplitBatchOp
import com.alibaba.alink.operator.batch.recommendation.AlsRateRecommBatchOp
import com.alibaba.alink.operator.batch.recommendation.AlsTrainBatchOp
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp
import com.alibaba.alink.pipeline.classification.GbdtClassifier

class AlinkTest extends AnyFunSuite{
  test("GBDT") {
    val schema = "age bigint, workclass string, fnlwgt bigint, education string, " +
      "education_num bigint, marital_status string, occupation string, " +
      "relationship string, race string, sex string, capital_gain bigint, " +
      "capital_loss bigint, hours_per_week bigint, native_country string, label string";

    val trainData = new CsvSourceBatchOp().setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/adult_train.csv").setSchemaStr(schema)
    val testData = new CsvSourceBatchOp().setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/adult_test.csv").setSchemaStr(schema)
//    trainData.getDataSet.print()
//    testData.getDataSet.print()
    val gbdt = new GbdtClassifier()
                  .setFeatureCols("age", "capital_gain", "capital_loss", "hours_per_week", "workclass", "education", "marital_status", "occupation")
                  .setCategoricalCols("workclass", "education", "marital_status", "occupation")
                  .setLabelCol("label")
                  .setNumTrees(20)
                  .setPredictionCol("prediction_result")
    val result = gbdt.fit(trainData).transform(testData)
//    result.getDataSet.print()
    println(result.getColNames.mkString(","))
  }

}
