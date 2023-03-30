// Databricks notebook source
// DBTITLE 1,Dernière séance

val df = spark.read.parquet("/FileStore/tables/ml_data2").toDF()

df.show
//Il essaye de prédire l'idClass à partir des molécules (codeSub)

// Q6 avec 1.0 d'accuracy
//val df_filtered_id = df.filter(col("idclass") === 117 || col("idclass") === 372)
//val df_filtered_code = df_filtered_id.filter(col("codeSub") === 94037 || col("codeSub") === 2202)

//Q7 avec 1.0 d'accuracy
//val df_filtered_id = df.filter(col("idclass") === 117 || col("idclass") === 372)
//val df_filtered_code = df_filtered_id.filter(col("codeSub") === 94037 || col("codeSub") === 2202 || col("codeSub") === 2092)

//Q8 avec 1.0 d'accuracy
//val df_filtered_id = df.filter(col("idclass") === 117 || col("idclass") === 372 || col("idclass") === 394)
//val df_filtered_code = df_filtered_id.filter(col("codeSub") === 94037 || col("codeSub") === 2202 || col("codeSub") === 2092 || col("codeSub") === 1014)

//Q9 avec 0.9868421052631579 d'accuracy
val df_filtered_id = df.filter(col("idclass") === 117 || col("idclass") === 372 || col("idclass") === 394 || col("idclass") === 204)
val df_filtered_code = df_filtered_id.filter(col("codeSub") === 94037 || col("codeSub") === 2202 || col("codeSub") === 2092 || col("codeSub") === 1014 || col("codeSub") === 24245)

//Q11
//val df_filtered_id = df.filter(col("idclass") === 117 || col("idclass") === 372 || col("idclass") === 394 || col("idclass") === 56 || col("idclass") === 422 || col("idclass") === 372)
//val df_filtered_code = df_filtered_id.filter(col("codeSub") === 94037 || col("codeSub") === 2202 || col("codeSub") === 2092 || col("codeSub") === 1014 || col("codeSub") === 24563 || col("codeSub") === 23957 || col("codeSub") === 30981)

val data = df_filtered_code.select("idclass", "codeSub")

val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

val sIndexer = new StringIndexer().setInputCol("idclass").setOutputCol("idclassLabel").fit(trainingData)

val ohe = new OneHotEncoder().setInputCol("codeSub").setOutputCol("codeOHE")

val assembler = new VectorAssembler().setInputCols(Array("codeOHE")).setOutputCol("features")

val tree = new DecisionTreeClassifier()
  .setLabelCol("idclassLabel")

val pipeline = new Pipeline()
  .setStages(Array(sIndexer, ohe, assembler, tree))

//println(df_filtered_code.count)
display(df_filtered_code)

// COMMAND ----------
val check = df_filtered_code.select("codeSub", "idclass").distinct
display(check)
//var test = check.select("codeSub","idclass").filter(col("codeSub") === 2202 && (col("idclass") === 117))
//var test = check.select("codeSub","idclass").filter(col("codeSub") === 2202 && (col("idclass") === 394))
//var test = check.select("codeSub","idclass").filter(col("codeSub") === 2092 && (col("idclass") === 204))
//var test = check.select("codeSub","idclass").filter(col("codeSub") === 2092 && (col("idclass") === 117))
//var test = check.select("codeSub","idclass").filter(col("codeSub") === 159 && (col("idclass") === 1810))
//println(test.count)

//val checkDF = df.groupBy("codeSub", "idClass").count()

//display(checkDF)



// COMMAND ----------

val model = pipeline
  .fit(trainingData)

val predictions = model.transform(testData)
//val predictions = model.transform(trainingData)

val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("idclassLabel")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions)
println(s"Test Error = ${(1.0 - accuracy)}")


// COMMAND ----------

val treeModelQ6 = model.stages(model.stages.length - 1).asInstanceOf[DecisionTreeClassificationModel]
display(treeModelQ6)

// COMMAND ----------

val treeModelQ7 = model.stages(model.stages.length - 1).asInstanceOf[DecisionTreeClassificationModel]
display(treeModelQ7)

// COMMAND ----------

val treeModelQ8 = model.stages(model.stages.length - 1).asInstanceOf[DecisionTreeClassificationModel]
display(treeModelQ8)

// COMMAND ----------

val treeModelQ9 = model.stages(model.stages.length - 1).asInstanceOf[DecisionTreeClassificationModel]
display(treeModelQ9)

// COMMAND ----------

val treeModelQ11 = model.stages(model.stages.length - 1).asInstanceOf[DecisionTreeClassificationModel]
display(treeModelQ11)

// COMMAND ----------

//display(df_filtered_code.select("idclass", "codeSub"))


// COMMAND ----------

//val df = spark.read.parquet("/FileStore/tables/ml_data2").toDF()
//display(df)
val records = MLUtils.loadLibSVMFile(sc, "/FileStore/tables/ml_data2", 1).toDF
records.show(false)
/*
val data = df.select("idclass", "codeSub")

val featureIndexer = new VectorIndexer()
  .setInputCol("codeSub")
  .setOutputCol("index")
  .setMaxCategories(4)
  .fit(data)

val labelIndexer = new StringIndexer()
  .setInputCol("idclass")
  .setOutputCol("index")
  .fit(data)
*/

val df = spark.read.parquet("/FileStore/tables/ml_data2").toDF()

val data = df.select("idclass", "codeSub") 
data.

val assembler = new VectorAssembler().setInputCols(Array("codeSub")).setOutputCol("codeSub")
val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

val tree = new DecisionTreeClassifier()
  //.setLabelCol("idClass")
  //.setFeaturesCol("features")

val pipeline = new Pipeline().setStages(Array(tree))
val model = pipeline.fit(trainingData)


//val Array(trainingData, testData) = assembledData.randomSplit(Array(0.7, 0.3))

//val model = DecisionTree
