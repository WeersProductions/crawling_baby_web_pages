from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

# Decision tree
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.regression import DecisionTreeRegressor, RandomForestRegressor
from pyspark.ml.feature import StringIndexer, VectorIndexer, IndexToString
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


class EvaluationResult:
    """
    Contains information about an evaluator that has ran and model.
    """

    def __init__(self, test_result, model, model_debug_string):
        self.test_result = test_result  # (test_name, test_value)
        self.model = model
        self.model_debug_string = model_debug_string

    def __str__(self):
        return "test_result: %s = %g \n model_summary: %s \n full_model: %s" % (
        self.test_result[0], self.test_result[1], self.model, self.model_debug_string)


class ModelResult:
    """
    Contains all information about a model.
    Data can be added (like feature_importance) and will be printed properly when converting to string.
    """

    def __init__(self, model_name):
        self.model_name = model_name

    def add_evaluation(self, evaluation):
        """
        Can be of type EvaluationResult
        """
        self.evaluation = evaluation

    def add_feature_importance(self, feature_importance):
        self.feature_importance = feature_importance

    def add_prediction_results_sample(self, prediction_results_sample):
        self.prediction_results_sample = prediction_results_sample

    def add_prediction_results(self, prediction_results):
        self.prediction_results = prediction_results

    def __str__(self):
        evaluation_string = "no evaluation"
        if self.evaluation is not None and len(self.evaluation) > 0:
            evaluation_string = [str(x) + "\n" for x in self.evaluation]
        feature_importance_string = "no feature importance"
        if self.feature_importance is not None:
            feature_importance_string = str(self.feature_importance)
        prediction_results_string = "no predictions"
        if self.prediction_results_sample is not None:
            prediction_results_string = str(self.prediction_results_sample)
        return "-- %s -- \n Evaluation: %s \n Feature importance: %s \n Prediction results: %s" % (
        self.model_name, evaluation_string, feature_importance_string, prediction_results_string)


def ExtractFeatureImp(featureImp, dataset, featuresCol):
    """
    From an index based importance list, to a feature label based importance list.
    """
    list_extract = []
    for i in dataset.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + dataset.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
    result = []
    for index, feature in enumerate(list_extract):
        result.append((feature, featureImp[index]))
    result.sort(key=lambda x: x[1])
    return result


def get_and_prepare_data(spark, original_label_col, label_col, feature_col_names, vector_col):
    feature_data = spark.read.parquet('WebInsight/analysis/result/labeled_07-13_09-14.parquet')
    feature_data = feature_data.withColumnRenamed(original_label_col, label_col)
    feature_data = feature_data.withColumn(label_col, col(label_col).cast("integer"))
    feature_data = feature_data.dropna()
    assembler = VectorAssembler(inputCols=feature_col_names, outputCol=vector_col)
    data = assembler.transform(feature_data).select(label_col, vector_col)

    return data


def get_pipeline(data, label_col, vector_col, model):
    """
    Creates a pipeline that converts labels and includes the model step. Also returns the index of the model step.

    Args:
        data ([type]): [description]
        label_col ([type]): [description]
        vector_col ([type]): [description]
        model ([type]): [description]

    Returns:
        tuple(<stages>, Number): (<stages>, <model_index>)
    """
    labelIndexer = StringIndexer(inputCol=label_col, outputCol="indexedLabel").fit(data)
    featureIndexer = VectorIndexer(inputCol=vector_col, outputCol="indexedFeatures", maxCategories=4).fit(data)
    # Convert indexed labels back to original labels.
    labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelIndexer.labels)

    return ([labelIndexer, featureIndexer, model, labelConverter], 2)


def evaluate(evaluator, predictions, vector_col, model, pipeline_model_index):
    test_value = evaluator.evaluate(predictions)
    test_name = evaluator.getMetricName()
    treeModel = model.stages[pipeline_model_index]
    return EvaluationResult((test_name, test_value), treeModel, treeModel.toDebugString)


def run_model(spark, original_label_col, label_col, vector_col, feature_col_names, model, evaluators):
    model_result = ModelResult(str(type(model)))

    data = get_and_prepare_data(spark, original_label_col, label_col, feature_col_names, vector_col)
    (trainingData, testData) = data.randomSplit([0.8, 0.2])

    stages, pipeline_model_idx = get_pipeline(data, label_col, vector_col, model)
    pipeline = Pipeline(stages=stages)
    model = pipeline.fit(trainingData)
    predictions = model.transform(testData)

    prediction_results_sample = predictions.select("predictedLabel", "label", "features").take(5)
    # prediction_results = predictions.select("label").groupBy("label").count().sort('count', ascending=False)
    prediction_results = predictions.select("label", "predictedLabel")
    evaluation_results = []
    for evaluator in evaluators:
        evaluation_results.append(evaluate(evaluator, predictions, vector_col, model, pipeline_model_idx))
    feature_importance = get_feature_importance(model.stages[pipeline_model_idx], predictions, vector_col)

    # Add to the model_result
    model_result.add_prediction_results_sample(prediction_results_sample)
    model_result.add_prediction_results(prediction_results)
    model_result.add_evaluation(evaluation_results)
    model_result.add_feature_importance(feature_importance)

    return model_result


def decision_tree_classifier(spark, original_label_col, feature_col_names):
    # Create two columns, 'label' and 'features'. Label is true or false, features is a vector of values.
    label_col = "label"
    vector_col = "features"

    dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")
    evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction",
                                                  metricName="accuracy")
    evaluator_w_precision = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction",
                                                  metricName="weightedPrecision")
    evaluator_w_recall = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction",
                                                  metricName="weightedRecall")
    return run_model(spark, original_label_col, label_col, vector_col, feature_col_names, dt, [evaluator, evaluator_w_precision, evaluator_w_recall])


def decision_tree_regressor(spark, original_label_col, feature_col_names):
    # Create two columns, 'label' and 'features'. Label is a number, features is a vector of values.
    label_col = "label"
    vector_col = "features"

    dt = DecisionTreeRegressor(labelCol="indexedLabel", featuresCol="indexedFeatures")
    evaluator_mae = RegressionEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="mae")
    evaluator_rmse = RegressionEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="r2")
    return run_model(spark, original_label_col, label_col, vector_col, feature_col_names, dt, [evaluator_mae, evaluator_r2, evaluator_rmse])


def random_forest_regressor(spark, original_label_col, feature_col_names):
    # Create two columns, 'label' and 'features'. Label is a number, features is a vector of values.
    label_col = "label"
    vector_col = "features"

    dt = RandomForestRegressor(labelCol="indexedLabel", featuresCol="indexedFeatures")
    evaluator_mae = RegressionEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="mae")
    evaluator_rmse = RegressionEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="r2")
    return run_model(spark, original_label_col, label_col, vector_col, feature_col_names, dt, [evaluator_mae, evaluator_r2, evaluator_rmse])


def get_feature_importance(model, prediction_df, feature_col):
    """
    Calculate the importance score of each feature.

    Args:
        model: trained model
        prediction_df: transformed dataframe based on the model
        feature_col: name of the column that contains vectors for each feature
    """
    feature_importances = model.featureImportances
    feature_importance_info = ExtractFeatureImp(feature_importances, prediction_df, feature_col)
    return feature_importance_info


if __name__ == "__main__":
    """
    Run using: spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=20 --conf spark.yarn.maxAppAttempts=1 analysis/regression.py

    Be sure to first run the 'analysis_pipeline.py' python script (this prepares the data that this regression relies on).
    """

    print("Starting analysis.")
    spark = SparkSession.builder.getOrCreate()

    # Train a model and print feature importance.
    features = ["fetch_textSize", "n_internalInLinks", "n_externalInLinks",
                "n_internalOutLinks", "n_externalOutLinks", "fetch_textQuality", "fetch_contentLength"]
    label = "history_changeCount"

    dt_regressor_result = decision_tree_regressor(spark, label, features)
    # regressor_result.evaluation.model.save("StackOverflow/analysis/regressor_saved.parquet")
    print(dt_regressor_result)

    rf_regressor_result = random_forest_regressor(spark, label, features)
    print(rf_regressor_result)
    # classifier_result = decision_tree_classifier(spark, label, features)
    # classifier_result.prediction_results.write.mode("overwrite").parquet("StackOverflow/analysis/classifier_prediction_results.parquet")
    # classifier_result.evaluation.model.save("StackOverflow/analysis/classifier_saved_training.parquet")
    # print(classifier_result)
