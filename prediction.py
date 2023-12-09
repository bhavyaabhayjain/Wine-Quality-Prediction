from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql.session import SparkSession

def initialize_spark():
    return SparkSession.builder\
        .appName("CS643_Wine_Quality_Predictions_Project")\
        .getOrCreate()

def configure_spark_for_s3(spark):
    spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")

def load_data_from_s3(spark, file_path):
    return spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .option("sep", ";")\
        .load(file_path)

def rename_columns(dataframe, column_mapping):
    for current_name, new_name in column_mapping.items():
        dataframe = dataframe.withColumnRenamed(current_name, new_name)
    return dataframe

def load_and_evaluate_model(model_path, model_type, dataframe, evaluator_metric="f1"):
    model = CrossValidatorModel.load(model_path)
    evaluator = MulticlassClassificationEvaluator(metricName=evaluator_metric)
    result = evaluator.evaluate(model.transform(dataframe))
    print(f"{evaluator_metric} Score for {model_type} Model: {result}")
    return result

if __name__ == "__main__":
    # Initialize Spark session
    spark = initialize_spark()

    # Configure Spark for AWS S3
    configure_spark_for_s3(spark)

    # Load validation dataset from S3
    validation_df = load_data_from_s3(spark, "s3a://cldassign2/ValidationDataset.csv")

    # Check if data loaded successfully
    if validation_df.count() > 0:
        print("Data loaded successfully")
    else:
        print("Something unexpected happened during data load")

    # Rename columns for better readability
    column_names_mapping = {
        '"""""fixed acidity""""': 'fixed_acidity',
        '"""fixed acidity""""': 'fixed_acidity',
        '""""volatile acidity""""': 'volatile_acidity',
        '""""citric acid""""': 'citric_acid',
        '""""residual sugar""""': 'residual_sugar',
        '""""chlorides""""': 'chlorides',
        '""""free sulfur dioxide""""': 'free_sulfur_dioxide',
        '""""total sulfur dioxide""""': 'total_sulfur_dioxide',
        '""""density""""': 'density',
        '""""pH""""': 'pH',
        '""""sulphates""""': 'sulphates',
        '""""alcohol""""': 'alcohol',
        '""""quality"""""': 'label'
    }

    # Rename columns in the DataFrame
    validation_df = rename_columns(validation_df, column_names_mapping)

    # Load and evaluate Logistic Regression model
    load_and_evaluate_model('s3a://cldassign2/LogisticRegression', 'LogisticRegression', validation_df)

    # Load and evaluate Decision Tree Classifier model
    load_and_evaluate_model('s3a://cldassign2/DecisionTreeClassifier', 'DecisionTreeClassifier', validation_df)
