from pyspark.sql.functions import col, isnan
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression,  DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline

def initialize_spark():
    return SparkSession.builder\
        .master("local")\
        .appName("CS643_Wine_Quality_Predictions_Project")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2")\
        .getOrCreate()

def configure_spark_for_s3(spark):
    spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key","ASIARNFNO5U42XKSFTAB")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key","yIv1SEeUsNJ9DBcHd8VYU+A7TLDaGYBAk9b1SxsX")
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

def check_null_or_nan_values(dataframe):
    null_counts = []
    for col_name in dataframe.columns:
        null_count = dataframe.filter(col(col_name).isNull() | isnan(col(col_name))).count()
        null_counts.append((col_name, null_count))

    for col_name, null_count in null_counts:
        print(f"Column '{col_name}' has {null_count} null or NaN values.")

def split_data(dataframe, test_size=0.3, seed=42):
    return dataframe.randomSplit([1 - test_size, test_size], seed=seed)

def build_pipeline(classifier):
    assembler = VectorAssembler(
        inputCols=['fixed_acidity', 'volatile_acidity', 'citric_acid', 'residual_sugar', 'chlorides',
                    'free_sulfur_dioxide', 'total_sulfur_dioxide', 'density', 'pH', 'sulphates', 'alcohol'],
        outputCol="inputFeatures")

    scaler = StandardScaler(inputCol="inputFeatures", outputCol="features")

    return Pipeline(stages=[assembler, scaler, classifier])

def train_and_evaluate_model(pipeline, train_data, test_data):
    model = pipeline.fit(train_data)
    predictions = model.transform(test_data)
    evaluator = MulticlassClassificationEvaluator(metricName="f1")
    f1_score = evaluator.evaluate(predictions)
    return f1_score

def save_model_to_s3(model, model_path):
    model.save(model_path)

if __name__ == "__main__":
    # Initialize Spark session
    spark = initialize_spark()

    # Configure Spark for AWS S3
    configure_spark_for_s3(spark)

    # Load training and validation datasets from S3
    df = load_data_from_s3(spark, "s3a://cldassign2/TrainingDataset.csv")
    validation_df = load_data_from_s3(spark, "s3a://cldassign2/ValidationDataset.csv")

    # Check if data is loaded successfully
    if df.count() > 0 and validation_df.count() > 0:
        print("Data loaded successfully")
    else:
        print("Something unexpected happened during data load")

    # Rename columns for consistency
    new_column_names = {
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

    df = rename_columns(df, new_column_names)
    validation_df = rename_columns(validation_df, new_column_names)

    # Display column names
    print(df.columns)
    print(validation_df.columns)

    # Check for null or NaN values in columns
    check_null_or_nan_values(df)

    # Split the dataset into training and testing sets
    df, test_df = split_data(df)

    # Define data preprocessing and modeling pipelines
    lr_pipeline = build_pipeline(LogisticRegression())
    dt_pipeline = build_pipeline(DecisionTreeClassifier(labelCol="label", featuresCol="features", seed=0))

    # Train and evaluate models
    f1_lr = train_and_evaluate_model(lr_pipeline, df, test_df)
    f1_dt = train_and_evaluate_model(dt_pipeline, df, test_df)

    # Print F1 scores
    print("F1 Score for LogisticRegression Model:", f1_lr)
    print("F1 Score for DecisionTreeClassifier Model:", f1_dt)

    # Save models to S3
    save_model_to_s3(lr_pipeline, "s3a://cldassign2/LogisticRegression")
    save_model_to_s3(dt_pipeline, "s3a://cldassign2/DecisionTreeClassifier")

    # Stop the Spark session
    spark.stop()
