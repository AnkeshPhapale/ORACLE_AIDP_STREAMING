# AIDP notebook: feature engineering and next-interval kWh model training.
from datetime import datetime, timezone
from pyspark.sql import functions as F, Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from src.settings import Settings

settings = Settings.from_env(); spark.sql(f"USE {settings.database}")
base = spark.table("silver_interval_reading").where("quality_code = 'ACTUAL'")
w = Window.partitionBy("meter_id").orderBy("interval_start_utc")
features = (base.select("meter_id", "interval_start_utc", "reading_date", "consumption_kwh", "temperature_c", "voltage_v", "tariff_code")
 .withColumn("target_next_kwh", F.lead("consumption_kwh", 1).over(w)).withColumn("lag_1_kwh", F.lag("consumption_kwh", 1).over(w))
 .withColumn("lag_4_kwh", F.lag("consumption_kwh", 4).over(w)).withColumn("lag_96_kwh", F.lag("consumption_kwh", 96).over(w))
 .withColumn("rolling_4_kwh", F.avg("consumption_kwh").over(w.rowsBetween(-4, -1)))
 .withColumn("hour", F.hour("interval_start_utc")).withColumn("day_of_week", F.dayofweek("interval_start_utc"))
 .withColumn("is_weekend", F.when(F.dayofweek("interval_start_utc").isin(1,7), 1).otherwise(0)).drop("consumption_kwh")
 .dropna().withColumn("ts_epoch", F.col("interval_start_utc").cast("long")))
features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("ml_meter_features")
cutoff = features.approxQuantile("ts_epoch", [0.80], 0.01)[0]
train, test = features.where(F.col("ts_epoch") < cutoff), features.where(F.col("ts_epoch") >= cutoff)
numeric = ["lag_1_kwh", "lag_4_kwh", "lag_96_kwh", "rolling_4_kwh", "hour", "day_of_week", "is_weekend", "temperature_c", "voltage_v"]
pipeline = Pipeline(stages=[StringIndexer(inputCol="tariff_code", outputCol="tariff_idx", handleInvalid="keep"), VectorAssembler(inputCols=numeric+["tariff_idx"], outputCol="features"), GBTRegressor(labelCol="target_next_kwh", maxIter=75, maxDepth=6, seed=42)])
model = pipeline.fit(train); predictions = model.transform(test)
metrics = {m: RegressionEvaluator(labelCol="target_next_kwh", predictionCol="prediction", metricName=m).evaluate(predictions) for m in ["mae", "rmse"]}
run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
model_path = f"/Workspace/models/smart_meter_next_kwh/{run_id}"
model.write().overwrite().save(model_path)
print(f"Saved candidate model to {model_path}; validation metrics={metrics}. Register/promote it through your governed model registry.")
