# AIDP notebook: batch scoring of latest feature rows. Load only an approved registry version.
from pyspark.sql import functions as F, Window
from pyspark.ml import PipelineModel
from src.settings import Settings
settings = Settings.from_env()
silver_table, predictions_table = settings.table("silver", "interval_reading"), settings.table("ml", "meter_predictions")
MODEL_VERSION = "approved-v1"; MODEL_URI = "/Workspace/models/smart_meter_next_kwh/approved-v1"
model = PipelineModel.load(MODEL_URI)
base = spark.table(silver_table).where("quality_code = 'ACTUAL'")
w = Window.partitionBy("meter_id").orderBy("interval_start_utc")
latest = (base.select("meter_id", "interval_start_utc", "temperature_c", "voltage_v", "tariff_code")
 .join(base.select("meter_id", "interval_start_utc", "consumption_kwh"), ["meter_id", "interval_start_utc"])
 .withColumn("lag_1_kwh", F.lag("consumption_kwh", 1).over(w)).withColumn("lag_4_kwh", F.lag("consumption_kwh", 4).over(w))
 .withColumn("lag_96_kwh", F.lag("consumption_kwh", 96).over(w)).withColumn("rolling_4_kwh", F.avg("consumption_kwh").over(w.rowsBetween(-4, -1)))
 .withColumn("hour", F.hour("interval_start_utc")).withColumn("day_of_week", F.dayofweek("interval_start_utc"))
 .withColumn("is_weekend", F.when(F.dayofweek("interval_start_utc").isin(1, 7), 1).otherwise(0))
 .where("lag_96_kwh IS NOT NULL").withColumn("rn", F.row_number().over(Window.partitionBy("meter_id").orderBy(F.col("interval_start_utc").desc()))).where("rn=1").drop("rn", "consumption_kwh"))
scored = model.transform(latest).select(F.lit(MODEL_VERSION).alias("model_version"), "meter_id", "interval_start_utc", F.greatest(F.lit(0.0), F.col("prediction")).alias("prediction_kwh"), F.current_timestamp().alias("scored_at"))
scored.createOrReplaceTempView("scored_batch")
spark.sql(f"MERGE INTO {predictions_table} t USING scored_batch s ON t.model_version=s.model_version AND t.meter_id=s.meter_id AND t.interval_start_utc=s.interval_start_utc WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
