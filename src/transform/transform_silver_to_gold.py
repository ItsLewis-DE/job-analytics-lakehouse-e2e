import argparse
from datetime import datetime
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from src.utils.spark_util import get_spark_session

class GoldTransformer:
    def __init__(self, date: datetime):
        self.date = date
        self.spark = get_spark_session("transform_silver_to_gold")
        self.start_time = date.strftime("%Y-%m-%d 00:00:00")
        self.end_time = date.strftime("%Y-%m-%d 23:59:59")
        self.sources = ['itviec', 'vietnamworks']

        self.logger = logging.getLogger("gold_transformer")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def read_unified_silver(self) -> DataFrame:
        dfs = []
        for source in self.sources:
            table_name = f"my_catalog.silver.{source}_jobs"
            if self.spark.catalog.tableExists(table_name):
                df = self.spark.table(table_name)
                df_filtered = df.filter(
                    (F.col("ingested_at") >= self.start_time) &
                    (F.col("ingested_at") <= self.end_time)
                )
                if df_filtered.head(1):
                    dfs.append(df_filtered)
                    self.logger.info(f"Loaded data from {table_name}")
            else:
                self.logger.warning(f"Table {table_name} does not exist.")
        
        if not dfs:
            self.logger.warning("No data found in any Silver tables for the given date.")
            return None
            
        unified_df = dfs[0]
        for i in range(1, len(dfs)):
            unified_df = unified_df.unionByName(dfs[i], allowMissingColumns=True)
            
        return unified_df

    def process_dimensions(self, df: DataFrame):
        self.logger.info("Processing Dimension Tables...")

        # 1. dim_company
        dim_company = df.select(
            "company_id", "company_name", "company_industry", "company_size_std", 
            "company_address", "company_link"
        ).dropDuplicates(["company_id"]).filter(F.col("company_id").isNotNull())
        self._merge_dimension(dim_company, "my_catalog.gold.dim_company", "company_id")

        # 2. dim_location
        dim_location = df.select("location").dropDuplicates().filter(F.col("location").isNotNull())
        dim_location = dim_location.withColumnRenamed("location", "location_name")
        dim_location = dim_location.withColumn("location_id", F.md5(F.col("location_name")))
        self._merge_dimension(dim_location, "my_catalog.gold.dim_location", "location_id")

        # 3. dim_job_category
        dim_job_category = df.select("job_category").dropDuplicates()
        dim_job_category = dim_job_category.filter(F.col("job_category").isNotNull())
        dim_job_category = dim_job_category.withColumnRenamed("job_category", "job_category_name")
        dim_job_category = dim_job_category.withColumn("job_category_id", F.md5(F.col("job_category_name")))
        self._merge_dimension(dim_job_category, "my_catalog.gold.dim_job_category", "job_category_id")

        # 4. dim_skill
        skills_df = df.select(F.explode_outer("skills_array").alias("skill_name")).dropDuplicates()
        skills_df = skills_df.filter(F.col("skill_name").isNotNull() & (F.trim(F.col("skill_name")) != ""))
        dim_skill = skills_df.withColumn("skill_id", F.md5(F.col("skill_name")))
        self._merge_dimension(dim_skill, "my_catalog.gold.dim_skill", "skill_id")

    def _merge_dimension(self, dim_df: DataFrame, table_name: str, pk_col: str):
        if self.spark.catalog.tableExists(table_name):
            temp_view_name = f"updates_{table_name.split('.')[-1]}"
            dim_df.createOrReplaceTempView(temp_view_name)
            
            # Auto-generate UPDATE SET statement for Type 1 SCD
            cols = [c for c in dim_df.columns if c != pk_col]
            update_set = ", ".join([f"target.{c} = source.{c}" for c in cols])
            
            merge_query = f"""
            MERGE INTO {table_name} target
            USING {temp_view_name} source
            ON target.{pk_col} = source.{pk_col}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT *
            """
            self.spark.sql(merge_query)
        else:
            dim_df.writeTo(table_name).tableProperty("format-version", "2").create()
        self.logger.info(f"Merged {dim_df.count()} records into {table_name}")

    def process_fact_and_bridge(self, df: DataFrame):
        self.logger.info("Processing Fact and Bridge Tables...")

        # Construct Fact Table
        fact_df = df.withColumn("location_id", F.md5(F.col("location"))) \
                    .withColumn("job_category_id", F.md5(F.col("job_category")))
                    
        fact_cols = [
            "job_id", "source", "job_url", "job_title",
            "company_id", "location_id", "job_category_id",
            "experience_req", "education", "working_type_std", 
            "working_day", "working_hour", 
            "salary_min", "salary_max", "salary_currency", "salary_band", 
            "deadline_date", "inserted_at", "ingested_at"
        ]
        
        # Ensure columns exist before selecting
        existing_cols = [c for c in fact_cols if c in fact_df.columns]
        fact_final = fact_df.select(*existing_cols)
        
        fact_table_name = "my_catalog.gold.fact_job_postings"
        if self.spark.catalog.tableExists(fact_table_name):
            fact_final.createOrReplaceTempView("updates_fact")
            # For facts, we update if job_id matches
            cols = [c for c in existing_cols if c != "job_id"]
            update_set = ", ".join([f"target.{c} = COALESCE(source.{c}, target.{c})" for c in cols])
            
            merge_query = f"""
            MERGE INTO {fact_table_name} target
            USING updates_fact source
            ON target.job_id = source.job_id
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT *
            """
            self.spark.sql(merge_query)
        else:
            # Partition fact by ingested_at
            fact_final.writeTo(fact_table_name) \
                .tableProperty("format-version", "2") \
                .partitionedBy(F.days("ingested_at")) \
                .create()
        self.logger.info(f"Merged records into {fact_table_name}")

        # Process Bridge Table
        bridge_df = df.select("job_id", F.explode_outer("skills_array").alias("skill_name"))
        bridge_df = bridge_df.filter(F.col("skill_name").isNotNull() & (F.trim(F.col("skill_name")) != ""))
        
        bridge_df = bridge_df.withColumn("skill_id", F.md5(F.col("skill_name")))
                             
        bridge_final = bridge_df.select("job_id", "skill_id").dropDuplicates()
        # Add a unique ID for merge
        bridge_final = bridge_final.withColumn("bridge_id", F.md5(F.concat(F.col("job_id"), F.col("skill_id"))))

        bridge_table_name = "my_catalog.gold.bridge_job_skills"
        if self.spark.catalog.tableExists(bridge_table_name):
            bridge_final.createOrReplaceTempView("updates_bridge")
            merge_query = f"""
            MERGE INTO {bridge_table_name} target
            USING updates_bridge source
            ON target.bridge_id = source.bridge_id
            WHEN NOT MATCHED THEN INSERT *
            """
            self.spark.sql(merge_query)
        else:
            bridge_final.writeTo(bridge_table_name).tableProperty("format-version", "2").create()
        self.logger.info(f"Merged records into {bridge_table_name}")

    def run(self):
        self.logger.info("=" * 60)
        self.logger.info(f"STARTING GOLD TRANSFORMATION FOR DATE: {self.date.strftime('%Y-%m-%d')}")
        self.logger.info("=" * 60)
        try:
            df_unified = self.read_unified_silver()
            if df_unified:
                self.process_dimensions(df_unified)
                self.process_fact_and_bridge(df_unified)
                self.logger.info("Successfully completed Gold transformation!")
        except Exception as e:
            self.logger.error(f"Error during Gold transformation: {e}", exc_info=True)
            raise
        finally:
            self.spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Transform data from Silver to Gold Layer")
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Date to process, format YYYY-MM-DD (default: today)"
    )
    args = parser.parse_args()
    date_obj = datetime.strptime(args.date, "%Y-%m-%d")

    transformer = GoldTransformer(date=date_obj)
    transformer.run()

if __name__ == "__main__":
    main()
