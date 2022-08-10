# -*- coding: utf-8 -*-
"""
Created on Wed Aug  10 10:28:00 2022

@author: Sandeep Kushwaha
"""

# importing necessary libraries
import sys
import os
os.environ["PYSPARK_PYTHON"]="/anaconda/envs/UCBExtension/bin/python"
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import when
import pyspark.sql.utils





class Hello_fresh_recipe():
    #"Wrapper class for Log4j JVM object."
    
    def __init__(self, spark):
        
        # get spark app details with which to prefix all messages
                
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')
        
                  
        spark.conf.set("spark.executor.memory", "16g")
            
        spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        
        #handling logs        
        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)
        
        # function handling the log message
        def info(self, message):
            """Log information.
            :param: Information message to write to log
            :return: None
            """
            self.logger.info(message)
            return None

def main():
        
        try:
            
            # Createing variable for input data   
            input_data1 = sys.argv[1]
            input_data2 = sys.argv[2]
            input_data3 = sys.argv[3]
            output_data1 = sys.argv[4]
            output_data2 = sys.argv[5]
            
            
            #defining the saprk session
            spark = SparkSession.builder.master("local").appName("hello_fresh.com").getOrCreate()
                    
            # Load up cooking data as dataframe
            ingredients1 = spark.read.option("header", "true").option("inferSchema", "true").option("fetchsize","100000").option("batchsize","100000").json(input_data1)
            ingredients2 = spark.read.option("header", "true").option("inferSchema", "true").option("fetchsize","100000").option("batchsize","100000").json(input_data2)
            ingredients3 = spark.read.option("header", "true").option("inferSchema", "true").option("fetchsize","100000").option("batchsize","100000").json(input_data3)
    
            # Remove duplicate records using union
            df_ingredients = ingredients1.union(ingredients2)
            df_recipe = df_ingredients.union(ingredients3)
            
            df_recipe.show()
            
            # Extracting only recipes that have beef as one of the ingredients
            beef_ingredients = df_recipe.where(col('ingredients').like("%beef%"))
            
            #display the result of only recipes that have beef as one of the ingredients
            beef_ingredients.show()
            
            #Converting the "cookTime" & "prepTime" columns values into seconds
            df_cook_Time_sec = df_recipe.withColumn(
                'cookTime',F.coalesce(F.regexp_extract('cookTime', r'(\d+)H', 1).cast('int'), F.lit(0)) * 3600 +
                 F.coalesce(F.regexp_extract('cookTime', r'(\d+)M', 1).cast('int'), F.lit(0)) * 60 + 
                 F.coalesce(F.regexp_extract('cookTime', r'(\d+)S', 1).cast('int'), F.lit(0))
                 )
            
            df_prepcook_time_sec = df_cook_Time_sec.withColumn(
                'prepTime',
                F.coalesce(F.regexp_extract('prepTime', r'(\d+)H', 1).cast('int'), F.lit(0)) * 3600 + 
                F.coalesce(F.regexp_extract('prepTime', r'(\d+)M', 1).cast('int'), F.lit(0)) * 60 + 
                F.coalesce(F.regexp_extract('prepTime', r'(\d+)S', 1).cast('int'), F.lit(0))
                )
            
            # Creating a dataframe with "cookTime"  & "prepTime" fields
            df_cook_prep_time = df_prepcook_time_sec.select("cookTime", "prepTime")
            
            # Adding "cookTime" & "prepTime" and creating a new field "total_cook_time" 
            df_sum_cook_prep_time=df_cook_prep_time.select(((col("cookTime") + col("prepTime"))).alias("total_cook_time"),col("cookTime"),col("prepTime"))
            
            # Converting "total_cook_time" field values into mintues    
            df_total_cooktime_min = df_sum_cook_prep_time.withColumn("total_cook_time", F.round((F.col("total_cook_time")/60),2))
            
            # Casting total_cooktime_min into Int
            df_total_cooktime = df_total_cooktime_min.select(col("total_cook_time").cast('int').alias("total_cook_time"),col("cookTime"),col("prepTime"))
            
            # Selecting the desired fields "total_cook_time" and "difficulity"
            df_cooktime_difficulity = df_total_cooktime.select(col("total_cook_time").alias("difficulity"),col("total_cook_time"))
            
            # Defining the difficulity levels
            df_difficulity_level = df_cooktime_difficulity.withColumn("difficulity",
                                                                      when((col("difficulity") < 30) , "Easy")
                                                                      .when((col("difficulity") >= 30) & (col("difficulity") <= 60), "Medium")
                                                                      .when((col("difficulity") > 60) , "Hard")
                                                                      .otherwise("Zero_difficulity"))
            
            # displaying difficulity level and total_cooking_time records
            df_difficulity_level.show()
            
            # Calculating average cooking time duration per difficulty level
            df_avg_cooking_time = df_difficulity_level.groupBy("difficulity").avg("total_cook_time")
            df_avg_cooking_time.show()
            
            #Writing output to desired location  
            beef_ingredients.write.csv(output_data1, mode = "overwrite")
            df_avg_cooking_time.write.csv(output_data2, mode = "overwrite")
            
           
        #handling the exceptions    
        except pyspark.sql.utils.AnalysisException:
            print("Unable to process the data!")
            
            spark.stop()
            
if __name__ == "__main__":
    main() 
