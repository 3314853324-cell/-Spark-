# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, trim, regexp_replace
from pyspark.sql.types import DoubleType

if __name__ == '__main__':
    # 1. 初始化 SparkSession
    spark = SparkSession.builder \
        .appName("Ecommerce_Data_Deduplication") \
        .master("local[*]") \
        .getOrCreate()

    # 步骤1：从HDFS读取原始数据
    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .csv("hdfs://localhost:9000/spark/ecommerce/input/user_behavior.csv")

    print("===== 步骤1完成：读取原始数据 =====")
    print(f"原始数据总条数：{df.count()}")
    df.printSchema()

    # 步骤2：检测缺失值
    print("\n===== 步骤2：缺失值检测 =====")
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        print(f"字段【{column}】缺失值：{null_count} 条")

    df_no_null = df.dropna()
    print(f"删除缺失值后数据量：{df_no_null.count()} 条")

    # 步骤3：过滤异常数据
    print("\n===== 步骤3：过滤异常数据 =====")
    valid_behavior = ["pv", "buy"]
    df_filtered = df_no_null.filter(
        (col("售价").cast(DoubleType()) > 0) &
        (col("行为类型").isin(valid_behavior)) 
    )
    print(f"过滤异常数据后数据量：{df_filtered.count()} 条")

    # 步骤4：时间戳转换
    print("\n===== 步骤4：时间格式转换 =====")
    df_time = df_filtered.withColumn("datetime", from_unixtime(col("时间戳"), "yyyy-MM-dd HH:mm:ss"))
    df_time.select("时间戳", "datetime").show(5, truncate=False)

    # 步骤5：清洗商品名称
    print("\n===== 步骤5：商品名称清洗 =====")
    df_clean_name = df_time.withColumn("商品名称",
                                       regexp_replace(trim(col("商品名称")), "[^a-zA-Z0-9\u4e00-\u9fa5]", ""))
    df_clean_name.select("商品名称").show(5, truncate=False)

    # 步骤6：分布式数据去重（核心）
    print("\n===== 步骤6：分布式业务去重 =====")
    before_dedup_count = df_clean_name.count()
    df_dedup = df_clean_name.dropDuplicates(["用户ID", "商品ID", "行为类型", "datetime"])
    after_dedup_count = df_dedup.count()
    print(f"去重前：{before_dedup_count} 条")
    print(f"去重后：{after_dedup_count} 条")
    print(f"删除重复：{before_dedup_count - after_dedup_count} 条")

    # 步骤7：保存到HDFS
    print("\n===== 步骤7：数据回存HDFS =====")
    df_dedup.write \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .mode("overwrite") \
        .csv("hdfs://localhost:9000/spark/ecommerce/output/standard_data")

    print("\n===== 全部步骤执行完成！=====")
    spark.stop()
