# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, trim, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType

if __name__ == '__main__':
    # ==========================================
    # 初始化 SparkSession（Spark 入口）
    # ==========================================
    spark = SparkSession.builder \ 
        .appName("Ecommerce_Data_Preprocess") \
        .master("local[*]") \
        .config("spark.sql.caseSensitive", "false") \
        .getOrCreate()

    # ==========================================
    # 步骤1：从HDFS读取原始电商用户行为数据
    # ==========================================
    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .csv("hdfs://localhost:9000/spark/ecommerce/input/user_behavior.csv")

    print("===== 步骤1完成：成功读取HDFS原始数据 =====")
    print(f"原始数据总量：{df.count()} 条")
    df.printSchema()
    df.show(5, truncate=False)

    # ==========================================
    # 步骤2：检测缺失值，完成数据完整性校验
    # ==========================================
    print("\n===== 步骤2：数据完整性校验（缺失值统计） =====")
    # 统计每列空值数量（分布式统计）
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        print(f"字段【{column}】缺失值数量：{null_count}")

    # 过滤空值（删除含缺失值的行，保证数据完整性）
    df_clean_null = df.dropna()
    print(f"删除缺失值后数据量：{df_clean_null.count()} 条")

    # ==========================================
    # 步骤3：过滤无效售价、非法行为类型等异常数据
    # ==========================================
    print("\n===== 步骤3：过滤异常数据 =====")
    # 合法行为类型（根据你的数据集定义：pv=浏览, buy=购买）
    valid_behavior = ["pv", "buy"]

    # 过滤规则：售价>0（无效售价） + 行为类型合法
    df_filter = df_clean_null.filter(
        (col("售价").cast(DoubleType()) > 0) &
        (col("行为类型").isin(valid_behavior))
    ) 
    print(f"过滤异常数据后总量：{df_filter.count()} 条")

    # ==========================================
    # 步骤4：Unix时间戳 → 标准日期时间格式
    # ==========================================
    print("\n===== 步骤4：时间戳格式转换 =====")
    df_time = df_filter.withColumn(
        "datetime",
        from_unixtime(col("时间戳"), "yyyy-MM-dd HH:mm:ss")  # 标准时间格式
    )
    df_time.show(5, truncate=False)

    # ==========================================
    # 步骤5：清洗商品名称：特殊字符 + 冗余空格
    # ==========================================
    print("\n===== 步骤5：清洗商品名称字段 =====")
    df_name = df_time.withColumn(
        "商品名称",
        # 1. trim()：去除首尾冗余空格 2. 正则替换：只保留中文、英文、数字
        regexp_replace(trim(col("商品名称")), "[^a-zA-Z0-9\u4e00-\u9fa5]", "")
    )
    df_name.select("商品名称").show(10, truncate=False)

    # ==========================================
    # 步骤6：基于业务规则 → 分布式数据去重
    # ==========================================
    print("\n===== 步骤6：分布式业务去重 =====")
    # 业务去重规则：同一用户 + 同一商品 + 同一行为 + 同一时间 → 重复
    df_dedup = df_name.dropDuplicates(["用户ID", "商品ID", "行为类型", "datetime"])
    print(f"去重后最终数据量：{df_dedup.count()} 条")
    print(f"删除重复数据：{df_name.count() - df_dedup.count()} 条")

    # ==========================================
    # 步骤7：预处理后数据回存至HDFS
    # ==========================================
    print("\n===== 步骤7：保存数据到HDFS =====")
    df_dedup.write \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .mode("overwrite") \
        .csv("hdfs://localhost:9000/spark/ecommerce/output/standard_da 
ta")

    print("===== 所有预处理步骤执行完成！ =====")
    spark.stop()
