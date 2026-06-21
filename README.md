# -Spark-
20233001546 陈朗
# Spark电商用户行为数据分析项目
## 项目介绍
基于Hadoop+HDFS、PySpark实现电商用户行为数据全流程处理，包含数据清洗、缺失值过滤、业务主键去重、多维度SQL查询、Matplotlib数据可视化。
## 环境依赖
- Ubuntu 20.04
- Hadoop 3.3.4
- Spark 3.5.0
- Python3 + matplotlib + pandas
## 文件说明
1. ecommerce_full_preprocess.py：数据预处理、去重、HDFS读写
2. data_query.py：Spark SQL多维度数据查询脚本
3. data_visualize.py：数据可视化绘图脚本
## 运行命令
spark-submit xxx.py
