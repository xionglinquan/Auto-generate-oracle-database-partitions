# Auto-generate-oracle-database-partitions
自动生成Oracle添加分区脚本，需安装相关包cx_Oracle、dateutil、pandas，运行环境win+cmd下

Usage:

1.python auto_generate_partitions.py dbuser dbpassword tns

2.输入1查看range类型分区表信息

3.输入2生成添加分区脚本

4.输入3退出程序

注意：分区命名必须为类似P201701
