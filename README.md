[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

# flink1.12.1 sql demo
 个人练习flink 的demo。
 
 代码里自带数据生成，配有合适的数据演示。
 
 pom.xml包含了flink绝大部分的依赖，maven下载会比较久。
 
 目前项目内容包含了flink streamset flinkSQL flinkCDC Mysql
 
# 环境
Windows 10 ltsc

Mysql8.0.23

Idea社区版2020.3

HaidiSQL

环境搭建教程：https://www.cnblogs.com/abramgyb/p/14587901.html

# streamset
 使用streamset api编写flink程序

# flink sql
 flink sql api编写程序，实现ETL功能，包含读写Mysql,kafka 
 
 source Mysql MysqlCDC
 
 sink Mysql

# 使用说明
 环境搭建教程：https://www.cnblogs.com/abramgyb/p/14587901.html

 依赖下载完成后运行demo测试
