### 实验目的

本实验旨在比较Spark SQL和Spark RDD在复杂查询中的性能差异基于 Spark RDD 实现TPC-H中复杂SQL（以Q3为例）

### 实验方法与设置

1、基于 Spark RDD 实现TPC-H中复杂SQL（以Q3为例）

2、与单机环境和 Spark SQL 进行性能对比。

3、优化Spark RDD任务的执行效率，分别从任务级别——Reduce、aggregateByKey、foldByKey、combineByKey算子；任务调参——并行度、缓存；运行参数——driver 和worker 参数配置，三个角度来展开。

### 分工

邓鹏：实验设计、配置环境、3、PPT

夏倍蓓：实验设计、1、2、PPT、汇报

吕晔：实验设计、1、3、PPT

赵伟珺：实验设计、1、3、PPT

### 结果

#### 1、基于 Spark RDD 实现TPC-H中复杂SQL（以Q3为例）

文件见附件

![image-20241224030808418](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030808418.png)

#### 2、与单机环境和 Spark SQL 进行性能对比。

![image-20241224030628422](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030628422.png)

![image-20241224030635614](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030635614.png)

![image-20241224030648826](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030648826.png)

![image-20241224030655613](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030655613.png)



![image-20241224030703929](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030703929.png)

![image-20241224030716588](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030716588.png)

#### 3、优化Spark RDD任务的执行效率

![image-20241224030849584](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030849584.png)

![image-20241224030900921](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030900921.png)

![image-20241224030926172](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030926172.png)

![image-20241224030935956](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030935956.png)

![image-20241224030944733](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030944733.png)

![image-20241224030954810](C:\Users\24579\AppData\Roaming\Typora\typora-user-images\image-20241224030954810.png)
