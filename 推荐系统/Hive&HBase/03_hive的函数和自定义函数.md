## 4.3 Hive 函数

### 学习目标

- 了解Hive内置运算符和内置函数
- 记忆Hive自定义函数的使用方法

### 1 Hive内置运算符和内置函数

**在 Hive 有四种类型的运算符：**

- 关系运算符

- 算术运算符

- 逻辑运算符

- 复杂运算


**内置函数**

- 简单函数: 日期函数 字符串函数 类型转换 
- 统计函数：sum avg distinct
- 集合函数：size array_contains
- show functions  显示所有函数;
- desc function 函数名;
- desc function extended 函数名;

### 2 Hive 自定义函数

- **UDF**  和**UDAF** 

  - **UDF**： 用户自定义函数（user-defined function）
  - **UDAF**： 用户自定义聚合函数 (user-defined aggregation function)
  - 当 Hive 提供的内置函数无法满足你的业务处理需要时，此时就可以考虑使用UDF（或UDAF）
  - **UDF**相当于mapper，对每一条输入数据，映射为一条输出数据。
  - **UDAF**相当于reducer，做聚合操作，把一组输入数据映射为一条(或多条)输出数据。
- 一个脚本至于是做mapper还是做reducer，又或者是做UDF还是做UDAF，取决于我们把它放在什么样的hive操作符中。放在select中的基本就是UDF，放在distribute by和cluster by中的就是UDAF。
  
- UDF示例(运行java已经编写好的UDF)

  - 在hdfs中创建 /user/hive/lib目录

    ```shell
    hadoop fs -mkdir /user/hive/lib
    ```

  - 把 hive目录下 lib/hive-contrib-2.3.4.jar  放到hdfs中

    ```shell
    hadoop fs -put hive-contrib-2.3.4.jar /user/hive/lib/
    ```

  - 把集群中jar包的位置添加到hive中

    ```shell
    hive> add jar hdfs:///user/hive/lib/hive-contrib-2.3.4.jar ;
    ```

  - 在hive中创建**临时**UDF

    ```sql
    hive> CREATE TEMPORARY FUNCTION row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence'
    ```

  - 在之前的案例中使用**临时**自定义函数(函数功能: 添加自增长的行号)

    ```sql
    Select row_sequence(),* from employee;
    ```

  - 创建**非临时**自定义函数

    ```sql
    CREATE FUNCTION row_sequence as 'org.apache.hadoop.hive.contrib.udf.UDFRowSequence'
    using jar 'hdfs:///user/hive/lib/hive-contrib-2.3.4.jar';
    ```

- Python UDF

  - 准备案例环境

    - 创建表

      ```sql
      CREATE table u(fname STRING,lname STRING);
      ```

    - 向表中插入数据

      ```sql
      insert into table u2 values('George','washington');
      insert into table u2 values('George','bush');
      insert into table u2 values('Bill','clinton');
      insert into table u2 values('Bill','gates');
      ```

  - 编写map风格脚本

    ```python
    import sys
    for line in sys.stdin:
        line = line.strip()
        fname , lname = line.split('\t')
        l_name = lname.upper()
        print '\t'.join([fname, str(l_name)])
    ```

  - 通过hdfs向hive中ADD file

    - 加载文件到hdfs

      ```shell
      hadoop fs -put udf.py /user/hive/lib/
      ```

    - hive从hdfs中加载python脚本

      ```shell
      ADD FILE hdfs:///user/hive/lib/udf.py;
      ADD FILE /root/tmp/udf1.py;
      ```

  - Transform

    ```sql
    SELECT TRANSFORM(fname, lname) USING 'python udf1.py' AS (fname, l_name) FROM u;
    ```

### 总结

- Hive内置运算符和内置函数
- Hive自定义函数
  - 清楚UDF和UDAF的区别：mapper 和 reducer
  - 使用别人编写好的UDF(UDAF)
  - 使用python脚本实现UDF(UDAF)