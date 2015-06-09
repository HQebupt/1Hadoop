# HadoopBasis
Hadoop learning quick starts.

hadoop version：2.7.0

java version: 1.7

## 编译
- mvn package

target-jar包路径：./target/hadoop-basic-1.0-SNAPSHOT.jar (rename：wc.jar)

> maven 使用
> * idea自动导入依赖的jar包，修改一下pom.xml,然后点击右上角的auto import。
> * mvn compile|package

## WordCount2
### 简单的运行
Sample text-files as input:

- `bin/hadoop fs -ls /user/joe/wordcount/input/`

/user/joe/wordcount/input/file01
/user/joe/wordcount/input/file02

- `bin/hadoop fs -cat /user/joe/wordcount/input/file01`

Hello World, Bye World!

- `bin/hadoop fs -cat /user/joe/wordcount/input/file02`

Hello Hadoop, Goodbye to hadoop.


Sample text-files as input:

- `bin/hadoop jar wc.jar WordCount2 /user/joe/wordcount/input /user/joe/wordcount/output`

Output:

- `bin/hadoop fs -cat /user/joe/wordcount/output/part-r-00000`

> Bye 1
> Goodbye 1
> Hadoop, 1
> Hello 2
> World! 1
> World, 1
> hadoop. 1
> to 1

### 带有过滤器的运行
Pattern：

- `bin/hadoop fs -cat /user/joe/wordcount/patterns.txt`

> \.
> \,
> \!
> to

Run it again, this time with more options:

- `bin/hadoop jar wc.jar WordCount2 -Dwordcount.case.sensitive=true /user/joe/wordcount/input /user/joe/wordcount/output -skip /user/joe/wordcount/patterns.txt`

Run it once more, this time switch-off case-sensitivity:

- `bin/hadoop jar wc.jar WordCount2 -Dwordcount.case.sensitive=false /user/joe/wordcount/input /user/joe/wordcount/output -skip /user/joe/wordcount/patterns.txt`

Sure enough, the output:

- `bin/hadoop fs -cat /user/joe/wordcount/output/part-r-00000`

> bye 1
> goodbye 1
> hadoop 2
> hello 2
> horld 2
