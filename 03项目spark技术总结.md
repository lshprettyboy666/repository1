# 001 PySpark笔记

今日内容:

- 1- Spark的基本内容 (了解 其中关于特点需要记录)
- 2- Spark的环境搭建(参考部署文档, 一步步配置成功即可)
- 3- 基于pycharm完成PySpark的入门案例(掌握-- 理解每一个API有什么作用)

## 1. Spark的基本内容

### 1.1. Spark的基本介绍

- mapreduce计算引擎

  - 大数据分布式计算引擎，对离线数据进行处理和分析

    ```properties
    1 计算慢 计算过程中需要不断读写磁盘，产生大量的io操作
    2 对于迭代支持不好
    3 MR偏底层，开发难度比较大
    ```

- 市场期望出现，计算效率高，对迭代计算支持比较好，易于上手的产品

- Spark基于内存的计算引擎，加州伯克利大学教授发表论文，基于论文进行开发的。RDD弹性分布式数据集，是spark的核心

- spark的思想：分而治之的思想，把大的任务拆分成若干个小任务，解决后再进行合并

- spark是计算引擎，取代mapreduce计算引擎

- spark是使用Scala语言开发，

  - java
    - xxx.java源代码
    - xxx.class字节码文件
    - xxx.jar文件
    - jvm（java virtual machine）把jar中class文件翻译成cpu可以执行的指令
  - scala
    - xxx.scala
    - xxx.class
  - spark提供了python的接口，pyspark进行开发
  - 目前是apache的顶级项目，[Apache Spark™ - Unified Engine for large-scale data analytics](https://spark.apache.org/)

  spark执行效率高的原因

  ```properties
  1 提供新的数据结构（编程模型）RDD使得程序员从原来的数据操作者变成规则定义者，规则定义完成后，所有的操作都是spark内部帮我们完成的。这些所有的计算都是基于RDD，有RDD后，迭代更加方法，可以基于内存进行运算
  2 spark基于线程，mr基于进程，线程调度比进程的调度快。
  ```

  面试题：hadoop基于进程计算和spark基于线程计算的优缺点

  ```
  hadoop的mapreduce是基于进程，进程的好处是每个进程独享资源，任务之间不方便共享数据，导致执行效率低，多个任务都读取同一份数据，这个数据就被加载多次。基于线程是为了数据共享，和提高执行效率，会产生资源竞争。
  ```

  

### 1.2. Spark的发展史

![image-20220523101413177](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220523101413177.png)

![image-20220523102410694](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220523102410694.png)

```properties
pyspark本质上是一个python库，spark官方提供的python操作spark的接口，需要安装pyspark

pip install xxx -i https://xxxx
```



### 1.3. Spark的特点

函数式编程范式

```
f(x) = 3x^2+5x-10
f(x) 结果只取决于x

def add_one(num):
    print(time.datetime())
    return num+1
```

1 速度快

```properties
原因1：spark基于内存，RDD数据结构，也可以基于磁盘进行运算，可以提供更加方便迭代计算，结果可以保存在内存中也可以存在磁盘中
原因2：spark基于线程，方便数据共享，线程启动 销毁 切换消耗的资源更少
```

2 易用性

```properties
1 提供多种编程语言的api：scala java python r sql
2 提供更加高级的api：很多功能都定义好，比如：转换，遍历，排序，不同编程语言函数名字非常接近，便于阅读其他语言编写的代码
```

3  通用性

```properties
spark提供了多种组件，可以应对不同的场景

SparkCore spark的核心：学习spark的基础，其中最为重要的就是RDD
会了解各种语言的客户端，使用python语言操作RDD，操作的接口封装在sparkCore中

SparkSQL ：DataFrame，可以使用sql语言进行数据分析处理，DSL语言（domain specified language），都是把代码翻译成对rdd的操作，从而执行

Spark Streaming：进行流式处理，现在官方已经放弃，仅做了解

Machine Learning：MLlib 线性回归 逻辑回归 聚类算法 

GraphX：图计算

Structrued Streaming ：结构化流，进行流式处理
```

4 随处运行

```properties
spark可以在不同资源调度平台进行运行：local模式，Spark集群模式，yarn，mesos，可以在云服务中运行
spark和Hadoop大数据生态集成很完善了

```



## 2. Spark的环境安装

### 2.0 修改虚拟机网络配置

目标网络信息:

```properties
1- 了解网段 :  ip中第三段  目前是 192.168.88   (所有虚拟机必须都是此网段下)
		查看虚拟机的ip地址即可: ifconfig
2- 了解此网段下的网关地址:  需要到虚拟机中查看网关(所有的服务器必须指向同一个网关)
		网关地址查看文件位置: /etc/sysconfig/network-scripts/ifcfg-ens33
		查看此文件
			vim /etc/sysconfig/network-scripts/ifcfg-ens33

```

![image-20220523114038102](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220523114038102.png)

修改外部的网络: 

- 1- 修改VMware的网络编辑器: 

![image-20220523114157143](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220523114157143.png)

![image-20220523114222788](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220523114222788.png)

![image-20220523114323233](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220523114323233.png)

- 2- 修改windows的网络适配器

![image-20220523114453457](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220523114453457.png)

![image-20220523114618071](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220523114618071.png)



- 3- 即可在fineShell  或者  CRT 或者其他各种连接工具进行连接操作:  
- 4- 连接后, 请测试网络是否畅通

```properties
ping  www.baidu.com

```



### 2.1. Local模式安装

local模式一般用于本地的开发测试，不能用做生产环境

本质上就是jvm（java virtual machine）的进程，进程中可以运行多个线程

local模式是单机模式，仅能够处理小数据集，无法处理大数据集

![image-20230517111805004](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/assets/image-20230517111805004.png)

**每位同学都需要配置以下映射**

![image-20230517141753258](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/assets/image-20230517141753258.png)

### 2.2. PySpark库安装

Spark和PySpark的关系：Spark是框架，提供了各种不同语言的接口，pyspark是其提供的python语言的接口

```
/export/server/spark/bin/pyspark 提供了交互式环境，便于进行验证的工作，查看函数使用方式
和
pyspark-3.1.2.tar.gz python编程语言的具体接口，把这个包安装后，可以在pycharm中使用python语言操作spark


```

安装完成anaconda后进行以下操作

```
1 conda config --set auto_activate_base false
2 修改/root/.bashrc
  添加以下内容：
  export PATH="/root/anaconda3/bin:$PATH"
3 source /root/.bashrc或者重新打开一个终端

```

在命令行中启动pyspark

```
cd /export/server/spark/bin
./pyspark 即可启动交互式pyspark界面

```



扩展: anaconda的常用命令

```properties
安装库: 
	conda install  包名
	pip install -i 镜像地址 包名
	pip install -i https://mirrors.ustc.edu.cn/pypi/web/simple pyspark
卸载库: 
	conda uninstall 包名
	pip uninstall 包名

设置 anaconda下载的库的镜像地址:  
	conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/free/
	conda config --set show_channel_urls yes

如何使用anaconda构建虚拟(沙箱)环境:

1- 查看当前有那些虚拟环境: 
conda env list

2- 如何创建一个新的虚拟环境
conda create 虚拟环境名称  python=版本号

例如: 创建一个pyspark_env 虚拟环境
conda create -n pyspark_env  python=3.8
例如: 创建一个itcast 虚拟环境
conda create -n itcast python=3.11

3- 如何进入虚拟环境(激活)
conda activate pyspark_env
或者
source activate pyspark_env 


4- 如何退出虚拟环境:
conda deactivate
或者
deactivate pyspark_env 


conda主要作用是创建和管理虚拟环境（沙箱sandbox环境）
pip安装卸载python的包

```



安装pyspark(python接口，pyspark包)

```
1 方式一
pip install -i https://mirrors.ustc.edu.cn/pypi/web/simple pyspark==3.1.2
2 方式二：
1) 手动把pyspark-3.1.2.tar.gz上传到/export/software目录中
2) cd /export/software
3) pip install -i https://mirrors.ustc.edu.cn/pypi/web/simple pyspark-3.1.2.tar.gz 

```



注意：每个节点都需要安装python环境（anaconda），但是pyspark库只需要在node1中安装即可。spark框架本身是集成pyspark环境的。便于在pycharm中编写代码，测试等使用。

如果直接使用后续的快照，所有环境都是安装完成。但是pyspark版本有点错误，需要重新安装一遍pyspark，安装以上安装方式进行更新pyspark。

```
pip卸载包
pip uninstall xxxx
pip安装
pip install -i https://mirrors.ustc.edu.cn/pypi/web/simple 包名==版本号

```



### 2.3 Spark集群模式架构

![spark集群架构模式](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/assets/spark%E9%9B%86%E7%BE%A4%E6%9E%B6%E6%9E%84%E6%A8%A1%E5%BC%8F.png)



## 3. 基于pycharm完成PySpark入门案例

![image-20230517170007923](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/assets/image-20230517170007923.png)

### 3.0 如何清理远端环境

![image-20230517170033882](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/assets/image-20230517170033882.png)

![image-20230517170119621](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/assets/image-20230517170119621.png)

![image-20230517170159877](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/assets/image-20230517170159877.png)

接下来, 还需要清理远端地址:

![image-20230517170230497](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/assets/image-20230517170230497.png)

![image-20230517170301613](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/assets/image-20230517170301613.png)



清理后, 重新配置当前项目使用远端环境: 





### 3.1 pycharm如何连接远程环境

背景说明:

```properties
	一般在企业中, 会存在两套线上环境, 一套环境是用于开发(测试)环境, 一套环境是用于生产环境, 首先一般都是先在开发测试环境上进行编写代码, 并且在此环境上进行测试, 当整个项目全部开发完成后, 需要将其上传到生产环境, 面向用于使用
	
	如果说还是按照之前的本地模式开发方案, 每个人的环境有可能都不一致, 导致整个团队无法统一一套开发环境进行使用, 从而导致后续在进行测试 上线的时候, 出现各种各样环境问题
	
	pycharm提供了一些解决方案: 远程连接方案, 允许所有的程序员都去连接远端的测试环境的, 确保大家的环境都是统一, 避免各种环境问题发生, 而且由于连接的远程环境, 所有在pycharm编写代码, 会自动上传到远端环境中, 在执行代码的时候, 相当于是直接在远端环境上进行执行操作

```

操作实现: 本次这里配置远端环境, 指的连接虚拟机中虚拟环境, 可以配置为 base环境, 也可以配置为 pyspark_env虚拟环境, 但是建议配置为 base环境, 因为base环境自带python包更全面一些

![image-20211106161942329](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211106161942329.png)

项目名为: zz17_pyspark_parent  (强烈建议与我项目名一致)

![image-20211106162224194](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211106162224194.png)



![image-20211106155834393](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211106155834393.png)

![image-20211106155927905](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211106155927905.png)

![image-20211106162610727](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211106162610727.png)

![image-20211203165949987](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211203165949987.png)

创建项目后, 设置自动上传操作

![image-20211106163027500](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211106163027500.png)

校验是否有pyspark



![image-20211106163226670](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211106163226670.png)



ok 后, 就可以在项目上创建子项目进行干活了: 最终项目效果图



最后, 就可以在 main中编写今日代码了, 比如WordCount代码即可



------

扩展: 关于pycharm 专业版 高级功能

- 1- 直接连接远端虚拟机, 进行文件上传, 下载 查看等等操作

![image-20211203170639696](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211203170639696.png)

![image-20211203171031827](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211203171031827.png)

- 2- 可以模拟shell控制台:

![image-20211203171106626](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211203171106626.png)

![image-20211203171158038](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211203171158038.png)

![image-20211203171222021](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211203171222021.png)



- 3- 模拟datagrip操作:

![image-20211203171303185](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/001_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211203171303185.png)

### 3.2 WordCount代码实现_local

#### 3.2.1 WordCount案例流程实现

![词频统计.drawio](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/assets/%E8%AF%8D%E9%A2%91%E7%BB%9F%E8%AE%A1.drawio.png)

#### 3.2.2 代码实现

可能出现的错误

![image-20230517175253424](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/assets/image-20230517175253424.png)

![image-20230517175334979](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/001/2%20%E7%AC%94%E8%AE%B0/assets/image-20230517175334979.png)

解决方案

```python
# 添加以下代码即可
import os

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

```

```python
from pyspark import SparkContext, SparkConf

import os

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'


if __name__ == '__main__':
    # 1 创建spark运行环境
    conf = SparkConf().setAppName('wordCount').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # 2 对数据进行处理
    # sc.textFile('D:/project/bigdata/zz17/001/data/word.txt') # 错误路径
    # sc.textFile('/tmp/pycharm_project_956/001/data/word.txt') # 如果路径中没有加任何协议，默认是到hdfs中读取
    # 2.1 读取数据
    rdd_file = sc.textFile('file:///tmp/pycharm_project_956/001/data/word.txt') # 如果路径中加协议file://，是到本地路径中读取
    # 2.2 处理数据：分割数据，转成一个大列表
    rdd_flat = rdd_file.flatMap(lambda line: line.split())
    # ['kafka spark spark', 'hadoop hadoop flume']
    # [[kafka, spark, spark], [hadoop, hadoop, flume]]
    # [kafka, spark, spark, hadoop, hadoop, flume]
    # print(rdd_flat.collect())
    # 2.3 处理数据：把单词转成元组
    rdd_tuple = rdd_flat.map(lambda word: (word, 1))
    # print(rdd_tuple.collect())
    # ('hello', 1), ('you', 1), ('Spark', 1), ('hello', 1), ('hello', 1)]
    # 3 词频统计
    result = rdd_tuple.reduceByKey(lambda agg, curr: agg+curr)
    # agg = 0
    # curr = 1
    # agg = curr+agg = 1
    # agg = 1
    # curr = 1
    # agg = agg + curr = 2
    # agg = 2
    # curr = 1
    # agg = agg + curr = 3
    print(result)

    # 4 调用action操作
    # action操作之前的操作称为transform操作，都是惰性
    print(result.collect())

```

# 002 PySpark笔记

今日内容:

- 1- spark的入门案例
- 2- Spark on yarn环境构建
- 3- Spark程序和PySpark交互流程
- 4- Spark-submit相关参数说明

## 1. pyspark入门案例

### 1.1 从HDFS上读取文件并实现排序

1. 启动hdfs

   ```
   start-dfs.sh
   或者
   start-all.sh
   
   ```

2. 确定启动成功

   ```
   jps检查进程
   访问web ui方式
   ```

3. 把待处理文件上传到hdfs

   ```
   cd 文件所在的目录
   hdfs dfs -put 文件名称 hdfs的路径
   hdfs dfs -put words.txt /
   ```

   可以遇到的错误

   ![image-20230518101320638](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/assets/image-20230518101320638.png)

解决方案

```
rdd_file = sc.textFile('hdfs://node1:8020/word.txt')
```

```python
from pyspark import SparkContext, SparkConf
import os

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    # 1 创建运行环境
    conf = SparkConf().setAppName('词频统计示例').setMaster('local[*]')
    sc:SparkContext = SparkContext(conf=conf)

    # 2 读取文件
    # sc.textFile('file:///tmp/pycharm_project_444/words.txt') # z这里是从linux磁盘中读取数据
    # sc.textFile('/word.txt') # 可以省略前面的协议
    rdd_file = sc.textFile('hdfs://node1:8020/word.txt') # 可以省略前面的协议
    print(rdd_file.collect())

    # 3 对数据进行处理
    # 3.1 对每行数据进行分割，把分割后的结果拼接成一个大的列表
    def line_split(line:str)->list:
        return line.split()

    rdd_flatmap = rdd_file.flatMap(line_split)
    # ['kafka spark spark', 'hadoop hadoop flume']
    # [[kafka, spark, spark], [hadoop, hadoop, flume]]
    # [kafka, spark, spark, hadoop, hadoop, flume]
    print(rdd_flatmap.collect())

    # 3.2 转换，把列表中的每个单词转成对应的元组(word, 1)
    def convert_tupe(word: str)->tuple:
        return (word, 1)
    rdd_tuple = rdd_flatmap.map(convert_tupe)
    print(rdd_tuple.collect())

    # 4 进行分组聚合，rdd_tuple，里面是存的列表，每个元素是元组，元组第一个元素是字符串，作为key进行分组
    # 第二个元素是对应的个数，作为聚合的对象
    def count_num(agg:int, curr:int)->int:
        return agg+curr
    result_rdd = rdd_tuple.reduceByKey(count_num)
    print(result_rdd.collect())
```

字符串排序

```
字典序
hello 

'1' '2' '3' ' 10' '111' '25' '33' '400'
'1' '10' '111' '2' '25' '3' '33' '400'
每个位置比较ascii码
```

```python
# 完整代码如下
from pyspark import SparkContext, SparkConf
import os

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    # 1 创建运行环境
    conf = SparkConf().setAppName('词频统计示例').setMaster('local[*]')
    sc:SparkContext = SparkContext(conf=conf)

    # 2 读取文件
    # sc.textFile('file:///tmp/pycharm_project_444/words.txt') # z这里是从linux磁盘中读取数据
    # sc.textFile('/word.txt') # 可以省略前面的协议
    rdd_file = sc.textFile('hdfs://node1:8020/word.txt') # 可以省略前面的协议
    print(rdd_file.collect())

    # 3 对数据进行处理
    # 3.1 对每行数据进行分割，把分割后的结果拼接成一个大的列表
    def line_split(line:str)->list:
        return line.split()

    rdd_flatmap = rdd_file.flatMap(line_split)
    # ['kafka spark spark', 'hadoop hadoop flume']
    # [[kafka, spark, spark], [hadoop, hadoop, flume]]
    # [kafka, spark, spark, hadoop, hadoop, flume]
    print(rdd_flatmap.collect())

    # 3.2 转换，把列表中的每个单词转成对应的元组(word, 1)
    def convert_tupe(word: str)->tuple:
        return (word, 1)
    rdd_tuple = rdd_flatmap.map(convert_tupe)
    print(rdd_tuple.collect())

    # 4 进行分组聚合，rdd_tuple，里面是存的列表，每个元素是元组，元组第一个元素是字符串，作为key进行分组
    # 第二个元素是对应的个数，作为聚合的对象
    def count_num(agg:int, curr:int)->int:
        return agg+curr
    result_rdd = rdd_tuple.reduceByKey(count_num)
    print(result_rdd.collect())

    # 按照key排序
    result_sortbykey = result_rdd.sortByKey()
    print(result_sortbykey.collect())

    # 按照value排序
    result_sortbyvalue = result_rdd.sortBy(lambda tup: tup[1], ascending=False)
    print(result_sortbyvalue.collect())

    # 使用sortByKey函数实现按照value进行排序
    result_sort = result_rdd.map(lambda tup:(tup[1], tup[0])).sortByKey().map(lambda tup:(tup[1], tup[0]))
    print(result_sort.collect())
    
    # 保存结果到hdfs中
    result_sortbykey.saveAsTextFile('hdfs://node1:8020/output')
    # 可以通过web ui进行查看 node1:9870
    
    # 停止Spark
    sc.stop()
```



### 1.2 基于Spark-Submit方式运行

```
./spark-submit --master local[*] /tmp/pycharm_project_956/002/src/01-pyspark-wordcount-hdfs.py

通过以上命令提交任务到local下进行执行
spark-submit --master local[*] python程序 

```



## 2. Spark On Yarn 环境搭建

### 2.1 Spark On Yarn的本质

SparkOnYearn的本质就是把spark应用提交到Yarn，由Yarn进行统一的调度和管理

这种方式是在公司使用比较多的方式

### 2.2 配置 Spark On Yarn

整个配置操作, 大家可参考<<spark的部署文档>>即可



在配置的时候, 一定要细心, 多校验

### 2.3 提交应用测试

- 测试pyspark自带的pi.py脚本

```shell
SPARK_HOME=/export/server/spark
${SPARK_HOME}/bin/spark-submit \
--master yarn \
--conf "spark.pyspark.driver.python=/root/anaconda3/bin/python3" \
--conf "spark.pyspark.python=/root/anaconda3/bin/python3" \
${SPARK_HOME}/examples/src/main/python/pi.py \
10

# 以上命令一共是两行
# pi.py是需要执行的任务，10是传给pi.py的参数，指定的是分区数量，并且程序会根据该参数确定具体循环次数

```

- 测试自己写的程序

```shell
SPARK_HOME=/export/server/spark
${SPARK_HOME}/bin/spark-submit \
--master yarn \
--conf "spark.pyspark.driver.python=/root/anaconda3/bin/python3" \
--conf "spark.pyspark.python=/root/anaconda3/bin/python3" \
/tmp/pycharm_project_956/002/src/01-pyspark-wordcount-hdfs.py

```

![image-20230518144907876](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/assets/image-20230518144907876.png)

![image-20230518144953345](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/assets/image-20230518144953345.png)



可能报出一下错误: 

![image-20220525105522089](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/002_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220525105522089.png)

```properties
思考为什么会报找不到python3的错误呢? 
	原因: anaconda我只在node1安装了, node2和node3没有安装
	
	解决方案: 在各个节点将anaconda安装成功

```

```
Spark程序运行时分了两块，都是JVM程序
Driver：是spark中的驱动程序，类似与mr中的applicationMaster
主要作用：申请资源，任务的分配，任务的监控
Executor：执行器，有线程池，运行了多个线程
主要作用：通过多个线程运行任务

```



### 2.4 两种部署方式说明



![image-20230518153304875](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/assets/image-20230518153304875.png)

spark把任务提交到集群时的两种部署方案

```properties
本质区别：spark程序中driver进程在哪里运行
例如：都是把任务提交到集群中，在Windows系统配置好spark环境，并且在Windows终端中执行spark-submit，--deploy-mode client，此时driver运行在Windows系统中，这种模式就是客户端模式
在Windows系统配置好spark环境，并且在Windows终端中执行spark-submit，--deploy-mode cluster，此时driver运行在集群中，这种模式就是集群模式

client模式:driver运行在执行spark-submit的机器中运行，
好处:driver就在本地运行，所有可以很方便的查看日志，便于测试
坏处:driver和executor不在同一个环境中，导致网络传输效率较低，从而影响整体的效率
一般是测试时使用

cluster模式:driver运行在集群的某个节点中
好处:driver和executor都在同一个环境中，网络传输效率高
坏处:日志就无法直接在客户端中进行查看
一般是生产中使用

```

![image-20230518154348678](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/assets/image-20230518154348678.png)



```
SPARK_HOME=/export/server/spark
${SPARK_HOME}/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--conf "spark.pyspark.driver.python=/root/anaconda3/bin/python3" \
--conf "spark.pyspark.python=/root/anaconda3/bin/python3" \
/tmp/pycharm_project_956/002/src/01-pyspark-wordcount-hdfs.py

```



查看spark程序日志

1. 在yarn中查看

   ![image-20230518155958854](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/assets/image-20230518155958854.png)

   ![image-20230518160013790](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/assets/image-20230518160013790.png)

2. spark日志服务中查看

   node1:18080

   ![image-20230518160248324](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/assets/image-20230518160248324.png)



## 3. Spark 程序与PySpark交互流程

spark-submit提交任务到spark集群，部署模式是client模式

```properties
1 启动Driver
2 向Master申请资源
3 Master向Driver分配资源
返回资源列表
Executor1 : node1 1core 2G
Executor2 : node3 1core 2G
4 连接对应的worker节点，通知它们启动Executor，当worker启动完成后，会反向注册到dirver（通知driver）
5 启动main程序
5.1 启动sparkcontext，构造出sc对象，基于py4j把python程序转成java程序进行执行
5.2 当sc启动完成后，会把后续的rdd相关程序合并在一起，根据RDD的依赖关系，形成一个DAG的执行图。根据DAG可以划分出有几个阶段，每个阶段有多少个线程，每个线程运行在那个executor上（任务分配）
5.3 每个阶段有多少个线程，每个线程运行在那个executor上确定后，把任务分发到对应的executor执行
5.4 executor获取到分发的任务后，开始执行，Task执行完成后把结果返回给driver。前提是有需要返回结果的操作，比如collect，也有操作不需要返回结果比如saveAsTextFile
5.5 当driver获取所有的executor返回的结果之后，开始执行非RDD程序，通知master关闭sc，执行资源回收的工作

```

spark-submit提交任务到spark集群，部署模式是cluster模式

```properties
1 首先把任务提交到主节点
2 Master根据Driver资源信息，在worker节点中随机找一台符合要求的节点，启动Driver，把任务提交Driver
3 对应worker节点接收到任务后，启动Driver，并且和Master保持心跳机制，告知master启动成功，申请资源
4 master根据申请的资源在集群中进行查找，返回资源列表
Executor1 : node1 1core 2G
Executor2 : node3 1core 2G
5 连接对应的worker节点，通知它们启动Executor，当worker启动完成后，会反向注册到dirver（通知driver）
6 启动main程序
6.1 启动sparkcontext，构造出sc对象，基于py4j把python程序转成java程序进行执行
6.2 当sc启动完成后，会把后续的rdd相关程序合并在一起，根据RDD的依赖关系，形成一个DAG的执行图。根据DAG可以划分出有几个阶段，每个阶段有多少个线程，每个线程运行在那个executor上（任务分配）
6.3 每个阶段有多少个线程，每个线程运行在那个executor上确定后，把任务分发到对应的executor执行
6.4 executor获取到分发的任务后，开始执行，Task执行完成后把结果返回给driver。前提是有需要返回结果的操作，比如collect，也有操作不需要返回结果比如saveAsTextFile
6.5 当driver获取所有的executor返回的结果之后，开始执行非RDD程序，通知master关闭sc，执行资源回收的工作

```

spark-submit提交任务到yarn集群，部署模式是client模式

![image-20230518171909185](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/assets/image-20230518171909185.png)

```properties
1 启动Driver
2 连接到yarn主节点（ResurceManager）向主节点提交资源任务（最终启动executor）
3 yarn主节点接收到任务后，会选择一个nodeManager，启动AppMaster，启动后，和ResourceManager建立心跳机制，通知主节点appmaster启动成功
4 appmaster根据提交的资源信息向ResourceManager申请资源，通过心跳方式发送请求，不断向ResoureManager询问
5 一旦appmaster检测到资源准备完成，也会获取资源信息列表，appmaster根据资源信息列表，找到对应的nodeManager启动Executor
6 当Executor启动完成后，会通知appMaster并且通知Driver
7 启动main程序
7.1 启动sparkcontext，构造出sc对象，基于py4j把python程序转成java程序进行执行
7.2 当sc启动完成后，会把后续的rdd相关程序合并在一起，根据RDD的依赖关系，形成一个DAG的执行图。根据DAG可以划分出有几个阶段，每个阶段有多少个线程，每个线程运行在那个executor上（任务分配）
7.3 每个阶段有多少个线程，每个线程运行在那个executor上确定后，把任务分发到对应的executor执行
7.4 executor获取到分发的任务后，开始执行，Task执行完成后把结果返回给driver。前提是有需要返回结果的操作，比如collect，也有操作不需要返回结果比如saveAsTextFile
7.5 当driver获取所有的executor返回的结果之后，开始执行非RDD程序，通知appmaster关闭sc，回收资源

```

spark-submit提交任务到yarn集群，部署模式是cluster模式

![image-20230518172911568](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/assets/image-20230518172911568.png)

```properties
yarn的client模式和cluster模式区别
client下：Driver和app Master是两个不同程序
Driver负责任务分配 监控 管理工作
appMaster负责申请资源启动executor
cluster模式下：Driver和AppMaster合二为一，作用合在一起

```





## 4. Spark-Submit相关参数说明

​	spark-submit 这个命令 是我们spark提供的一个专门用于提交spark程序的客户端, 可以将spark程序提交到各种资源调度平台上: 比如说 **local(本地)**, spark集群,**yarn集群**, 云上调度平台(k8s ...)

​		spark-submit在提交的过程中, 设置非常多参数, 调整任务相关信息

- 基本参数设置

![image-20211204181315569](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/002_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211204181315569.png)

- Driver的资源配置参数

![image-20211204181424151](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/002_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211204181424151.png)

- executor的资源配置参数

![image-20211204181522309](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/002/2%20%E7%AC%94%E8%AE%B0/002_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211204181522309.png)

# 003 PySpark笔记

今日内容:

- 1- 如何构建RDD
- 2- RDD算子相关的操作(记录)
- 3- 综合案例

## 1.  Spark Core

### 1.1 RDD的基本介绍

```properties
RDD:弹性分布式数据集

早期计算模式：
单机版：python MySQL
限制:只适合在小规模数据中使用，不适合扩展

随着技术的发展，单机模式不再适合大规模数据，有资源上线，不适合扩展

使用分布式技术解决以上的限制

产生MR计算框架: 分而治之

MR流程: 首先读取数据到内存，对数据进行转换处理，把处理的结果写入环形缓冲区(内存)，当缓冲区满了则写入到磁盘。如果有多个缓冲区的内容，则进行合并，最终把结果存储在磁盘中。
这种处理流程，在有限的资源下可以处理大规模数据。


使用mr进行处理时，如果需要多个MR进行迭代，执行完第一个MR后，保存结果串行执行第二个，每个MR都需要重新申请资源，IO效率较低

因此就出现可以高效计算和迭代工具
 Spark (RDD)
 
```

![image-20230520100738705](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/003/2%20%E7%AC%94%E8%AE%B0/assets/image-20230520100738705.png)

![image-20230520100813897](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/003/2%20%E7%AC%94%E8%AE%B0/assets/image-20230520100813897.png)



### 1.2 RDD的五大特性

```properties
1 可分区: 每个RDD都必须是可以分区的，每个分区代表一个Task线程，可分区也是分布式的基本要求
2 计算函数会作用在每个分区: 每个RDD都是由一个计算函数得到的，计算函数作用在每个分区上
3 RDD之间存在依赖关系

4 对于kv类型rdd存在分区函数
5 移动存储不如移动计算(让计算和数据更近)
```



### 1.3 RDD的五大特点

```properties
1 RDD可分区
2 RDD是只读（不能更改）
3 RDD之间有依赖：宽依赖，窄依赖
4 RDD每次计算都会重新计算，需要把结果缓存下来，RDD不保存数据
5 检查点(checkpoint)，相当于对结果进行备份。当RDD的依赖关系越来越长，为了减少因为计算失败导致回溯效率低问题，不要再检查之前的依赖关系，直接从检查点的位置获取结果即可
```



### 1.4  如何构建RDD

![image-20230520110458152](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/003/2%20%E7%AC%94%E8%AE%B0/assets/image-20230520110458152.png)

```properties
一种：通过并行化的方式把代码中数据构建RDD，测试中使用
另一种：通过读取外部数据的方式构建RDD，生产中使用
```

#### 1 通过并行化的方式来构建RDD

创建pycharm代码模板

![image-20230520110919540](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/003/2%20%E7%AC%94%E8%AE%B0/assets/image-20230520110919540.png)

模板代码

```python
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    conf = SparkConf().setMaster("local[*]").setAppName("wordCount")

    sc = SparkContext(conf=conf)
```

创建RDD完整代码

```python
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")

    # 1 构造运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("并行化创建RDD")

    sc = SparkContext(conf=conf)

    # 2 并行化创建RDD
    rdd_para = sc.parallelize([1,2,3,4,5], 5)

    # 3 查看RDD相关信息
    print(rdd_para.glom().collect()) # 查看RDD每个分区中的元素
    print(rdd_para.getNumPartitions()) # 查看RDD的分区个数
    print(rdd_para.collect()) # 查看RDD完整内容

    print('-'*20)

    # 2 并行化创建RDD
    rdd_para = sc.parallelize([1,1,2,2,3,4,5], 10)

    # 3 查看RDD相关信息
    print(rdd_para.glom().collect())
    print(rdd_para.getNumPartitions())
    print(rdd_para.collect())
    
    # 2 并行化创建RDD
    rdd_para = sc.parallelize(['张三', '李四', '王五'], 10)

    rdd_map = rdd_para.map(lambda word: word+str('100'))

    # 3 查看RDD相关信息
    print(rdd_map.glom().collect())
    print(rdd_map.getNumPartitions())
    print(rdd_map.collect())
```

```
[
	[
		[1,2,3],[3,4,5]
	],
	[
		[6,7,8],[8,9,10]
	]
]
```



#### 2 通过读取外部数据方式构建RDD

完整代码

```python
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    # 1 构建运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("创建RDD")

    sc = SparkContext(conf=conf)

    # 2 读取文件创建RDD
    rdd_file = sc.textFile('hdfs://node1:8020/word.txt', 10)

    # 3 查看RDD的信息
    print(rdd_file.glom().collect())
    print(rdd_file.getNumPartitions())
    print(rdd_file.collect())
```

```properties
验证了每个分区都有计算函数，例如map操作
glom函数，显示如何分区的
getNumPartitions 获取分区数量
collect 获取整个RDD、的完整内容（不显示分区情况）

setMaster('local[*]') *表示的是按照机器的cpu core、的数量设置并行度
在并行化创建时：如果没有parallelize没有设置分区数量，则以setMaster中设置的并行度为准、
如果设置了则以parallelize设置的为准

读取外部文件创建RDD时，以textFile中为准

```

textFile读取目录创建RDD，具体分区设置

![image-20230520150853498](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/003/2%20%E7%AC%94%E8%AE%B0/assets/image-20230520150853498.png)

读取目录创建RDD完整代码

```python
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    conf = SparkConf().setMaster("local[1]").setAppName("读取hdfs目录")

    sc = SparkContext(conf=conf)

    # rdd_dir = sc.textFile('hdfs://node1:8020/data') # 默认分区数量取决于文件数量
    rdd_dir = sc.textFile('hdfs://node1:8020/data', 5) # 默认分区数量取决于文件数量

    print(rdd_dir.glom().collect())
    print(rdd_dir.getNumPartitions())
    print(rdd_dir.collect())

```

# 读取小文件适用wholeTextFile

#### 使用方法和textFile一样

![1684985368360](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1684985368360.png)





## 2. RDD算子相关的操作

在spark中，把支持接收函数函数（高阶函数）或者具有特殊功能的函数称为算子

### 2.1 RDD算子分类

在整个RDD中算子可以分成两类：transformation（转换算子）、action（动作算子）

```properties
转换算子:
1 返回的结果都是新的RDD（RDD是只读）
2 所有的转换算子都是惰性的，只有遇到action算子才会真正的执行
3 RDD不存储数据，只存储转换的规则，遇到action算子后对数据进行处理

动作算子:
1 返回的结果不是RDD或者没有返回值
2 所有的动作算子都是立即执行的，每个action操作都会触发任务
```

常见的转换算子

![1685067247200](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685067247200.png)

常见动作算子

![1685067268766](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685067268766.png)

整个Spark所有的RDD的算子文档: https://spark.apache.org/docs/3.1.2/api/python/reference/pyspark.html#rdd-apis



### 2.2 RDD的Transformation算子操作

值类型算子：主要针对值进行操作的算子

![1685064659119](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685064659119.png)



- map算子：一对一的转换操作

  - 作用，根据用传入的函数，把原始的RDD中的每个元素一对一的转成新的RDD中的元素

  - 作用在每个分区的每个元素上，对每个元素都去执行接收的函数

  - **作用:根据原始RDD里的元素把原RDD里的元素转换成新的元素,例如将字符串转换成元组**

    ```python
    >>> rdd_init = sc.parallelize([1,2,3,4,5,6,7,8,9])
    >>> rdd_init.map(lambda x: x+1).collect()
    [2, 3, 4, 5, 6, 7, 8, 9, 10]
    
    lambda 匿名函数也可以有名的函数
    ```

- flatMap算子

  作用:先做map做的事情接着

  ​         再进行降维（拉平）操作

  ```python
  >>> rdd_init = sc.parallelize(['hello world', 'hadoop hadoop'])
  >>> rdd_init.flatMap(lambda line: line.split()).collect()
  ['hello', 'world', 'hadoop', 'hadoop']
  ```

  

  ![1684996311865](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1684996311865.png)



- mapvalue算子

  作用:针对二元元组rdd,对其内部的二元元组的value执行map操作

  ![1685001086790](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685001086790.png)

- groupBy算子：进行分组操作的，根据接收的函数（每个元素传给接收到函数，返回的值作为key）进行分组操作

  - 代码实现如下：

    ```python
        def groupBy(self, f, numPartitions=None, partitionFunc=portable_hash):
            """
            Return an RDD of grouped items.
    
            Examples
            --------
            >>> rdd = sc.parallelize([1, 1, 2, 3, 5, 8])
            >>> result = rdd.groupBy(lambda x: x % 2).collect()
            >>> sorted([(x, sorted(y)) for (x, y) in result])
            [(0, [2, 8]), (1, [1, 1, 3, 5])]
            """
            return self.map(lambda x: (f(x), x)).groupByKey(numPartitions, partitionFunc)
        
        
        # rdd_init = sc.parallelize([1,2,3,4,5,6,7,8,9])
        # rdd_groupby = rdd_init.groupBy(lambda x: x%2).groupByKey(numPartitions, partitionFunc)
        # rdd_init.map(lambda x: (x%2, x))
        # [1,2,...]
        # [(1,1), (0,1),(1,3),(0,4),(1,5),(0,6)...]
        
        
    ```

    示例代码

    ```python
    >>> rdd_init = sc.parallelize([1,2,3,4,5,6,7,8,9])
    >>> rdd_init.groupBy(lambda x: x%2).collect()
    [(0, <pyspark.resultiterable.ResultIterable object at 0x7ff232f3ebe0>), (1, <pyspark.resultiterable.ResultIterable object at 0x7ff232f4b6a0>)]
    >>> rdd_init.groupBy(lambda x: x%2).mapValues(list).collect()
    [(0, [2, 4, 6, 8]), (1, [1, 3, 5, 7, 9])]
    ```

- distinct算子

  作用:对rdd数据进行去重,然后返回新的rdd

  ![1685001989363](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685001989363.png)

#### 关联算子:

- join算子

  作用:对两个RDD执行join操作(可实现SQL的内外链接)

  注意:join算子只能用于二元元组

  ![1685003821754](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685003821754.png)

  

  示例:

  ![1685003945483](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685003945483.png)

- filter算子

  作用:过滤数据

  ![1685067015398](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685067015398.png)







#### 值类型算子

- union和intersection算子

  - union求并集 insetsection 求交集

    ```python
    >> rdd1 = sc.parallelize([1,2,3,4])
    >>> rdd2 = sc.parallelize([3,4,6,5])
    >>> rdd1.union(rdd2).collect()   # 求并集
    [1, 2, 3, 4, 3, 4, 6, 5]
    >>> rdd1.union(rdd2).distinct().collect() # 求并集 并且进行去重操作
    [4, 1, 5, 2, 6, 3]
    >>> rdd1.intersection(rdd2).collect() # 求交集
    [4, 3]
    ```





#### key类型算子

- groupByKey算子，仅仅做了分组操作

  - 作用:针对KV型RDD,自动按照key分组

  - ![1685004685875](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685004685875.png)

    ```python
    >>> rdd1 = sc.parallelize([1,2,3,4]) # 原始数据不是key value类型的所有不能直接调用groupByKey
    
    # 以下操作根据原始数据，对每个元素取模得到余数，把余数作为key，进而可以进行分组
    >>> rdd1.map(lambda x: (x%2, x)).groupByKey().collect()
    [(0, <pyspark.resultiterable.ResultIterable object at 0x7ff232f4bf10>), (1, <pyspark.resultiterable.ResultIterable object at 0x7ff232f31b80>)]
    >>> rdd1.map(lambda x: (x%2, x)).groupByKey().mapValues(list).collect()
    [(0, [2, 4]), (1, [1, 3])]
    
    # 单独定义key value类型的数据，进行分组操作
    >>> rdd_init = sc.parallelize([('bd17', 'zs'),('bd17', 'ls'),('bd16', 'ww')])
    >>> rdd_init.groupByKey().mapValues(list).collect()
    [('bd17', ['zs', 'ls']), ('bd16', ['ww'])]
    >>> 
    ```

- reduceByKey算子

  - 作用，按照key进行分组,再按接收的函数对每个分组进行聚合操作

  - ![1684998827035](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1684998827035.png)

    ```python
    >>> rdd_init = sc.parallelize([('bd17', 'zs'),('bd17', 'ls'),('bd16', 'ww')])
    
    # 原始数据中无法直接进行累加运算，需要进行替换，把每个人名字，替换成1即可 
    >>> rdd_init.map(lambda tup: (tup[0], 1)).collect()
    [('bd17', 1), ('bd17', 1), ('bd16', 1)]
    
    >>> rdd_init.map(lambda tup: (tup[0], 1)).reduceByKey(lambda agg, curr: agg+curr).collect()
    [('bd17', 2), ('bd16', 1)]
    >>> 
    ```

- sortByKey算子

- sortBy算子和sortBykey算子作用相似,任意使用一个即可

- sortBy和sortBykey都是上传三个参数:ascending(升序或降序,默认升序,true升,false降),numpartitions(设置分区数量,全局排序就选1),func(告知按照rdd中哪个数据进行排序,例如:lambda x:x[1]表示按照valu值排序),,,,,,,,,sortBy可以用:lambda x:x[1]表示按照valu值排序,sortBykey自动按照key值排序,可使用lambda设置其他设置

  - 作用，按照key进行排序，可以升序或者降序

    ```python
    >>> rdd_init = sc.parallelize([('bd17', 'zs'),('bd17', 'ls'),('bd16', 'ww')])
    
    # 直接使用sortByKey按照key的字典序对数据进行排序，升序排序
    >>> rdd_init.sortByKey().collect()
    [('bd16', 'ww'), ('bd17', 'zs'), ('bd17', 'ls')]
    
    # 直接使用sortByKey按照key的字典序对数据进行排序，降序排序
    >>> rdd_init.sortByKey(ascending=False).collect()
    [('bd17', 'zs'), ('bd17', 'ls'), ('bd16', 'ww')]
    
    # 使用sortByKey按照value进行排序
    >>> rdd_init.map(lambda tup: (tup[1], tup[0])).sortByKey().map(lambda tup: (tup[1],tup[0])).collect()
    [('bd17', 'ls'), ('bd16', 'ww'), ('bd17', 'zs')]
    ```

  

### 2.3 RDD的action算子

- collect()算子
  - 作用：收集各个分区处理之后的结果，并且放在一个列表中

#### 聚合算子:(reduce|fold)

- reduce()算子

  - reduce(fn)，根据接收的fn对调用reduce的rdd进行聚合操作

    ```python
    >>> rdd_init.collect()
    [1, 1, 1, 2, 4, 2, 3]
    >>> rdd_init.reduce(lambda agg, curr: agg+curr)
    ```

- fold算子

  功能:和reduce一样,但是聚合带有初始值,初始值最作用在:分区内聚合,分区间聚合

  ​	简单来说就是一个分区结果会加上初始值,两个分区再聚合还会加速初始值

  ![1685068682926](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685068682926.png)

  ```python
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6,7,8,9],3)
      rdd1=rdd.fold(10,lambda x,y:x+y)
      print(rdd1)
      结果:85
  ```

  

  

- first() 算子

  - 获取第一个元素

    ```python
    >>> rdd_init = sc.parallelize([1,2,4,3])
    >>> rdd_init.first()
    1
    ```

- take(n) 算子

  - 获取前n个元素

    ```python
    >>> rdd_init = sc.parallelize([1,2,4,3])
    >>> rdd_init.take(2)
    [1, 2]
    ```

- top算子

  - 格式top(n, [fn])，对数据进行降序排序取出前n个

    ```python
    >>> rdd_init.collect()
    [1, 2, 4, 3]
    >>> rdd_init.top(2)
    [4, 3]
    ```

- count算子

  - 统计rdd中元素的个数

    ```python
    >>> rdd_init.collect()
    [1, 2, 4, 3]
    >>> rdd_init.count()
    4
    ```

- takeSample算子

  - 格式 takeSample(是否放回，抽取几个，随机种子)

    ```python
    >>> rdd = sc.parallelize(range(0, 10))
    >>> rdd.collect()
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    >>> rdd.takeSample(True, 5, 1) # True表示有放回抽样，5表示抽取几个元素，1随机种子
    [8, 8, 0, 3, 6]
    >>> rdd.takeSample(True, 5, 1) # 随机种子一样则结果一样
    [8, 8, 0, 3, 6]
    >>> rdd.takeSample(True, 5, 10) # 种子不一样结果不一样
    [3, 3, 9, 7, 4]
    >>> rdd.takeSample(False, 5, 1) # False表示无放回抽样
    [6, 8, 9, 7, 5]
    ```

- takeOrdered算子

  作用:对RDD进行排序取前N个,与top类似,但takeOrdered可以正序和反序

  ​	使用lambda X:-X表示反序

  ​	使用lambda X:X表示正序

  ![1685062676068](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685062676068.png)





- foreach算子

  ​	作用可遍历列表,但没有返回值,在特定情况下使用

  ![1685062826811](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685062826811.png)

- saveAsTextFile算子

  作用:将RDD的数据保存

  ![1685062925401](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685062925401.png)

- countByKey算子

​	统计Key的个数

- countByValue算子

  统计value的个数

```python
>>> rdd_init = sc.parallelize([1,1,1,2,4,2,3])

>>> rdd_init.countByValue()
defaultdict(<class 'int'>, {1: 3, 2: 2, 4: 1, 3: 1})
```





#### 分区函数:





- mappartitions算子

  作用:与map相同,但是map是一个分区一个元素处理,而mappartitions是处理整个分区,减少IO次数提高磁盘空间效率

- foreachpartition算子

  作用:与foreach相同,但是foreach是一个分区一个元素处理,而foreachpartition是处理整个分区,减少IO次数提高磁盘空间效率



- partitionBy算子

  作用:对RDD进行自定义分区操作

  

![1685063370435](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685063370435.png)

示例:![1685063422525](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685063422525.png)



示例:

![1685063450434](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685063450434.png)





- repartition算子

  作用:对RDD的分区执行重新分区(仅数量)

  ```python
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6],3)
      add=rdd.repartition(1)
      print(add.getNumPartitions.collect())
  
  ```

  

注意:

![1685064146880](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685064146880.png)











- coalesce算子

  作用:和repartiti相同,对RDD的分区执行重新分区(仅数量)

  ```python
  rdd.coalesce(5,shuffle=True)
  
  ```

  coalesce增加分区需要添加shuffle=True,shuffle=True是安全阀

  reaprtition的提出就是默认shuffle=True的coalesce

  ![1685064598453](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685064598453.png)

  







- ### groupByKey和reduceByKey的区别



![1685066512005](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685066512005.png)	



![1685066545544](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685066545544.png)

功能上的区别:groupByKey仅有分组功能

​				reduceByKey除了有ByKey的分组功能,还有reduce的聚合功能,reducByKey是一个分组+聚合为				一体的算子

​	在性能上:reduceBeKey的性能是远大于groupBekey

​			因为groupByKey只能分组,执行时是先分组(shuffle)后聚合

​			reduceByKey自带聚合逻辑,可以先在分区内做预聚合再做最终聚合

​	

对与groupByKey,reduceByKey的最大提升在于,分组前进行了预聚合,那么在shuffle时被shaffle的数据可以极大的减少,这就极大的提升了性能;



分组+聚合,首选reduceByKey优势就月高s

# 004 PySpark笔记

今日内容:

- 1- RDD的综合案例
- 2- RDD的持久化:  缓存 和 checkpoint
- 3- RDD的共享变量: 广播变量  和 累加器
- 4- RDD内核调度原理



## 1 RDD的重要算子

![image-20230521092139115](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521092139115.png)

### 分区函数

```properties
1 针对每个分区进行操作的函数
map接收的函数会作用在每个元素中，mapPartitions接收的函数会作用在每个分区上，在接收函数内部再对每个分区内的元素进行处理
foreach函数和foreachPartition函数
2 重新修改分区的函数
```

![image-20230521092557654](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521092557654.png)

![image-20230521092623487](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521092623487.png)

![image-20230521093236600](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521093236600.png)

![image-20230521094532203](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521094532203.png)

```properties
map函数接收的函数接收的参数是RDD中的每个元素
mapPartitions函数接收的函数接收的参数是RDD中每个分区

def add_one(e): # e代表的就是每个分区
    l = []
    for i in e:
        l.append(i+1)
    return l
    
def add_one(e):
    for i in e:
        yield i+1

rdd1.mapPartitions(add_one) # 前面两种写法的使用

rdd1.mapPartitions(lambda e: map(lambda x:x+1, e)) # 第三种写法
```

![image-20230521100751023](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521100751023.png)

![image-20230521100810665](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521100810665.png)

```properties
foreach 类似于map，把接收的函数作用在RDD的每个元素上
foreachPartition类似于mapPartitions，把接收的函数作用在RDD的每个分区上

>>> rdd1.foreach(lambda x: print(x))
1
2
3
4
5
6
7
8
9
0
>>> rdd1.foreachPartition(lambda x: print(x))
<itertools.chain object at 0x7f06a4ae8760>
<itertools.chain object at 0x7f06a4ae8760>
>>> rdd1.foreachPartition(lambda x: print(list(x)))
[6, 7, 8, 9, 0]
[1, 2, 3, 4, 5]
```

```properties
分区函数：mapPartitions foreachPartition

分区函数是每个分区中执行自定义函数，所有自定义函数接收的参数是一个个的分区，普通函数（map foreach）在每个元素上执行自定义函数，所有自定义函数接收的是一个个的元素。

分区函数触发的次数更少一些，可以减少一些耗时操作（IO）

工作中如果有对应的需求，有限使用分区函数

```

### 重分区函数

```properties
作用：对RDD中分区数量进行修改
什么时候需要修改分区数量？增加分区数
一个分区是由一个线程进行处理的，如果分区数量较少，则线程数量就少，并行度就不够高，因此可以增加分区数，进而增加线程的数量，进行一步提升并行度

减少分区数？
当需要把数据写入到hdfs中时，文件越多分区就越多。占用磁盘空间就越多，浪费很多空间，此时就可以减少分区后再写入到hdfs
当分区太多，线程就过多，太多的线程就导致很多资源浪费在线程切换上，导致效率也比较低
```

- repartition函数，可以增加分区，也可以减少分区（减少分区时建议使用coalesce函数）

  ![image-20230521102656096](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521102656096.png)

  ```python
  >>> rdd1.glom().collect()
  [[1, 2, 3, 4, 5], [6, 7, 8, 9, 0]]
  >>> rdd2 = rdd1.repartition(3)
  >>> rdd2.glom().collect()
  [[], [1, 2, 3, 4, 5], [6, 7, 8, 9, 0]]                                          
  >>> rdd2 = rdd1.repartition(1) #减少分区时建议使用coalesce函数
  >>> rdd2.glom().collect()
  [[1, 2, 3, 4, 5, 6, 7, 8, 9, 0]]
  
  # 通过repartition进行重分区时会产生shuffle
  ```

- coalesec函数

  ![image-20230521103348988](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521103348988.png)

  ```python
  >>> rdd1.glom().collect()
  [[1, 2, 3], [4, 5, 6], [7, 8, 9, 0]]
  >>> rdd2 = rdd1.coalesce(2)
  >>> rdd2.glom().collect()
  [[1, 2, 3], [4, 5, 6, 7, 8, 9, 0]]
  >>> rdd3 = rdd1.coalesce(4)
  >>> rdd3.glom().collect()
  [[1, 2, 3], [4, 5, 6], [7, 8, 9, 0]]
  >>> rdd4 = rdd1.coalesce(4, shuffle=True)
  >>> rdd4.glom().collect()
  [[7, 8, 9, 0], [4, 5, 6], [], [1, 2, 3]]
  >>> 
  
  作用：默认减少分区，不会产生shuffle
  其中shuffle参数为False则不会产生shuffle过程，如果增加分区则无法实现
  ```

- partitionBy，针对kv类型数据进行分区的

  ![image-20230521104406420](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521104406420.png)

  ```python
  partitioinBy(num, [fn])
  
  >>> rdd2 = rdd1.map(lambda x: (x, 1))
  >>> rdd2.collect()
  [(1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1), (7, 1), (8, 1), (9, 1), (0, 1)]
  >>> rdd2.partitionBy(5).glom().collect()
  [[(5, 1), (0, 1)], [(1, 1), (6, 1)], [(2, 1), (7, 1)], [(3, 1), (8, 1)], [(4, 1), (9, 1)]]
  >>> rdd2.partitionBy(2).glom().collect()
  [[(2, 1), (4, 1), (6, 1), (8, 1), (0, 1)], [(1, 1), (3, 1), (5, 1), (7, 1), (9, 1)]]
  >>> 
  
  >>> rdd2.collect()
  [(1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1), (7, 1), (8, 1), (9, 1), (0, 1)]
  >>> rdd2.partitionBy(2, lambda num: 1 if num >= 5 else 0).glom().collect()
  [[(1, 1), (2, 1), (3, 1), (4, 1), (0, 1)], [(5, 1), (6, 1), (7, 1), (8, 1), (9, 1)]]
  >>> 
  ```

### 聚合类算子

```properties
单值聚合算子:
reduce | fold | aggregate
语法:
reduce(fn): 根据接收的fn函数，对整个数据集进行聚合操作
例如: reduce(lambda x, y: x+y) 对整个数据集进行累加，相当于累加是默认的初始值是0

>>> rdd1.glom().collect()
[[1, 2, 3], [4, 5, 6], [7, 8, 9, 0]]
>>> rdd1.reduce(lambda x, y: x+y)
45

fold(defultValue, fn) : 根据接收的fn函数和默认的初始值，先对每个分区进行聚合，再对分区结果进行聚合

>>> rdd1.fold(0, lambda x, y: x+y)
45
>>> rdd1.fold(10, lambda x, y: x+y)
85
>>> rdd2 = rdd1.repartition(5)
>>> rdd2.fold(10, lambda x, y: x+y)
105
fold对每个分区进行聚合时，每个分区都会把初始值传入进去进行计算，对分区结果进行计算时会再次计算一遍初始值。

aggregate(defaultValue, fn1, fn2): 根据接收的fn1 fn2默认的初始值，使用fn1相对每个分区进行聚合，再使用fn2对每个分区结果进行聚合

>>> rdd1.glom().collect()
[[1, 2, 3], [4, 5, 6], [7, 8, 9, 0]]
>>> def add(x, y):
...     return x+y
... 
>>> def add1(x, y):
...     return x+y
... 
>>> rdd1.aggregate(0, add, add1)
45
>>> rdd1.aggregate(10, add, add1)
85
>>> def add2(x, y):
...     return x+1
... 
>>> def add(x, y):
...     return x+y
>>> rdd1.aggregate(10, add, add2)
13
[[1, 2, 3], [4, 5, 6], [7, 8, 9, 0]] # 原始数据
[16, 25, 34] # 使用add分别对每个分区进行聚合
# 接下来使用add2对分区结果进行聚合
# 第一次调用add2时 add2(10, 16)结果是11
# 11作为上一次的累加结果传给下一次调用add2时的一个参数add1(11, 25)，结果是12
# 12作为上一次的累加结果传给下一次调用add2时的一个参数add1(12, 34)，结果是13

```

```properties
多值聚合算子
groupByKey reduceByKey foldByKey aggregateByKey
相对于单值聚合算子，多了一个分组的操作

rdd5 = sc.parallelize([('a', 1), ('b', 1), ('a', 2)])
>>> rdd5.glom().collect()
[[('a', 1)], [('b', 1), ('a', 2)]]
>>> rdd5.foldByKey(0, lambda x, y : x+y).collect()
[('b', 1), ('a', 3)]

>>> rdd5.aggregateByKey(0, lambda x, y: x+y, lambda x,y: x+y).collect()
[('d', 10), ('b', 1), ('a', 6), ('c', 5), ('e', 30)]
>>> rdd5.aggregateByKey(10, lambda x, y: x+y, lambda x,y: x+y).collect()
[('d', 20), ('b', 11), ('a', 26), ('c', 15), ('e', 40)]
>>> rdd5.glom().collect()
[[('a', 1), ('b', 1)], [('a', 2), ('a', 3)], [('c', 5), ('d', 10), ('e', 30)]]

```

### groupByKey reduceByKey区别

- groupByKey 分组 没有聚合 

- reduceByKey分组聚合

- groupByKey + reduce 可以实现reduceByKey collect的效果

  reduceByKey执行过程，提前在分区内进行分组聚合，减少数据量

  ![image-20230521152021305](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521152021305.png)

  groupByKey只能是先分组，发送的消息量更多一些，效率低

  ![image-20230521152027761](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521152027761.png)



### 关联算子

```properties
作用类似于MySQL表的连接
join() 类似于表的内连接
>>> rdd1 = sc.parallelize([('a', 1), ('b',2)])
>>> rdd2 = sc.parallelize([('a', 2), ('a', 3), ('c',4)])
>>> rdd1.join(rdd2)
PythonRDD[197] at RDD at PythonRDD.scala:53
>>> rdd1.join(rdd2).collect()
[('a', (1, 2)), ('a', (1, 3))]
leftOuterJoin 左外连接
>>> rdd1.leftOuterJoin(rdd2).collect()
[('b', (2, None)), ('a', (1, 2)), ('a', (1, 3))]
rightOuterJoin 右外连接
>>> rdd1.rightOuterJoin(rdd2).collect()
[('c', (None, 4)), ('a', (1, 2)), ('a', (1, 3))]
>>> 
全外连接
>>> rdd1.fullOuterJoin(rdd2).collect()
[('b', (2, None)), ('c', (None, 4)), ('a', (1, 2)), ('a', (1, 3))]
>>> 

```



## 2. 综合案例

### 2.1 搜索案例

#### 1 数据集介绍:

![image-20230521153457450](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521153457450.png)

```properties
访问时间 用户ID  [搜索关键词] url在搜索结果的排名  url在页面展示的排名 url
字段直接的分隔符 \t 

1, 统计每个关键词出现的次数
2, 统计每个用户每个关键词点击的次数
3, 统计每个小时点击的次数


```

```
需求1分析思路
1 上传文件到linux hdfs
2 读取文件，textFile
3 把每一行进行分割
4 取出第三个字段
5 把关键词转成元组
6 进行聚合操作，统计出关键词出现的次数

```

#### 需求一部分代码实现（搜索词次数统计）

```python
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    # 1 准备运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("sogou案例")
    sc = SparkContext(conf=conf)

    # 2 读取文件，textFile
    rdd_file = sc.textFile('file:///tmp/pycharm_project_956/004/data/SogouQ.sample')
    # 3 把每一行进行分割
    rdd_split = rdd_file.map(lambda line: line.split('\t'))
    # ["00:00:03	7954902679225404	[芜湖旅游]	7 3	www.tourunion.com/spot/city/583340.htm"
    # , "00:00:03	9717831746543397	[奥运]	7 3	www.beijing2008.com"
    # ]
    # [
    #  ["00", "795", "[芜湖旅游]", "7', "3", "www"],
    #  ["00", "971", "[奥运]"， "7", "3", "www"]
    # ]
    # 4 取出第三个字段
    # 4.1 先进行过滤 把分割后长度小于3的行过滤掉，以防止取第三个字段时数组下标越界
    rdd_filter = rdd_split.filter(lambda line: len(line) >= 3)
    # 4.2 取出下标为2的元素，同时去掉方括号
    rdd_keyword = rdd_filter.map(lambda line: line[2][1:-1])
    # 5 把关键词转成元组
    rdd_keyword_tuple = rdd_keyword.map(lambda word: (word, 1))
    # 6 进行聚合操作，统计出关键词出现的次数
    result = rdd_keyword_tuple.reduceByKey(lambda x, y: x+y)
    # 7 按照value排序
    result = result.sortBy(lambda tup: tup[1], ascending=False)
    # 8 输出结果
    print(result.take(10))

```

运行结果

![image-20230521160337011](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521160337011.png)

#### jieba分词介绍

```properties
安装jieba分词库
pip install -i https://mirrors.ustc.edu.cn/pypi/web/simple jieba

import jieba

print(list(jieba.cut('商品和服务')))
print(list(jieba.cut('商品和服务', cut_all=True)))
print(list(jieba.cut('武汉市长江大桥')))
print(list(jieba.cut('武汉市长江大桥', cut_all=True)))

运行结果:
['商品', '和', '服务']
['商品', '和服', '服务']
['武汉市', '长江大桥']
['武汉', '武汉市', '市长', '长江', '长江大桥', '大桥']

```

#### 需求一代码完整实现（jieba分词得到关键词）

```python
import os
from pyspark import SparkConf, SparkContext
import jieba

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    # 1 准备运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("sogou案例")
    sc = SparkContext(conf=conf)

    # 2 读取文件，textFile
    rdd_file = sc.textFile('file:///tmp/pycharm_project_956/004/data/SogouQ.sample')
    # 3 把每一行进行分割
    rdd_split = rdd_file.map(lambda line: line.split('\t'))
    # ["00:00:03	7954902679225404	[芜湖旅游]	7 3	www.tourunion.com/spot/city/583340.htm"
    # , "00:00:03	9717831746543397	[奥运]	7 3	www.beijing2008.com"
    # ]
    # [
    #  ["00", "795", "[芜湖旅游]", "7', "3", "www"],
    #  ["00", "971", "[奥运]"， "7", "3", "www"]
    # ]
    # 4 取出第三个字段
    # 4.1 先进行过滤 把分割后长度小于3的行过滤掉，以防止取第三个字段时数组下标越界
    rdd_filter = rdd_split.filter(lambda line: len(line) >= 3)
    # 4.2 取出下标为2的元素，同时去掉方括号
    rdd_keyword = rdd_filter.map(lambda line: line[2][1:-1])
    # ["芜湖旅游", "奥运", "360安全卫士", "哄抢救灾物资"...]
    # 4.3 对搜索词进行分词操作
    rdd_keyword = rdd_keyword.flatMap(lambda keyword: jieba.cut(keyword))
    # [["芜湖", "旅游"], ["奥运"], ["360", "安全卫士"], ["哄抢", "救灾物资"]...]
    # ["芜湖", "旅游", "奥运", "360", "安全卫士", "哄抢", "救灾物资"...]
    # 5 把关键词转成元组
    rdd_keyword_tuple = rdd_keyword.map(lambda word: (word, 1))
    # 6 进行聚合操作，统计出关键词出现的次数
    result = rdd_keyword_tuple.reduceByKey(lambda x, y: x+y)
    # 7 按照value排序
    result = result.sortBy(lambda tup: tup[1], ascending=False)
    # 8 输出结果
    print(result.take(10))

```

#### 需求二：统计每个用户每个搜索词的点击次数

```python
访问时间 用户ID  [搜索词] url在搜索结果的排名  url在页面展示的排名 url
使用SQL：select 用户ID, 搜索词, count(1) from 表 group by 用户ID, 搜索词

需求2分析思路
1 上传文件到linux hdfs
2 读取文件，textFile
3 把每一行进行分割
4 取出用户ID和搜索词
5 把用户ID和搜索词转成元组 (('用户ID', '搜索词'),1)
6 进行聚合操作，统计出关键词出现的次数

```

完整代码实现

```python
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    # 1 准备运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("sogou案例")
    sc = SparkContext(conf=conf)

    # 2 读取文件，textFile
    rdd_file = sc.textFile('file:///tmp/pycharm_project_956/004/data/SogouQ.sample')
    # 3 把每一行进行分割
    rdd_split = rdd_file.map(lambda line: line.split('\t'))
    # ["00:00:03	7954902679225404	[芜湖旅游]	7 3	www.tourunion.com/spot/city/583340.htm"
    # , "00:00:03	9717831746543397	[奥运]	7 3	www.beijing2008.com"
    # ]
    # [
    #  ["00", "795", "[芜湖旅游]", "7', "3", "www"],
    #  ["00", "971", "[奥运]"， "7", "3", "www"]
    # ]
    # 4 取出第三个字段
    # 4.1 先进行过滤 把分割后长度小于3的行过滤掉，以防止取第三个字段时数组下标越界
    rdd_filter = rdd_split.filter(lambda line: len(line) >= 3)
    # 4.2 取出下标为2的元素，同时去掉方括号
    rdd_keyword = rdd_filter.map(lambda line: ((line[1], line[2][1:-1]),1 ))
    # 5 把用户ID,搜索词转成元组，这一步可以合并到4.2中完成
    # rdd_keyword_tuple = rdd_keyword.map(lambda word: (word, 1))
    # 6 进行聚合操作，统计出关键词出现的次数
    result = rdd_keyword.reduceByKey(lambda x, y: x+y)
    # 7 按照value排序
    result = result.sortBy(lambda tup: tup[1], ascending=False)
    # 8 输出结果
    print(result.take(10))


```

#### 需求3，统计每个小时点击的次数

完整代码实现

```python
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    # 1 准备运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("sogou案例")
    sc = SparkContext(conf=conf)

    # 2 读取文件，textFile
    rdd_file = sc.textFile('file:///tmp/pycharm_project_956/004/data/SogouQ.sample')
    # 3 把每一行进行分割
    rdd_split = rdd_file.map(lambda line: line.split('\t'))
    # ["00:00:03	7954902679225404	[芜湖旅游]	7 3	www.tourunion.com/spot/city/583340.htm"
    # , "00:00:03	9717831746543397	[奥运]	7 3	www.beijing2008.com"
    # ]
    # [
    #  ["00", "795", "[芜湖旅游]", "7', "3", "www"],
    #  ["00", "971", "[奥运]"， "7", "3", "www"]
    # ]
    # 4 取出第三个字段
    # 4.1 先进行过滤 把分割后长度小于3的行过滤掉，以防止取第三个字段时数组下标越界
    rdd_filter = rdd_split.filter(lambda line: len(line) >= 3)
    # 4.2 取出下标为0的元素，并且只要其中的小时部分
    rdd_keyword = rdd_filter.map(lambda line: (line[0].split(':')[0], 1))
    # 5 把用户ID,搜索词转成元组，这一步可以合并到4.2中完成
    # rdd_keyword_tuple = rdd_keyword.map(lambda word: (word, 1))
    # 6 进行聚合操作，统计出关键词出现的次数
    result = rdd_keyword.reduceByKey(lambda x, y: x+y)
    # 7 按照value排序
    result = result.sortBy(lambda tup: tup[1], ascending=False)
    # 8 输出结果
    print(result.take(10))


```



### 2.2 点击日志案例

### 1 数据集介绍

![image-20230521171027262](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/004/2%20%E7%AC%94%E8%AE%B0/assets/image-20230521171027262.png)

```properties
1- ip地址: 
2- 用户标识cookie信息(- - 标识没有)
3- 访问时间(时间,时区)
4- 请求方式(get / post /Head ....)
5- 请求的URL路径
6- 请求的协议
7- 请求状态码: 200 成功
8- 响应的字节长度
9- 来源的URL( - 标识直接访问, 不是从某个页面跳转来的)
10- 访问的浏览器标识

```

#### 需求一：统计pv和uv的数量

pv page view 页面访问数

uv unique visitor 独立访客访问数

```properties
统计pv，有多少行就有多少pv
1 上传文件到hdfs linux目录
2 读取文件 textFile
3 直接count即可得到pv

uv 统计去重后的ip有多少个
2 读取文件
3 取出ip
4 去重操作
5 统计去重的ip数量

如果不去重如何解决？
按照ip进行分组，统计分组的个数

```

完整代码实现

```python
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    # 1 准备运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("pyspark案例")
    sc = SparkContext(conf=conf)

    # 2 读取文件 textFile
    rdd_file = sc.textFile('file:///tmp/pycharm_project_956/004/data/access.log')
    # 3 直接count即可得到pv
    print(rdd_file.count())

    # 统计uv 方法1
    # 3 取出ip  4 去重操作
    rdd_ip_distinct = rdd_file.map(lambda line: (line.split()[0])).distinct()
    # 5 统计去重的ip数量
    print(rdd_ip_distinct.count())
    print(rdd_ip_distinct.map(lambda ip: 1).reduce(lambda x, y: x+y))

    # 统计uv 方法2
    # 3 取出ip
    rdd_ip = rdd_file.map(lambda line: (line.split()[0], 1))

    # 4 对ip进行分组
    print(rdd_ip.groupByKey().count())


```

#### 需求2：统计每个url访问次数，取出top(10)

```python
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    # 1 准备运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("pyspark案例")
    sc = SparkContext(conf=conf)

    # 2 读取文件 textFile
    rdd_file = sc.textFile('file:///tmp/pycharm_project_956/004/data/access.log')
    print(rdd_file.collect())

    # 3 处理数据
    result = rdd_file.map(lambda line: line.split())\
        .filter(lambda words_list: len(words_list)>=11)\
        .map(lambda words_list: (words_list[10], 1))\
        .reduceByKey(lambda x, y: x+y)
    # ["111.192.165.229 - - [19/Sep/2013:06:20:16 +0000] "POST /wp...", "111.192.165.229 - - [19/Sep/2013:06:20:16 +0000] "POST /wp..."]
    # [["111.192.165.229", "-", "-",  "[19/Sep/2013:06:20:16",  "+0000]", '"POST', '/wp"' ...],
    # ["111.192.165.229", "-", "-",  "[19/Sep/2013:06:20:16",  "+0000]", '"POST', '/wp"' ...]
    # ...
    # ]

    # 4 取出前10个
    print(result.sortBy(lambda tup: tup[1],ascending=False).take(10))`

```



Spark是基于内存的计算引擎

RDD（弹性分布式数据集）把这个RDD想成一个列表

```
1 创建运行环境
conf = SparkConf().appName("app").setMaster("local[*]")
sc = SparkContext(conf=conf)

2 读取文件
rdd_file = sc.textFile("文件路径（file:// 或者 hdfs://node1:8020/xxxx）")

3 实现需求
transformation
action
本质上就是函数

4 有难点的地方，想一下python该如何实现


```

所有的高阶函数（transformation action），都需要接收一个自定义函数，高阶函数内部相当于实现了for循环，可以想成在高阶函数内部循环调用接收的函数，最终得到结果



# 005 PySpark笔记

今日内容:

- 1- RDD的持久化:  缓存 和 checkpoint
- 2-RDD的共享变量: 广播变量  和 累加器
- 3-RDD内核调度原理



## 1. RDD的持久化

### 1.1 RDD的缓存

```properties
为什么需要缓存:
当RDD产生时，代价都是比较大的（从磁盘中读取（耗时较多）通过复杂计算（消耗资源）），RDD可能会被多方使用，默认情况每次使用RDD都会重新计算一遍（RDD本身不存储数据的）。期望RDD能够不用重复计算，提升执行的效率。
可以将RDD设置成缓存，后续使用时不用再重复计算。

Spark有容错能力，如果某个RDd计算失败（失败的原因有很多，例如当前资源不够）需要对整个RDD链进行回溯。如果有缓存，则可以继续从缓存的地方继续进行计算。只用从环境的地方计算，不用从头计算。

缓存使用的场景：
1 当RDD重复使用时，避免重复计算，可以用缓存解决
2 当RDD计算代价很大时，设置缓存
3 当需要提升容错能力时，设置缓存

注意事项：
1 缓存是一种临时存储，可以把数据存在内存中，磁盘中
2 由于缓存时临时存储机制，所以数据有可能会丢失。所以缓存不会清理RDD之间的依赖关系，万一缓存丢失，还可以从头计算一遍。
3 rdd的transformation操作都是惰性，所以并不会立即触发，如果需要立即触发，则可以调用一个action操作，一般推荐count()
```

```properties
缓存如何使用：
相关API
rdd.cache() 缓存默认设置在内存中，本质就是调用persist函数
rdd.persist() 缓存默认设置在内存中，可以设置缓存其他位置，


DISK_ONLY: ClassVar[StorageLevel] 仅存在磁盘中
DISK_ONLY_2: ClassVar[StorageLevel] 仅存在磁盘中，保存两份
MEMORY_ONLY: ClassVar[StorageLevel] 仅存在内存中
MEMORY_ONLY_SER: 先进行序列化操作，再进行缓存，占用空间少，花费时间多
MEMORY_ONLY_2: ClassVar[StorageLevel] 
DISK_ONLY_3: ClassVar[StorageLevel]
MEMORY_AND_DISK: ClassVar[StorageLevel] 有限保存到内存中，内存不够可以保存至磁盘中
MEMORY_AND_DISK_2: ClassVar[StorageLevel]
OFF_HEAP: ClassVar[StorageLevel]

序列化：把对象转成二进制，序列化后占用空间少，是需要时间进行转换的
反序列化：把二进制转成对象
```

添加缓存的示例代码

```python
import os
from pyspark import SparkConf, SparkContext, StorageLevel
import time

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    # 1 准备运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("pyspark案例")
    sc = SparkContext(conf=conf)

    # 2 读取文件 textFile
    rdd_file = sc.textFile('file:///tmp/pycharm_project_956/004/data/access.log')
    # 3 直接count即可得到pv
    rdd_file.cache() # 这里添加缓存，在DAG图中，有绿色点，表示缓存
    rdd_file.persist(storageLevel=StorageLevel.DISK_ONLY) # 设置缓存后，一般立即执行action操作，才是真正计算出rdd的结果（就是一个transformation的行为）
    print(rdd_file.count())

    # 统计uv 方法1
    # 3 取出ip  4 去重操作
    rdd_ip_distinct = rdd_file.map(lambda line: line.split()[0]).distinct()
    # 5 统计去重的ip数量
    print(rdd_ip_distinct.count())

    # 统计uv 方法2
    # 3 取出ip
    rdd_ip = rdd_file.map(lambda line: (line.split()[0], 1))

    # 4 对ip进行分组
    print(rdd_ip.groupByKey().count())


    time.sleep(10000)
    sc.stop()
```

在Job的DAG图中有绿色的点，则表名缓存生效

![image-20230522111356800](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522111356800.png)

可以在以下位置查看缓存

![image-20230522111652446](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522111652446.png)

### 1.2 RDD的checkpoint检查点

```properties
检查点和缓存类似的，也是把rdd的结果进行存储的操作。一般把结果存储在hdfs中，数据存储就更加可靠（利用hdfs的存储机制），所以检查点会清楚之前的RDD的依赖关系（因为数据存储更加可靠）。

checkpoint出现之后是否可以提升执行效率？
相对来说并不会提升太多的执行效率，但是可以提升缓存数据的安全性

可以想象成虚拟机的快照，或者游戏的存档

checkpoint是由spark进行设置的，但是存在hdfs中，需要手动删除。即使程序停止、运行结束也不会自动删除。但是这个保存的结果在程序下一次运行时没什么帮助的。


如何设置检查点：
1，使用SparkContext.setCheckpointDir设置检查点保存的目录
2，rdd.checkpoint()必须在任何一个action操作之前被调用
3，最好设置rdd缓存在内存中，否则设置检查点时会重新计算
4，设置完成后调用action操作，立即触发

```

检查点示例代码

```python
import os
from pyspark import SparkConf, SparkContext, StorageLevel
import time

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    # 1 准备运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("pyspark案例")
    sc = SparkContext(conf=conf)

    # 设置检查点目录
    sc.setCheckpointDir('hdfs://node1:8020/spark/checkpoint')

    # 2 读取文件 textFile
    rdd_file = sc.textFile('file:///tmp/pycharm_project_956/004/data/access.log')

    rdd_split = rdd_file.map(lambda line : line.split())

    rdd_filter = rdd_split.filter(lambda words_list: len(words_list)>=11)

    rdd_filter.checkpoint() # 如果在checkpoint之前执行了action操作，检查点无效
    rdd_filter.cache() # 在Memory中进行缓存
    rdd_filter.count() # 设置检查点，并且立即触发

    rdd_ip = rdd_filter.map(lambda words_list: words_list[0])

    rdd_ip.distinct().count()

    rdd_url = rdd_filter.map(lambda words_list: words_list[10])

    rdd_url.distinct().count()


    time.sleep(10000)

    sc.stop()
```





![image-20230522115132601](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522115132601.png)

缓存和检查点区别

```properties
1，存储位置
缓存：内存和磁盘都可以存储
检查点：hdfs中，也可以存在本地磁盘，仅限于使用local模式运行，local模式运行时没有必要取设置检查点

2，依赖关系
缓存：缓存不会删除依赖关系，缓存是临时存储，不能删除依赖关系，防止回溯计算
检查点：会删除（截断）依赖关系，因为检查点把数据存在hdfs中，认为是安全的

3，生命周期
缓存：当程序运行完成后，缓存会被自动清理。也可以手动清理，unpersist()函数进行清理
检查点：保存在hdfs中，并且不会自动删除的，需要手动删除。即使不删除检查点，下一次运行也没有用了

实际使用时，是用checkpoint还是缓存
两个可以同时使用，官方强烈建议
rdd_filter.cache() # 在Memory中进行缓存
rdd_filter.checkpoint() # 设置检查点，如果在checkpoint之前执行了action操作，检查点无效

```





## 2. RDD的共享变量

![image-20230522142952175](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522142952175.png)

### 2.1 广播变量

```properties
广播变量
作用:减少Driver和Executor之间的数据传输，减少每个线程中内存的占用
使用场景: 多个线程使用同一个变量时


如果不设置广播变量，则每个线程都会拷贝一份这个变量的副本，拷贝时占用带宽，拷贝完成会占用过多的内存

解决方案:让executor从driver中拉取一个副本，不需要每个线程中的都持有者个副本，可以减少带宽和内存

广播变量只能读取，不能修改


如何使用:
在driver中创建
a = 100
广播变量名= sc.broadcast(a)

获取变量值
广播变量名.value
```

示例代码

```python
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    # 1 准备运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("pyspark案例")
    sc = SparkContext(conf=conf)

    # 2 定义广播变量
    a = 1000
    bc_a = sc.broadcast(a)

    # 3 使用广播变量
    rdd1 = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 5)


    rdd2 = rdd1.map(lambda x: x+bc_a.value)

    print(rdd2.collect())
```



### 2.2 累加器

```properties
累加器在多个线程中对同一个变量提供累加的操作，对于每个线程只能写数据到这个累加器，不能读取累加器的值，读取的操作只能由Driver进行。

应用场景：全局中需要进行累加的场景

如何使用
1 在Driver中设置一个累加器
累加器变量名 = sc.accumulator(初始值)
2 在线程中进行累加操作
累加器变量名.add(累加值)
3 在Driver中获取
累加器变量名.value

```

累加器代码

```python
import os
from pyspark import SparkConf, SparkContext

import time
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    # 1 准备运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("pyspark案例")
    sc = SparkContext(conf=conf)

    # 2 定义累加器
    acc = sc.accumulator(100)

    # 3
    rdd = sc.parallelize([1,2,3,4,5])


    rdd1 = rdd.map(lambda x: acc.add(x)) # 进行累加操作
    print(rdd1.collect()) # 必须通过action操作触发，才会执行累加的过程，否则不会得到预取的结果

    print(acc.value)

    time.sleep(10000)

```

累加器现象

```properties
如果对设置累加器的RDD进行多次的action操作，则这个累加器会累加多次
如何避免？当累加器执行完成RDD可以设置一个缓存或者检查点，可以避免多次累加

原因：当调用action操作时，会产生多个Job（任务），RDD不存储值，只存储规则，每个任务执行时，对应累加器都会进行累加操作，累加器是全局性的，所以有多少个action就有多少次累加。

```



## 3. RDD的内核调度

### 3.1 RDD的依赖

```properties
RDD之间存在依赖关系，这个也是RDD非常重要的特性
RDD的依赖关系可以分成窄依赖和宽依赖


```

窄依赖：

作用：可以让各个分区中的数据进行并行运算

定义：父RDD的某个分区被子RDD的某个分区全部继承

![image-20230522152533085](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522152533085.png)

宽依赖：

作用：划分stage

定义：父RDD的一个分区被子RDD的多个分区接收并处理

![image-20230522152931677](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522152931677.png)

判断两个RDD之间是宽依赖的依据就是是否发生shuffle，一旦产生shuffle，必须前面的RDD计算完成后才能计算后面的RDD

```properties
Spark中每个算子是否会产生shuffle，由Spark本身决定，实现spark框架代码时就已经确定了。
map一定不会产生shuffle，reduceByKey一定会产生shuffle

如何判断某个算子是否有shuffle，如果执行到某个算子，执行流程被断开生产新的stage，则说明该算子会产生shuffle。也可以查看官方文档。

不用纠结这个问题，该用什么就用什么算子。以实现最终的需求为目的。

会产生shuffle的算子分在前面这个stage中


```

![image-20230522154503027](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522154503027.png)

### 3.2 DAG与stage

DAG有向无环图

整个的流程是有方向的，但是不会形成一个圆环

如何形成DAG

```properties
第一步：driver程序处理rdd相关代码时，分析代码，遇到action操作时，就会把action依赖的rdd统一进行分析

第二步：对整个阶段进行回溯，回溯时判断依赖关系，如果宽依赖就会形成一个新的stage

```

![image-20230522160902899](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522160902899.png)

![image-20230522164613601](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522164613601.png)

```
1 有几个action操作就有几个Job
2 针对一个Job进行分析，最开始就可以确定Job的线程数量，Task就确定
3 还可确定这个Job中Stage（每个Job中的Stage也是各不相同）
4 从上一个Stage到下一个Stage RDD分区数也可确定

```



### 3.3 RDD的shuffle

```properties
shuffle发展历史
1 1.1之前采用 hash shuffle
2 1.1之后引入sort shuffle 主要增加合并和排序操作，对hash shuffle进行优化
3 1.5 钨丝计划 提升cpu和内存效率
4 1.6 合并到sort shuffle
5 2.0 删除hash shuffle 全部合并到sort shuffle

```

![image-20230522165430479](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522165430479.png)

优化前的hash shuffle

![image-20230522165507094](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522165507094.png)

```properties
shuffle过程
1 父RDD进行shuffle时会在各自的分区中产生和子RDD分区数量相等的文件数量（在内存中放不下时）每个文件对应子RDD的一个分区，shuffle结束后，子RDD的分区从对应文件获取数据

有问题
父RDD产生的文件数量太多，并且都是小文件
对应磁盘IO增大，打开关闭文件的次数就增多
子RDD读取时也会遇到相同问题

```

![image-20230522170206573](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522170206573.png)

```properties
由executor进行统一的管理，产生和子RDD分区数量相同的文件数量
线程数据数据时输出到对应的文件即可
子RDD拉去数据时只用拉去分区对应的数据即可

```

sort shuffle

![image-20230522172014740](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522172014740.png)

```properties
父RDD把线程中的数据分好区之后写入到内存中，当内存中数据达到阈值（1w），触发溢写操作，把数据写入到小文件中，当父RDD的操作完成后，需要对小文件进行合并，最终合并成一个大文件，大文件有一个对应的索引文件。子RDD获取数据时，可以通过索引文件快速找到对应数据，进行拉取。

```

```properties
sort shuffle有两种运行机制
普通运行机制，带有排序操作，父RDD把线程中的数据分好区之后写入到内存中，当内存中数据达到阈值（1w），触发溢写操作，把数据写入到小文件中，写入过程中会进行排序操作。当父RDD的操作完成后，需要对小文件进行合并，最终合并成一个大文件，大文件有一个对应的索引文件。子RDD获取数据时，可以通过索引文件快速找到对应数据，进行拉取。

bypass机制，没有排序操作，并不是所有的sort shuffle都会走bypass机制
需要满足条件：
1 上游RDD分区数量小于200
2 上游不能提前进行聚合操作

bypass机制效率会更高一些


```



### 3.4 JOB调度流程

```properties
一个JOB(任务)和一个action操作是对应的

Driver调度方案
Driver会构建的四个核心对象：SparkContext DAGscheduler TaskScheduler scheduleBackend
1 当main运行时，首先创建SparkContext 同时在底层会创建DAGscheduler 和Task
Scheduler
2 当driver处理RDD相关代码时遇到一个action操作就会触发一个Job，有一个action就有一个Job
3 当触发了Job后，driver负责分配工作（构造DAG，确定每个stage中有多少个线程，每个线程分别在哪个executor中运行）
3.1 driver中运行DAGschudler，进而确定任务有几个stage，每个stage中有多少线程，把每个stage中运行的线程封装到TaskSet的列表中，有多少个Stage，就有多少个TaskSet，把TaskSet发给TaskScheduler
3.2 接下来就有TaskScheduler进行处理，根据接收的TaskSet中描述的线程信息，发送线程任务到executor中，由它进行执行。尽可能保证每个分区的task任务运行在不同的executor中，确保资源运行最大化。资源是由TaskScheduler进行申请

```



![image-20230522173347572](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/005/2%20%E7%AC%94%E8%AE%B0/assets/image-20230522173347572.png)

```
一个spark程序可以产生多个Job,由几个action就有几个Job,每个Job都会产生一个DAG,根据DAG就可以分出多个stage,每个stage再根据RDD的分区数量产生线程数量(Task)

```



### 3.5 Spark的并行度

```properties
spark的并行度,决定了spark执行效率的很重要的因素.
spark并行度取决于
1 资源: 提交任务时设置executor的数量以及每个executor的cpu memery
2 数据: 根据数据大小 分区数量

当申请资源比较大,但是数据量不大,虽然不会影响当前任务的效率,但是对资源是浪费的
申请资源较小,但是数据量很大,无法充分利用集群的并行能力

需要进行反复的调试
一般 一个cpu核 运行2~3线程 一个cpu核对应3~5G


命令行提交时
--conf "spark.default.parallelism=10" 这里是针对集群设置的,如果是local,local[*]设置

这里并行度决定shuffle后的分区数量

```

!image-20230522180245562](assets/image-20230522180245562.png)

![1685694487096](C:\Users\15263\AppData\Roaming\Typora\typora-user-images\1685694487096.png)

### 3.6 了解combineByKey

```properties
reduceByKey(fn)
foldByKey(defaultValue, fn)
aggregateByKey(defaultValue, fn1, fn2)
combineByKey(fn1, fn2, fn3) fn1设置初始值,fn2对每个分区进行聚合,需要结合初始值,fn3对每个分区的结果进行聚合,需要结合初始值

```

```
[('a', 1),('a', 2),('b', 3),('b', 4),('c', 5),('d', 6)]
从上面的数据得到下面的结果
[
('a', [1,2]),
('b', [3,4]),
('c', [5]),
('d', [6])
]

```



```properties
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("pyspark")
    # 1 准备运行环境
    conf = SparkConf().setMaster("local[*]").setAppName("pyspark案例")
    sc = SparkContext(conf=conf)

    # 2
    rdd1 = sc.parallelize([('a', 1),('a', 2),('b', 3),('b', 4),('c', 5),('d', 6),('a', 7)])
    # print(rdd1.groupByKey().mapValues(list).collect())
    # [('b', [3, 4]), ('c', [5]), ('d', [6]), ('a', [1, 2])]

    # ('a', 1), ('a', 2) [1,2]
    # ('a', 7) [7]
    # ('b', 3), ('b', 4)
    # ('c', 5)
    # ('d', 6)
    #
    def fn1(agg1:list):
        return [agg1]

    def fn2(agg2:list, curr2:list):
        agg2.append(curr2)
        return agg2

    def fn3(agg3:list, curr3:list):
        agg3.extend(curr3)
        return agg3


    print(rdd1.combineByKey(fn1, fn2, fn3).collect())
    # [('b', [3, 4]), ('c', [5]), ('d', [6]), ('a', [1, 2, 7])]


```



# 006_PySpark笔记

今日内容:

- 1- Spark SQL的基本概念
- 2- Spark SQL的入门案例
- 3- dataFrame对象详细说明: 



## 1- SparkSql的基本概念

### 1.1 了解什么是Spark SQL

​	SparkSQL 处理海量的结构化数据的spark模块

```properties
结构化数据：
csv，mysql表，json，xml
每一行中字段的个数据是固定，每一列中的类型是固定的
能够使用excel表存储的数据就是结构化数据

id(int) name(string) gender(string) age(int)
1 张三 男 20
2 李四 女 25
3 王五 男 28
4 赵六 女 26
是否是结构化数据，是


SparkSQL处理结构化数据 Spark RDD可以处理任意数据
SparkSQL中的核心数据结构DataFrame：数据（RDD）+ 表结构（schema） SparkCore中非常重要的数据结构RDD
有表结构后，spark可以批量的处理数据，从而提升处理效率


id(int) name(string) gender(string) age(int)
[(1, 张三, 男, 20),(2, 李四, 女, 25), (3, 王五, 男, 28), (4, 赵六, 女, 26)]
```

为什么学习SparkSQL

```properties
特点
1 融合性 可以写sql可以写代码，可以混合用
2 统一的接口，使用spark sql可以访问各种数据源，hive mysql csv json，获取数据后可以使用同一套的api及逆行数据的处理和分析
3 hive兼容，使用spark直接替换hive的MR计算引擎
4 支持标准化连接，jdbc odbc都可以
```

```properties
1 sql比较简答，sql更加通用，写sql是没问题
2 spark sql兼容hive 把spark sql和hive进行集成，替换hive mr计算引擎
3 spark sql不仅可以写sql也可以写代码（通过调用函数的方式）进行数据处理和分析
4 spark sql可以处理大规模数据（底层仍然是基于RDD）
```





### 1.2 Spark SQL的发展史

pandas库，DataFrame

![image-20230524090416868](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/006/2%20%E7%AC%94%E8%AE%B0/assets/image-20230524090416868.png)

Spark2.0后把dataframe和dataset合二为一，统一称为dataset，泛型（python没有）

列表[T]，会存储什么类型的数据暂时无法确定，仍然保留dataframe概念，当dataframe提交到spark中运行时，还是会被转成dataset。

### 1.3 Spark SQL与hive异同

```properties
相同点
1 都可以使用sql进行数据统计分析
2 都可以处理大规模的数据
3 都可以处理结构化数据
4 spark sql和hql都可以提交到yarn中运行
```

```properties
不同点
1 spark sql基于内存进行迭代 hive只能基于磁盘进行迭代
2 hive只能写sql，spark sql可以写sql还可写dsl（domain specified language，就是spark框架封装的函数）
3 hive有一个元数据库metastore，spark sql没有元数据库，是自身进行管理的
4 hive底层是mr，spark sql底层是RDD
```



### 1.4 Spark SQL的数据结构对比

pandas中的dataframe，可以加载csv json excel等格式，统一生成dataframe对象，dataframe提供相同的接口对数据进行处理。只能在本机运行，无法在集群运行，只能处理小规模数据（G级别数据）

spark core中rdd，没有表结构，不会区分结构化非结构化数据

spark sql中的dataframe，有表结构，分布式计算，（T级别必须使用分布式计算引擎）

![image-20230524100514912](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/006/2%20%E7%AC%94%E8%AE%B0/assets/image-20230524100514912.png)

![image-20230524101630141](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/006/2%20%E7%AC%94%E8%AE%B0/assets/image-20230524101630141.png)



```properties
RDD:[(1, 张三, 男, 20),(2, 李四, 女, 25), (3, 王五, 男, 28), (4, 赵六, 女, 26)]
无法提前获知每个元素的内部结构（字段数量，类型等），RDD表示的是具体的数据对象，每个RDD都是一个数据集

DataFrame: 
id(int) name(string) gender(string) age(int)
1 张三 男 20
2 李四 女 25
3 王五 男 28
4 赵六 女 26
有表结构，所以能够知道每个元素（RDD）字段类型 名称，就可以把每个字段拆解出来，形成一张二维表
dataframe是在python中使用，python中没有泛型的支持的，继续使用dataframe，java scala有泛型的语言中使用dataset

DataSet: 在DataFrame的基础上增加泛型的支持，每一行都可以表示成一个泛型
```

![image-20230524102547286](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/006/2%20%E7%AC%94%E8%AE%B0/assets/image-20230524102547286.png)

![image-20230524102852440](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/006/2%20%E7%AC%94%E8%AE%B0/assets/image-20230524102852440.png)

## 2. Spark SQL的入门案例

### 2.1 Spark SQL的统一入口

SparkSession是在spark2.0后新的接口，可以由SparkSession创建出SparkContext对象，并且可以由SparkSession进行相关的编程操作

dataframe = rdd + schema（表结构），底层会调用rdd的接口，有一些优化引擎，dataframe代码更加简单高效。

spark core的入口：SparkContext

spark sql的入口：SparkSession（包括SparkContext）

![image-20230524105105820](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/006/2%20%E7%AC%94%E8%AE%B0/assets/image-20230524105105820.png)



```python
# SparkContext和SparkSession关系
# 命令行启动时，会自动创建好这两个对象，sc spark
Spark context available as 'sc' (master = local[*], app id = local-1684896681198).
SparkSession available as 'spark'.

# 可以直接通过SparkSession获取到sparkContext对象
>>> sc1 = spark.sparkContext
>>> rdd1 = sc1.parallelize([1,2,3,4,5])
>>> rdd1
ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
>>> rdd1.collect()
[1, 2, 3, 4, 5]                                                                 

```

构造SparkSession代码

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import time

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    # 1 构造spark运行环境
    spark: SparkSession = SparkSession\
        .builder\
        .master('local[*]')\
        .appName('pyspark')\
        .getOrCreate()

    sc: SparkContext = spark.sparkContext


    #
    print(spark)
    print(sc)

    time.sleep(10000)
```



以下错误是没有启动HDFS

![image-20230524110310355](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/006/2%20%E7%AC%94%E8%AE%B0/assets/image-20230524110310355.png)

### 2.2 Spark SQL的入门案例

- 1 准备数据，并且上传到虚拟机

  ```csv
  id name gender age
  1 张三 男 20
  2 李四 女 25
  3 王五 男 28
  4 赵六 女 26
  
  text csv(分隔符是空格)
  
  ```

- 2 创建新的代码文件，实现需求：找出数据中年龄大于25岁人

  ```python
  # stu
  # select * from stu where age > 25;
  
  # comma seperated value csv
  
  from pyspark.sql import SparkSession, DataFrame
  from pyspark import SparkContext, SparkConf
  import os
  import time
  
  os.environ['SPARK_HOME'] = '/export/server/spark'
  os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
  os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
  os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'
  
  if __name__ == '__main__':
      # 1 构造spark运行环境
      spark: SparkSession = SparkSession \
          .builder \
          .master('local[*]') \
          .appName('pyspark') \
          .getOrCreate()
  
      sc: SparkContext = spark.sparkContext
  
      # 2 之前使用sc.textFile()加载文件生产RDD对象
      # 使用spark.read读取数据
      # :DataFrame 指定df的类型，可以省略，不是强制性
      df: DataFrame = spark.read.csv(path='file:///tmp/pycharm_project_956/006/data/stu.csv',
                     sep=' ', header=True, inferSchema=True)
  
      print(df)
      df.show()
      df.printSchema()
      
      # spark.read.csv中的参数
      # sep 指定csv文件中的分隔符
      # header 是否把第一行加载成表头
      # inferSchema 设置为True之后，可以自动推断字段的类型
      # df.show() 打印dataframe的内容
      # df.printSchema() 打印表结构
      # 这两个函数都没有返回值
  
  ```

  实现需求

  ```python
  # 找出数据中年龄大于25岁人
  from pyspark.sql import SparkSession, DataFrame
  from pyspark import SparkContext, SparkConf
  import os
  import time
  
  os.environ['SPARK_HOME'] = '/export/server/spark'
  os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
  os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
  os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'
  
  if __name__ == '__main__':
      # 1 构造spark运行环境
      spark: SparkSession = SparkSession \
          .builder \
          .master('local[*]') \
          .appName('pyspark') \
          .getOrCreate()
  
      sc: SparkContext = spark.sparkContext
  
      # 2 之前使用sc.textFile()加载文件生产RDD对象
      # 使用spark.read读取数据
      df: DataFrame = spark.read.csv(path='file:///tmp/pycharm_project_956/006/data/stu.csv',
                     sep=' ', header=True, inferSchema=True)
  
      print(df)
      df.show()
      df.printSchema()
  
      # 3 使用DSL实现需求
      df_25: DataFrame = df.where('age > 25')
      df_25.show()
  
      # 3 使用sql实现
      df.createTempView('stu')
      spark.sql('select * from stu where age > 25').show()
      
      # 建议使用DSL进行代码编写，写sql需要进行翻译的工作
  
  ```

  



## 3. DataFrame详解

### 3.1 DataFrame基本介绍

​	![image-20230524143736780](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/006/2%20%E7%AC%94%E8%AE%B0/assets/image-20230524143736780.png)

dataframe是一个二维表，有字段名称，字段的类型

```properties
dataframe = RDD + schema
dataframe中的表结构，class:`pyspark.sql.types.StructType` or str是这两种类型
第一种：字符串，schema='id string, name string, gender string, age int'
第二种：StructType构造 schema = StructType([StructField("id", StringType(), True),
                         StructField("name", StringType(), True),
                         StructField("gender", StringType(), True),
                         StructField("age", IntegerType(), True),
                         ])
       StructType是DataFrame中表示schem的核心对象，StructFeild表示每个字段（名称，类型，可否为空）
       StrucType可以包含多个StructFeild，取决字段个数
       
       StructType代表了一个Row对象，Row对象封装在RDD中
       
       # 2 StuctType的add方法构造StructType
       schema = StructType()\
        .add(StructField("id", StringType(), True))\
        .add(StructField("name", StringType(), True))\
        .add(StructField("gender", StringType(), True))\
        .add(StructField("age", IntegerType(), True))


```

![image-20230524150344854](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/006/2%20%E7%AC%94%E8%AE%B0/assets/image-20230524150344854.png)



### 3.2 DataFrame的构建方式

构建dataframe1

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import time
from pyspark.sql.types import *

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    # 1 构造spark运行环境
    spark: SparkSession = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('pyspark') \
        .getOrCreate()

    sc: SparkContext = spark.sparkContext

    # 2 使用RDD构建dataframe
    # data的实现方式1
    l = [('bd17', '张三', 90), ('bd17', '李四', 91), ('bd17', '王五', 92), ('bd17', '赵六', 93)]
    # data的实现方式2
    rdd = sc.parallelize(l)

    # 表结构实现方式1
    schema = "cname string, sname string, score int"
    # 表结构实现方式2
    # schema = ['cname', 'sname', 'score']
    # 表结构实现方式3
    # schema = StructType().add(StructField('cname', StringType(), True))\
    #                     .add(StructField('sname', StringType(), True))\
    #                     .add(StructField('score', IntegerType(), True))
    # 表结构实现方式4
    # schema = StructType([StructField('cname', StringType(), True),
    #             StructField('sname', StringType(), True),
    #             StructField('score', IntegerType(), True)])
    # dataframe = rdd + schema

    # df = spark.createDataFrame(rdd, schema=schema) # 使用RDD构建dataframe
    # df = spark.createDataFrame(l, schema=schema) # 使用列表构建dataframe

    df = spark.createDataFrame(l, schema=schema) # 不指定表结构


    # 3 查看dataframe
    df.show()
    df.printSchema()
    print(df.rdd.collect())


```

```properties
适用场景：
如果数据是板结构化数据，希望适用SparkSQL进行处理，可以提前先转成RDD，适用RDD中的算子对数据进行预处理工作。然后再转成dataframe进一步处理。

```

使用pandas的dataframe转成sparkDataframe

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import time
import pandas as pd

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    # 1 构造spark运行环境
    spark: SparkSession = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('pyspark') \
        .getOrCreate()

    sc: SparkContext = spark.sparkContext

    # 通过pandas读取数据，生成一个pandas 的dataframe
    # df_pandas = pd.read_csv('/tmp/pycharm_project_956/006/data/stu.csv', sep=' ')
    df_pandas = pd.DataFrame({'id':[1,2,3,4], 'name':['zs', 'ls', 'ww', 'zl']})
    print(df_pandas)

    df_spark = spark.createDataFrame(df_pandas)

    df_spark.show()
    df_spark.printSchema()
    print(df_spark.rdd.collect())

```

```properties
如果数据源比较特殊，比如Excel格式，一般先使用pandas读取生成一个pandas的dataframe，再把这个dataframe转成spark的dataframe。

```

spark加载外部文件生成dataframe

```properties
完整的语法格式:

SparkSession对象.read.format('csv|json|text|parquet|jdbc|orc...')
					.option('参数', '取值') 
					# option('sep', ' ') 
					# option('inferSchema', 'true') 
					# option('schema', 'id string, name string, score int')
					.schema(schema) # 设置表结构信息
					.load(文件路径)
					
					
spark[SparkSession对象].read.csv() # 之前加载csv文件用的这个接口

```

读取csv文件完整代码

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, SparkConf
import os
import time

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    # 1 构造spark运行环境
    spark: SparkSession = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('pyspark') \
        .getOrCreate()

    sc: SparkContext = spark.sparkContext

    df:DataFrame = spark.read\
        .format('csv')\
        .option('sep', ' ')\
        .option('inferSchema', 'true').schema('id int, name string, gender string, score int')\
        .load('file:///tmp/pycharm_project_956/006/data/stu1.csv')
    df.show()
    df.printSchema()
    print(df.rdd.collect())

```

读取text数据完整代码

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, SparkConf
import os
import time

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    # 1 构造spark运行环境
    spark: SparkSession = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('pyspark') \
        .getOrCreate()

    sc: SparkContext = spark.sparkContext

    df: DataFrame = spark.read.format('text').load('file:///tmp/pycharm_project_956/006/data/stu1.csv')

    df.show()
    df.printSchema()
    print(df.rdd.collect())

```

读取json文件完整代码

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import time

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    # 1 构造spark运行环境
    spark: SparkSession = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('pyspark') \
        .getOrCreate()

    sc: SparkContext = spark.sparkContext

    # 2 读取文件
    df = spark.read.format('json').load('file:///tmp/pycharm_project_956/006/data/people.json')
    df.show()
    df.printSchema()
    print(df.rdd.collect())


```

以上不同格式文件读取，都有对应简化版代码

```python
spark.read.csv(path='xxx', sep=' ', inferSchema=True, schema='id int, name string')
spark.read.text
spark.read.json


```



### 3.3 DataFrame的相关API

dataframe的api有两种风格：DSL(domain specified language) SQL

```properties
DSL: dataframe封装了很多api，比较简单，api的名称和sql中的关键字同名，例如df.where 等价于sql中where条件查询
SQL: 编写sql语句进行数据处理分析

SQL使用更容易上手，但是DSL效率会更高一些。sql语句还是会被翻译成对应的函数，损失效率。


```

DSL相关的API，已经有dataframe了，然后通过这个dataframe调用某些函数进行处理和分析工作

- show() 函数，展示dataframne内容，默认最多展示20行，truncate参数，是否截断输出

  ![image-20230524173548937](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/006/2%20%E7%AC%94%E8%AE%B0/assets/image-20230524173548937.png)

- printSchema() 打印表结构

- select()函数，dataframe中选择指定列

  ```python
      df.select('address').show()  # 接收列名的字符串
      df.select('address','age').show() # 接收多个列名的字符串
      df.select(['address','age']).show() # 接收列表，里面存放多个列名
      df.select(df['address'],df['age']).show() # 接收Column对象
      df.select(df.address, df.age).show() # 接收Column对象
  
  ```

- where和filter，where是filter的别名，都是接收一个条件对行进行过滤

- groupby 分组函数，分组后一般会接聚合函数agg，agg接收具体的聚合操作

  ```python
      df.groupby('gender').agg(F.max('age').alias('max_age'),
                               F.mean('age').alias('avg_age'),
                               F.count('name').alias('cnt')
                               ).show()
      
  # 结果如下
  +------+-------+-------+---+
  |gender|max_age|avg_age|cnt|
  +------+-------+-------+---+
  |    男|     30|   25.0|  2|
  |    女|     19|   19.0|  1|
  +------+-------+-------+---+
  
  
  使用F之前需要导包，import pyspark.sql.functions as F
  
  可以在这里查看spark sql支持的函数
  https://spark.apache.org/docs/3.1.2/api/sql/index.html
  
  ```

SQL风格的API

```properties
1 使用SQL之前必须把df注册成一张表(视图)
2 使用spark.sql('编写sql语句') # 这个spark变量，是最开始创建的SparkSession对象

```

![image-20230524180007193](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/006/2%20%E7%AC%94%E8%AE%B0/assets/image-20230524180007193.png)

```python
# 示例代码
    df.createTempView('stu')
    spark.sql('select gender, max(age) as max_age, avg(age) as avg_age, count(name) as cnt from stu group by gender').show()


```



### 3.4 综合案例

#### 3.4.1 词频统计分析案例

读取hdfs://node1:8020/word.txt文件中的单词，统计出每个单词的个数

```
1 使用spark.read.text方法读取数据，得到dataframe，有一个列，列名value "hello you Spark Flink"
2 对每一行进行分割，调用F.split函数对dataframe中value列进行处理 "hello","you","spark","Flink"，这些都在同一行
3 对上一步的结果使用explode函数，炸成一个列 
"hello"
"you"
"spark"
"Flink"
4 然后进行分组聚合，统计出每个单词的个数

```

完整代码实现

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import time
import pyspark.sql.functions as F

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    # 1 构造spark运行环境
    spark: SparkSession = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('pyspark') \
        .getOrCreate()

    sc: SparkContext = spark.sparkContext

    # 2 读取数据，生成dataframe
    df_file = spark.read.text('hdfs://node1:8020/word.txt')
    df_file.show()

    # 使用DSL风格实现需求
    df_words = df_file.select(
        F.explode(
            F.split(df_file['value'], ' ')
        ).alias('words'))

    df_words.groupby('words').agg(F.count('words').alias('cnt')).show()
    # df_words.groupby('words').count().show()

    # 使用SQL实现
    df_file.createTempView('words')
    t1 = spark.sql('select explode(split(value, " ")) as word from words')
    t1.createTempView('t1')

    spark.sql('select word, count(1) as cnt from t1 group by word').show()



```

- 1 作业，使用RDD实现这个需求，把结果转成dataframe

```python
    rdd1 = df.rdd.flatMap(lambda line: line['value'].split()).map(lambda word: (word,))        # 每行为表中value字段一个数据
    # [hello, world, spark] -> [(hello,),(world,),(spark,)]
    df2 = spark_ses.createDataFrame(rdd1, schema='word string')
    df2.groupby('word').count().show()
    df2.printSchema()
    df2.show()
    
    rdd2 = df.rdd.flatMap(lambda line: line['value'].split()).map(lambda word: (word,1)).reduceByKey(lambda x, y:x+y)       # 每行为表中value字段一个数据
    # [(hello, 2), (world,3)]
    schema = "word string, cnt int"
    spark_ses.createDataFrame(rdd2, schema=schema).show()

```



#### 3.4.2 电影分析案例

![image-20230524182514342](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/006/2%20%E7%AC%94%E8%AE%B0/assets/image-20230524182514342.png)



```python
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, SparkConf
import os
import time

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    # 1 构造spark运行环境
    spark: SparkSession = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('pyspark') \
        .getOrCreate()

    sc: SparkContext = spark.sparkContext

    # 2 读取数据
    df = spark.read.csv(path='file:///tmp/pycharm_project_956/006/data/u.data',
                   sep='\t', header=False, schema='uid string, mid string, score int, time string')
    # df.show()
    # df.printSchema()
    #
    # # 3.1 需求1
    # df.groupby('uid').agg(F.avg('score').alias('avg_score')).show()
    #
    # # 3.2 需求2
    # df.groupby('mid').agg(F.avg('score').alias('avg_score')).show()
    #
    # # 3.3 需求3
    # df1 = df.select(F.avg('score').alias('avg_score'))
    # df2 = df.join(df1, how='left')
    # df2.show()
    # df2.where('score > avg_score').show()

    # # 3.4 需求4
    # df1:DataFrame = df.where('score > 3').groupby('uid').count()
    # df1.show()
    # df2 = df1.orderBy(F.desc('count')).select('uid').limit(1)
    # df2.show()
    # df.where(df['uid'] == df2.first()['uid']).groupBy('uid').avg().show()
    #
    # # 3.5 需求5
    # df.groupby('uid').agg(
    #     F.avg('score').alias('avg_score'),
    #     F.min('score').alias('min_score'),
    #     F.max('score').alias('max_score')
    # ).show()

    # 3.6 需求6
    # df.show()
    df1 = df.groupby('mid').count().where('count > 100').select(df.mid.alias('mid1')) # 超过一百次的电影
    # df1.show()
    df2 = df1.join(df, how='left', on=(df1.mid1==df.mid))
    # df2.show()
    df3 = df2.groupby('mid1').agg(F.avg('score').alias('avg_score'))
    df3.orderBy(F.desc('avg_score')).limit(10).show()

```



# 007_pyspark笔记

今日内容:

- 0- Spark SQL中数据清洗的相关API   (知道即可)
- 1- Spark SQL中shuffle分区设置 (会设置)
- 2- Spark SQL数据写出操作 (掌握)
- 3- Pandas的相关的内容 (整体了解)
- 4- Spark SQL的函数定义(掌握)

## 0. Spark SQL的相关的清洗API

- 1- 去重API:  df.dropDuplicates()
  - 说明: 当不加参数的时候, 默认对数据整体进行去重,  同样支持针对指定列进行去重操作

![image-20220601093936710](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/day07_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220601093936710.png)

- 2-  删除null值数据:  df.dropna()
  - 说明: 默认支持对所有列进行判断, 如果有一列的对应值为null, 就会将为null这一行数据全部都删除, 也支持针对某些列处理

![image-20220601094108963](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/day07_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220601094108963.png)

- 3- 替换null值:  df.fillna()
  - 说明: 将表中为null的数据替换为指定的值. 同样也可以针对某些列来处理

![image-20220601094235734](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/day07_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220601094235734.png)



代码演示

```properties
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, SparkConf
import os
import time

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    # 1 构造spark运行环境
    spark: SparkSession = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('pyspark') \
        .getOrCreate()

    sc: SparkContext = spark.sparkContext

    # 2 读取文件
    df = spark.read.csv('file:///tmp/pycharm_project_956/day07/data/stu.csv',
                   sep=',', schema='id string, name string, score int', header=False)

    df.show()
    # # dropDuplicates
    # df.dropDuplicates().show() # 判断所有列，如果有一模一样的行，则去掉重复
    # df.dropDuplicates(subset=['name', 'score']).show() # 判断指定name score列，如果这两个列中有一模一样的行，则去掉重复
    #
    # dropna 和drop函数互为别名
    # df.dropna().show()
    # df.dropna('all').show()
    #
    # df.dropna(thresh=2).show() # thresh=2 至少一行有两个非空值才会被保留，如果非空值的个数小于2个，则整行被删除
    #
    # df.dropna(subset=['score']).show()

    # fillna填充缺失值
    df.fillna({'id':10000, 'name':'水水水', 'score':60}).show()
```



## 1. Spark SQL的shuffle分区设置

​		spark SQL在执行的过程中, 会将SQL翻译为Spark的RDD程序来运行, 对于Spark SQL来说, 执行的时候, 同样也会触发shuffle操作, 默认情况下, Spark SQL的shuffle的分区数量默认为 200个

​		在实际生产中使用的时候, 默认为200个 有时候并不合适, 当任务比较大的时候, 一般可能需要调整分区数量更多, 当任务较小的时候, 可能需要调小分区数量

```properties
如何来调整spark SQL的分区数量呢?  参数: spark.sql.shuffle.partitions

方案一: 在配置文件中调整:  spark-defaults.conf   (全局的方式)
	添加以下配置: 
		 spark.sql.shuffle.partitions   100

方案二:  在通过spark-submit 提交任务的时候, 通过 --conf "spark.sql.shuffle.partitions=100"  主要在生产中

方案三:  在代码中, 当构建SparkSession对象的时候, 来设置shuffle的分区数量:    主要是在测试中
		sparkSession.builder.config("spark.sql.shuffle.partitions",'100')
		

在测试环境中, 一般都是右键运行, 此时需要设置分区数量, 可以通过方案三来处理, 但是在后续上线部署的时候, 需要通过spark-submit提供, 为了能够让参数动态传递, 会将代码中参数迁移到 spark-submit命令上设置


示例代码设置spark.sql.shuffle.partitions 为4 运行时间20.4秒，没有设置这参数（使用的默认的配置100分区，）运行时间25.9秒

```





## 2. Spark SQL的数据写出操作

统一写出的API:

![image-20220601101805911](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/day07_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220601101805911.png)

```
上述的完整的API 同样也有简单写法:
	df.write.mode().输出类型()
	
	比如说:
		df.write.mode().csv()
		df.write.mode().json()
		....
```





演示: 输出到文件

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示数据写出到文件中")

    # 1- 创建SparkSession对象:
    spark = SparkSession.builder.master('local[*]').appName('movie').config("spark.sql.shuffle.partitions", "1").getOrCreate()

    # 2- 读取HDFS上movie数据集
    df_init = spark.read.csv(
        path='file:///export/data/workspace/sz30_pyspark_parent/_03_pyspark_sql/data/stu.csv',
        sep=',',
        header=True,
        inferSchema=True
    )
    # 3- 对数据进行清洗操作:
    # 演示一:  去重API:  df.dropDuplicates()
    df = df_init.dropDuplicates()
    # 演示二: 2-  删除null值数据:  df.dropna()
    df = df.dropna()

    # 4- 将清洗后的数据写出到文件中
    # 演示写出为CSV文件
    #df.write.mode('overwrite').format('csv').option('header',True).option('sep','|').save('hdfs://node1:8020/sparkwrite/output1')

    # 演示写出为 JSON
    #df.write.mode('overwrite').format('json').save('hdfs://node1:8020/sparkwrite/output2')

    # 演示输出为text
    #df.select('name').write.mode('overwrite').format('text').save('hdfs://node1:8020/sparkwrite/output3')

    # 演示输出为orc
    df.write.mode('overwrite').format('orc').save('hdfs://node1:8020/sparkwrite/output4')

```



将数据写出到HIVE中

```properties
df.write.mode('append|overwrite|ignore|error').saveAsTable('表名','存储类型')

# 说明: 目前无法演示输出到HIVE , 因为 Spark 和 HIVE没有整合
```



将数据输出到MYSQL: 

```properties
df.write.mode('append|overwrite|ignore|error').format('jdbc')
.option("url","jdbc:mysql://xxx:3306/库名?useSSL=false&useUnicode=true&characterEncoding=utf-8")\
.option("dbtable","表名")\
.option("user","用户名")\
.option("password","密码")\
.save()


说明:
	当表不存在的时候, 会自动建表, 对于overwrite来说, 每次都是将表删除, 重建
	
	如果想要自定义字段的类型, 请先创建表, 然后使用append的方式来添加数据即可
```



- 1- 在mysql中创建一个库

```sql
CREATE DATABASE  day07_pyspark CHARSET utf8;
```

- 2- 编写代码:

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示数据写出到文件中")

    # 1- 创建SparkSession对象:
    spark = SparkSession.builder.master('local[*]').appName('movie').config("spark.sql.shuffle.partitions", "1").getOrCreate()

    # 2- 读取HDFS上movie数据集
    df_init = spark.read.csv(
        path='file:///export/data/workspace/sz30_pyspark_parent/_03_pyspark_sql/data/stu.csv',
        sep=',',
        header=True,
        inferSchema=True
    )
    # 3- 对数据进行清洗操作:
    # 演示一:  去重API:  df.dropDuplicates()
    df = df_init.dropDuplicates()
    # 演示二: 2-  删除null值数据:  df.dropna()
    df = df.dropna()

    # 4- 将清洗后的数据写出到文件中
    
    # 演示输出MySQL
    df.write.mode('overwrite').format('jdbc')\
        .option("url", "jdbc:mysql://node1:3306/day07_pyspark?useSSL=false&useUnicode=true&characterEncoding=utf-8") \
        .option("dbtable", "stu") \
        .option("user", "root") \
        .option("password", "123456") \
        .save()
```



可能报出的错误:

![image-20220601104251963](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/day07_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220601104251963.png)

```properties
原因:
	当前Spark无法找到一个适合的驱动连接MySQL

解决方案: 添加MySQL的驱动包, 需要在以下几个位置中添加驱动包
	1- 在python的环境中添加mysql的驱动包 (在pycharm本地右键运行的时候, 需要加载)
		Base的pyspark库的相关的jar包路径:  /root/anaconda3/lib/python3.8/site-packages/pyspark/jars/
		虚拟环境: /root/anaconda3/envs/虚拟环境名称/lib/python3.8/site-packages/pyspark/jars/

	2- 需要在 Spark的家目录下jars目录下添加mysql的驱动包 (spark-submit提交到spark集群或者local模式需要使用)
		/export/server/spark/jars/
	
	3- 需要在HDFS的/spark/jars目录下添加mysql的驱动包 (spark-submit提交到yarn环境的时候)


建议: 如果是常用的jar包, 建议以上三个位置都添加
```







## 3. Spark SQL函数定义

### 3.1 如何使用窗口函数

回顾窗口函数:

```properties
窗口函数格式: 
	分析函数 over(partition by xxx order by xxx [asc | desc] [rows between  窗口范围 and 窗口范围 ])
	
分析函数:
	第一类函数:  row_number() rank() dense_rank() ntile(N)
	# 100 100 98 98 95
	# 1 2 3 4 5
	# 1 1 3 3 5
	# 1 1 2 2 3
	第二类函数: 与聚合函数组合使用  sum() max() min() avg() count()

	第三类函数:  lead() lag() frist_value() last_value()
```

这些窗口函数如何在spark SQL中使用呢?

- 通过SQL的方式 : 与在hive中使用基本是雷同

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Window as win
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示spark SQL的窗口函数使用")

    # 1- 创建SparkSession对象:
    spark = SparkSession.builder.master('local[*]').appName('windows').getOrCreate()

    # 2- 读取外部数据源
    df_init = spark.read.csv(path='file:///export/data/workspace/sz30_pyspark_parent/_03_pyspark_sql/data/pv.csv',header=True,inferSchema=True)

    # 3- 处理数据操作:  演示窗口函数
    df_init.createTempView('pv_t1')
    # 3.1  SQL方式
    spark.sql("""
        select
            uid,
            datestr,
            pv,
            row_number() over(partition by uid order by pv desc) as rank1,
            rank() over(partition by uid order by pv desc) as rank2,
            dense_rank() over(partition by uid order by pv desc) as rank3
        from pv_t1
    """).show()
```

- DSL方式

```sql
# 3.2 DSL方式:
    df_init.select(
        df_init['uid'],
        df_init['datestr'],
        df_init['pv'],
        F.row_number().over(win.partitionBy('uid').orderBy(F.desc('pv'))).alias('rank1'),
        F.rank().over(win.partitionBy('uid').orderBy(F.desc('pv'))).alias('rank2'),
        F.dense_rank().over(win.partitionBy('uid').orderBy(F.desc('pv'))).alias('rank3')
    ).show()

```



### 3.2 SQL函数的分类说明

回顾 SQL 函数的分类:

- UDF:   用户自定义函数
  - 特点: 一进一出  大部分的内置函数都是属于UDF函数  
  - 比如  substr()  
- UDAF:  用户自定义聚合函数
  - 特点: 多进一出 
  - 比如: sum()  count() avg()....
- UDTF: 用户自定义表生成函数
  - 特点:  一进多出  (给一个数据, 返回多行或者多列的数据)
  - 比如: explode()  爆炸函数



其实不管是spark SQL中  内置函数, 还是hive中内置函数, 其实也都是属于这三类中其中一类



自定义函数目的: 

- 扩充函数, 因为在进行业务分析的时候, 有时候会使用某些功能, 但是内置函数并没有提供这样的功能, 此时需要进行自定义函数来解决

目前支持自定义函数:

```
	对于spark SQL  目前支持定义 UDF 和 UDAF , 但是对于python语言 仅支持定义UDF函数, 如果想要定义UDAF函数, 需要使用pandas UDF实现

```

![image-20211115093809302](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/day07_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211115093809302.png)



注意点:

```properties
	在使用python 原生 spark SQL的 UDF方案, 整个执行效率不是特别高, 因为整个内部运算是一个来处理., 一个个返回, 这样会导致频繁进行 序列化和反序列化的操作 从而影响效率
	后续改进版本: 采用java 或者scala来定义, 然后python调用即可
	
	目前主要使用版本: 是采用pandas的UDF函数解决, 同时采用apache arrow 内存数据结构框架 来实现, 提升整个执行效率

```



### 3.3 Spark原生自定义UDF函数

如何自定义UDF函数:

```properties
1- 第一步: 根据也要功能要求,  定义一个普通的Python的函数: 

2- 第二步: 将这个python的函数注册到Spark SQL中:  
	注册方式有以下二种方案: 
		方式一:  可适用于  SQL 和 DSL
			udf对象 = sparkSession.udf.register(参数1, 参数2,参数3)
			
			参数1: udf函数的函数名称, 此名称用于在SQL风格中使用
			参数2: 需要将那个python函数注册为udf函数
			参数3: 设置python函数返回的类型
			
			udf对象 主要使用在DSL中
		
		方式二: 仅适用于 DSL方案
			udf对象 =F.udf(参数1,参数2)
			
			参数1: 需要将那个python函数注册为udf函数
			参数2: 设置python函数返回的类型
			udf对象 主要使用在DSL中
			
		方式二还有一种语法糖写法:  @F.udf(设置返回值类型)  底层走的是装饰器
			放置在普通的python函数的上面

3- 在SQL或者 DSL中使用即可

```

自定义UDF函数演示操作:

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示如何自定义UDF函数")

    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()

    # 2- 读取外部的数据集
    df_init = spark.read.csv(
        path='file:///export/data/workspace/sz30_pyspark_parent/_03_pyspark_sql/data/student.csv',
        schema='id int,name string,age int'
    )
    df_init.createTempView('t1')
    # 3- 处理数据
    # 需求: 自定义函数需求  请在name名称后面 添加一个  _itcast
    # 3.1 自定义一个python函数, 完成主题功能
    # 方式三: 不能和方式二共用
    # @F.udf(returnType=StringType())
    def concat_udf(name: str) -> str:
        return name + '_itcast'


    # 3.2 将函数注册给spark SQL
    #方式一:
    concat_udf_dsl = spark.udf.register('concat_udf_sql',concat_udf,StringType())

    # 方式二:
    concat_udf_dsl_2 = F.udf(concat_udf,StringType())

    # 3.3 使用自定义函数
    # SQL使用
    spark.sql("""
        select id,concat_udf_sql(name) as name ,age from  t1
    """).show()

    # DSL中使用
    df_init.select(
        df_init['id'],
        concat_udf_dsl(df_init['name']).alias('name'),
        df_init['age']
    ).show()

    df_init.select(
        df_init['id'],
        concat_udf_dsl_2(df_init['name']).alias('name'),
        df_init['age']
    ).show()

    df_init.select(
        df_init['id'],
        concat_udf(df_init['name']).alias('name'),
        df_init['age']
    ).show()



```

![image-20230525164503880](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/assets/image-20230525164503880.png)

演示返回类型为 字典/列表

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import  *
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示spark sql的UDF函数: 返回列表/字典")
    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()

    # 2- 读取外部的数据集
    df_init = spark.read.csv(
        path='file:///export/data/workspace/sz30_pyspark_parent/_03_pyspark_sql/data/user.csv',
        header=True,
        inferSchema=True
    )
    df_init.createTempView('t1')

    # 3- 处理数据
    # 需求: 自定义函数 请将line字段切割开, 将其转换 姓名 地址 年龄
    # 3.1 定义一个普通python的函数
    def split_3col_1(line):
        return line.split('|')

    def split_3col_2(line):
        arr = line.split('|')
        return {'name':arr[0],'address':arr[1],'age':arr[2]}


    # 3.2 注册
    # 对于返回列表的注册方式
    # 方式一
    schema = StructType().add('name',StringType()).add('address',StringType()).add('age',StringType())
    split_3col_1_dsl = spark.udf.register('split_3col_1_sql',split_3col_1,schema)

    # 对于返回为字典的方式
    # 方式二:
    schema = StructType().add('name', StringType()).add('address', StringType()).add('age', StringType())
    split_3col_2_dsl = F.udf(split_3col_2,schema)

    # 3.3 使用自定义函数
    spark.sql("""
        select
            userid,
            split_3col_1_sql(line) as 3col,
            split_3col_1_sql(line)['name'] as name,
            split_3col_1_sql(line)['address'] as address,
            split_3col_1_sql(line)['age'] as age
        from t1
    """).show()

    # DSL中
    df_init.select(
        'userid',
        split_3col_2_dsl('line').alias('3col'),
        split_3col_2_dsl('line')['name'].alias('name'),
        split_3col_2_dsl('line')['address'].alias('address'),
        split_3col_2_dsl('line')['age'].alias('age')
    ).show()

```



### 3.4 基于pandas的Spark UDF函数

#### 3.4.1 apache arrow基本介绍

​		apache arrow 是apache旗下的一款顶级的项目, 是一个跨平台的在内存中以列式存储的数据层, 它设计的目的是作为一个跨平台的数据层, 来加快大数据分析项目的运行效率

​		pandas与pyspark SQL 进行交互的时候, 建立在apache arrow上, 带来低开销 高性能的UDF函数

​		arrow 并不会自动使用, 需要对配置以及代码做一定小的更改才可以使用并兼容



如何安装?

```properties
	pip install pyspark[sql]
	
	说明: 三个节点要求要安装, 如果使用除base虚拟环境以外的环境, 需要先切换到对应虚拟环境下
	
	注意: 
		如果安装比较慢, 可以添加一下 清华镜像源
			pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pyspark[sql]
			
		不管是否使用我的虚拟机, 都建议跑一次, 看一下是否存在

```

如何使用呢?

```properties
	 spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

```

![image-20211115114425971](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/day07_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211115114425971.png)



#### 3.4.2 如何基于arrow完成pandas DF 与 Spark DF的互转操作

如何将pandas的DF对象 转换 spark的DF , 以及如何从spark df 转换为 pandas的 df对象

```properties
pandas DF  --> Spark DF : 
	spark_df = sparkSession.createDataFrame(pd_df)
	
Spark DF  ---> pandas DF:
	pd_df = spark_df.toPandas()

```

代码演示:

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pandas as pd
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("pySpark模板")

    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()

    # 开启arrow
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # 2- 创建 pandas的DF对象:
    pd_df = pd.DataFrame({'name':['张三','李四','王五'],'age':[20,18,15]})
	# name age
	# zs 20    True
	# ls 18    True
	# ww 15    False
    # 3- 可以使用panda的API来对数据进行处理操作
    pd_df = pd_df[pd_df['age'] > 16]
    print(pd_df)

    # 4- 将其转换为Spark DF
    spark_df = spark.createDataFrame(pd_df)


    # 5- 可以使用spark的相关API处理数据
    spark_df = spark_df.select(F.sum('age').alias('sum_age'))

    spark_df.printSchema()
    spark_df.show()

    # 6- 还可以将spark df 转换为pandas df
    pd_df = spark_df.toPandas()

    print(pd_df)

```



请注意: 开启arrow方案, 必须先安装arrow, 否则无法使用, 一执行就会报出: no module 'pyarrow'  (没有此模块)

```properties
pandas UDF代码, 请各位先检查 当前虚拟机中pyspark库是否为3.1.2

命令:
	conda list | grep pyspark

```



#### 3.4.3 pandas UDF

- 方式一: series TO  series
  - 描述: 定义一个python的函数, 接收series类型, 返回series类型, 接收一列返回一列 
  - 目的: 用于定义 pandas的UDF函数

![image-20220601172138718](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/day07_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220601172138718.png)

代码演示:

```properties
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("基于pandas定义UDF函数")
    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()

    # 2- 构建数据集
    df_init = spark.createDataFrame([(1,3),(2,5),(3,8),(5,4),(6,7)],schema='a int,b int')
    df_init.createTempView('t1')
    # 3- 处理数据:
    # 需求: 基于pandas的UDF 完成 对 a 和 b列乘积计算
    # 3.1 自定义一个python的函数: 传入series类型, 返回series类型
    @F.pandas_udf(returnType=IntegerType())  # 装饰器  用于将pandas的函数转换为 spark SQL的函数
    def pd_cj(a:pd.Series,b:pd.Series) -> pd.Series:
        return a * b

    #3.2 对函数进行注册操作
    # 方式一:
    pd_cj_dsl = spark.udf.register('pd_cj_sql',pd_cj)

    #3.3 使用自定义函数
    # SQL
    spark.sql("""
        select a,b, pd_cj_sql(a,b) as cj from  t1
    """).show()
    
    # DSL
    df_init.select('a','b',pd_cj('a','b').alias('cj')).show()


```

- 从series类型 到 标量(python基本数据类型) :
  - [1,2,3] 6
  - 描述: 定义一个python函数, 接收series类型的数据, 输出为标量, 用于定义 UDAF函数

```python
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("基于pandas定义UDF函数")
    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('udf_01').getOrCreate()

    # 2- 构建数据集
    df_init = spark.createDataFrame([(1,3),(1,5),(1,8),(2,4),(2,7)],schema='a int,b int')
    df_init.createTempView('t1')
    # 3- 处理数据:
    # 需求: pandas的UDAF需求   对 B列 求和
    # 3.1 创建python的函数:  接收series类型, 输出基本数据类型(标量)
    @F.pandas_udf(returnType=IntegerType())
    def pd_b_sum(b:pd.Series) -> int:
        return b.sum()


    # 3.2 注册函数
    spark.udf.register('pd_b_sum_sql',pd_b_sum)

    # 3.3 使用自定义函数
    spark.sql("""
        select
            pd_b_sum_sql(b) as avg_b
        from t1
    """).show()

    spark.sql("""
            select
                a,
                b,
                pd_b_sum_sql(b) over(partition by a order by b) as sum_b
            from t1
        """).show()

```

#### 3.4.4 pandas UDF函数案例 (作业)

![image-20220601174635026](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/day07_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220601174635026.png)

需求: 必须自定义panda的UDF函数使用 (无难度 (连一点点都没有))

![image-20220601174644844]()





# 008_pandas相关内容

## 1. pandas的基本介绍

​		Python在数据处理上独步天下：代码灵活、开发快速；尤其是Python的Pandas包，无论是在数据分析领域、还是大数据开发场景中都具有显著的优势：

- Pandas是Python的一个第三方包，也是商业和工程领域最流行的**结构化数据**工具集，用于数据清洗、处理以及分析
- Pandas和Spark中很多功能都类似，甚至使用方法都是相同的；当我们学会Pandas之后，再学习Spark就更加简单快速
- Pandas在整个数据开发的流程中的应用场景
  - 在大数据场景下，数据在流转的过程中，Python Pandas丰富的API能够更加灵活、快速的对数据进行清洗和处理
- Pandas在数据处理上具有独特的优势：
  - 底层是基于Numpy构建的，所以运行速度特别的快
  - 有专门的处理缺失数据的API
  - 强大而灵活的分组、聚合、转换功能

![image-20220123020412280](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123020412280.png)

适用场景:

- 数据量大到excel严重卡顿，且又都是单机数据的时候，我们使用pandas
  - pandas用于处理单机数据(小数据集(相对于大数据来说))
- 在大数据ETL数据仓库中，对数据进行清洗及处理的环节使用pandas

## 2. 安装pandas的库

​	打开cmd界面, 执行 pip install -i https://pypi.tuna.tsinghua.edu.cn/simple/ pandas

![image-20220123021121835](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123021121835.png)



说明:

```properties
	在安装python环境的时候, 除了基于之前直接安装python解析器方案, 其实安装python还有一些其他的操作, 
	比如说, 我们可以通过anaconda 方式来进行安装, 
	
	anaconda: 数据科学库 
		包含了python环境, 以及包含了非常多余数据科学相关的库, 全部的集成在了一起, 如果是基于anaconda安装的时候, 很多的数据科学库就不需要自己安装了, anaconda都自带,  其中pandas其实就是anaconda库中一员
		anaconda提供了虚拟环境方案,  可以在一个操作系统中, 安装不同版本的python环境, 各个环境之间还相对独立
```





## 3. pandas的初体验

- 1-将资料中提供的数据集导入到data目录中

![image-20220123021824587](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123021824587.png)

- 2- 创建python脚本, 导入pandas库

```python
import pandas as pd
```

- 3- 基于pandas加载数据

```python
df = pd.read_csv('../数据集/1960-2019全球GDP数据.csv', encoding='gbk', )  
```

- 4- 基于pandas完成相关查询:

```python
# 查询中国的GDP
china_gdp = df[df.country=='中国'] # df.country 选中名为country的列
china_gdp.head(10) # 显示前10条数据
```

![image-20220123022204903](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123022204903.png)



## 4. pandas的数据结构

![image-20220123022429098](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123022429098.png)

上图为上一节中读取并展示出来的数据，以此为例我们来讲解Pandas的核心概念，以及这些概念的层级关系：

- DataFrame
  - Series
    - 索引列
      - 索引名、索引值
      - 索引下标、行号
    - 数据列
      - 列名
      - 列值，具体的数据

其中最核心的就是Pandas中的两个数据结构：DataFrame和Series



### 4.1 series对象

​		Series也是Pandas中的最基本的数据结构对象，下文中简称s对象；是DataFrame的列对象，series本身也具有索引。

Series是一种类似于一维数组的对象，由下面两个部分组成：

- values：一组数据（numpy.ndarray类型）
- index：相关的数据索引标签；如果没有为数据指定索引，于是会自动创建一个0到N-1(N为数据的长度)的整数型索引。

#### 4.1.1 创建Series对象

- 1- 导入pandas 

```python
import pandas as pd
```

- 2- 通过list列表来创建

```python
# 使用默认自增索引
s2 = pd.Series([1, 2, 3])
print(s2)
# 自定义索引
s3 = pd.Series([1, 2, 3], index=['A', 'B', 'C'])
s3


结果为:
0    1
1    2
2    3
dtype: int64
A    1
B    2
C    3
dtype: int64
```

- 3- 使用字典或元组创建series对象

```python
#使用元组
tst = (1,2,3,4,5,6)
pd.Series(tst)

#使用字典：
dst = {'A':1,'B':2,'C':3,'D':4,'E':5,'F':6}
pd.Series(dst)
```

#### 4.1.2 Series对象常用API

构造一个series对象

```python
s4 = pd.Series([i for i in range(6)], index=[i for i in 'ABCDEF'])
s4
# 返回结果如下
A    0
B    1
C    2
D    3
E    4
F    5
dtype: int64
```

- 1- series对象常用属性和方法

```python
# s对象有多少个值，int
len(s4) 
s4.size

# s对象有多少个值，单一元素构成的元组 (6,)
s4.shape 

# 查看s对象中数据的类型
s4.dtypes

# s对象转换为list列表
s4.to_list()

# s对象的值 array([0, 1, 2, 3, 4, 5], dtype=int64)
s4.values 

# s对象的值转换为列表
s4.values.tolist() 

# s对象可以遍历，返回每一个值
for i in s4: 
    print(i)

# 下标获取具体值
s4[1] 

# 返回前2个值，默认返回前5个
s4.head(2) 

# 返回最后1个值，默认返回后5个
s4.tail(1) 

# 获取s对象的索引 Index(['A', 'B', 'C', 'D', 'E', 'F'], dtype='object')
s4.index 

# s对象的索引转换为列表
s4.index.to_list() 

# s对象中数据的基础统计信息
s4.describe()
# 返回结果及说明如下
#count    6.000000 # s对象一共有多少个值
#mean     2.500000 # s对象所有值的算术平均值
#std      1.870829 # s对象所有值的标准偏差
#min      0.000000 # s对象所有值的最小值
#25%      1.250000 # 四分位 1/4位点值
#50%      2.500000 # 四分位 1/2位点值
#75%      3.750000 # 四分位 3/4位点值
#max      5.000000 # s对象所有值的最大值
#dtype: float64
# 标准偏差是一种度量数据分布的分散程度之标准，用以衡量数据值偏离算术平均值的程度。标准偏差越小，这些值偏离平均值就越少，反之亦然。
# 四分位数（Quartile）也称四分位点，是指在统计学中把所有数值由小到大排列并分成四等份，处于三个分割点位置的数值。

# seriest对象转换为df对象
s4.to_frame()
s4.reset_index()

```

#### 4.1.3  Series 对象的运算

​		Series和数值型变量计算时，变量会与Series中的每个元素逐一进行计算	

​		两个Series之间计算，索引值相同的元素之间会进行计算；索引不同的元素最终计算的结果会填充成缺失值，用NaN表示

- Series和数值型变量计算

```python
s4 * 5
# 返回结果如下
A     0
B     5
C    10
D    15
E    20
F    25
dtype: int64

```

- 索引完全相同的两个Series对象进行计算

```python
s4
# 构造与s4索引相同的s对象
s5 = pd.Series([10]*6, index=[i for i in 'ABCDEF'])
s5
# 两个索引相同的s对象进行运算
s4 + s5 

# 返回结果如下
A    0
B    1
C    2
D    3
E    4
F    5
dtype: int64
A    10
B    10
C    10
D    10
E    10
F    10
dtype: int64
A    10
B    11
C    12
D    13
E    14
F    15
dtype: int64

```

- 索引不同的两个s对象运算

```python
s4
# 注意s6的最后一个索引值和s4的最后一个索引值不同
s6 = pd.Series([10]*6, index=[i for i in 'ABCDEG'])
s6
s4 + s6


# 返回结果如下
A    0
B    1
C    2
D    3
E    4
F    5
dtype: int64
A    10
B    10
C    10
D    10
E    10
G    10
dtype: int64
A    10.0
B    11.0
C    12.0
D    13.0
E    14.0
F     NaN
G     NaN
dtype: float64

```

### 4.2  DataFrame

#### 4.2.1 创建DF对象

DataFrame的创建有很多种方式

- Serires对象转换为df：上一小节中学习了`s.to_frame()`以及`s.reset_index()`
- 读取文件数据返回df：在之前的学习中我们使用了`pd.read_csv('csv格式数据文件路径')`的方式获取了df对象
- 使用字典、列表、元组创建df：接下来就展示如何使用字段、列表、元组创建df

------

- 使用字典加列表创建df，使默认自增索引

```python
df1_data = {
    '日期': ['2021-08-21', '2021-08-22', '2021-08-23'],
    '温度': [25, 26, 50],
    '湿度': [81, 50, 56] 
}
df1 = pd.DataFrame(data=df1_data)
df1

# 返回结果如下
        日期    温度    湿度
0    2021-08-21    25    81
1    2021-08-22    26    50
2    2021-08-23    50    56

```

- 使用列表加元组创建df，并自定义索引

```python
df2_data = [
    ('2021-08-21', 25, 81),
    ('2021-08-22', 26, 50),
    ('2021-08-23', 27, 56)
]

df2 = pd.DataFrame(
    data=df2_data, 
    columns=['日期', '温度', '湿度'],
    index = ['row_1','row_2','row_3'] # 手动指定索引
)
df2

# 返回结果如下
            日期    温度    湿度
row_1    2021-08-21    25    81
row_2    2021-08-22    26    50
row_3    2021-08-23    27    56

```

#### 4.2.2 DataFrame对象常用API

- DataFrame对象常用API与Series对象几乎相同

```python
# 返回df的行数
len(df2)

# df中数据的个数
df2.size

# df中的行数和列数，元组 (行数, 列数)
df2.shape

# 返回列名和该列数据的类型
df2.dtypes

# 返回nparray类型的2维数组，每一行数据作为一维数组，所有行数据的数组再构成一个二维数组
df2.values

# 返回df的所有列名
df2.columns

# df遍历返回的只是列名 
for col_name in df2: 
    print(col_name)

# 返回df的索引对象
df2.index

# 返回第一行数据，默认前5行
df2.head(5)

# 返回倒数第1行数据，默认倒数5行
df2.tail(5)

# 返回df的基本信息：索引情况，以及各列的名称、数据数量、数据类型
df2.info() # series对象没有info()方法

# 返回df对象中所有数字类型数据的基础统计信息
# 返回对象的内容和Series.describe()相同
df2.describe()

# 返回df对象中全部列数据的基础统计信息
df2.describe(include='all')

```

#### 4.2.3 DataFrame对象的运算

​		当DataFrame和数值进行运算时，DataFrame中的每一个元素会分别和数值进行运算，但df中的数据存在非数值类型时不能做加减法运算

​		两个DataFrame之间、以及df和s对象进行计算，和2个series计算一样，会根据索引的值进行对应计算：当两个对象的索引值不能对应时，不匹配的会返回NaN

- df和数值进行运算

```python
f2 * 2 # 不报错
df2 + 1 # 报错，因为df2中有str类型（Object）的数据列

```

- df和df进行运算

```python
# 索引完全不匹配
df1 + df2 

# 构造部分索引和df2相同的新df
df3 = df2[df2.index!='row_3']
df3 

# 部分索引相同
df2 + df3 

# 返回结果如下
   日期 温度 湿度
0    NaN    NaN    NaN
1    NaN    NaN    NaN
2    NaN    NaN    NaN
row_1    NaN    NaN    NaN
row_2    NaN    NaN    NaN
row_3    NaN    NaN    NaN

            日期    温度    湿度
row_1    2021-08-21    25    81
row_2    2021-08-22    26    50

                        日期    温度    湿度
row_1    2021-08-212021-08-21    50.0    162.0
row_2    2021-08-222021-08-22    52.0    100.0
row_3    NaN    NaN    NaN


```

### 4.3 pandas的数据类型

- df或s对象中具体每一个值的数据类型有很多，如下表所示

| Pandas数据类型 | 说明         | 对应的Python类型            |
| -------------- | ------------ | --------------------------- |
| Object         | 字符串类型   | string                      |
| int            | 整数类型     | int                         |
| float          | 浮点数类型   | float                       |
| datetime       | 日期时间类型 | datetime包中的datetime类型  |
| timedelta      | 时间差类型   | datetime包中的timedelta类型 |
| category       | 分类类型     | 无原生类型，可以自定义      |
| bool           | 布尔类型     | True,False                  |
| nan            | 空值类型     | None                        |

- 可以通过下列API查看s对象或df对象中数据的类型

```python
s1.dtypes
df1.dtypes
df1.info() # s对象没有info()方法

```

## 5. pandas多格式数据读写

常用读写文件函数清单

| 文件格式 | 读取函数          | 写入函数        |
| -------- | ----------------- | --------------- |
| xlsx     | pd.read_excel     | df.to_excel     |
| xls      | pd.read_excel     | df.to_excel     |
| csv      | pd.read_csv       | df.to_csv       |
| tsv      | pd.read_csv       | df.to_csv       |
| json     | pd.read_json      | to_json         |
| html     | pd.read_html      | df.to_html      |
| sql      | pd.read_sql       | df.to_sql       |
| 剪贴板   | df.read_clipboard | df.to_clipboard |



### 5.1 写文件

数据准备

```python
# 导包 加载数据集
import pandas as pd 
# 构造df数据集
df = pd.DataFrame(
    [
        ['1960-5-7', '刘海柱', '职业法师'],
        ['1978-9-1', '赵金龙', '大力哥'],
        ['1984-12-27', '周立齐', '窃格瓦拉'],
        ['1969-1-24', '于谦', '相声皇后']
    ], 
    columns=['birthday', 'name', 'AKA']
)
df

```

- 以写入csv文件为例

```python
df.to_csv('./写文件.csv') # 此时应该在运行代码的相同路径下就生成了一个名为“写文件.csv”的文件

```

​		注意：执行`df.to_csv()`时，文件需要关闭才能写入，不然会报 `PermissionError: [Errno 13] Permission denied: 'xxxx.csv'`的异常



### 5.2 读文件

以读取csv文件为例

```python
df = pd.read_csv('./写文件.csv')
df

```

![image-20220123163537467](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123163537467.png)

- index_col 参数指定索引

  ```
  index_col参数可以在读文件的时候指定列作为返回dataframe的索引，两种用法如下:
  * 通过列下标指定为索引
  * 通过列名指定为索引
  
  ```

  - 通过列下标指定为索引`index_col=[列下标]`

  ```python
  df = pd.read_csv('./写文件.csv', index_col=[0])
  df
  
  ```

  - 通过列名指定为索引`index_col=['列名']`

  ```python
  df = pd.read_csv('./写文件.csv', index_col=['Unnamed: 0'])
  df
  
  ```

  ![image-20220123164049964](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123164049964.png)

- parse_dates 参数指定列解析为时间日期类型

  ```properties
  parse_dates参数可以在读文件的时候解析时间日期类型的列，两种作用如下：
  
  - 将指定的列解析为时间日期类型
    - 通过列下标解析该列为时间日期类型
    - 通过列名解析该列为时间日期类型
  - 将df的索引解析为时间日期类型
  
  ```

  - 通过列下标解析该列为时间日期类型`parse_dates=[列下标]`

  ```python
  pd.read_csv('./写文件.csv').info()
  pd.read_csv('./写文件.csv', parse_dates=[1]).info()
  
  ```

  ![image-20220123164314613](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123164314613.png)

  - 通过列名解析该列为时间日期类型`parse_dates=[列名]`

  ```python
  pd.read_csv('./写文件.csv').info()
  pd.read_csv('./写文件.csv', parse_dates=['birthday']).info()
  
  ```

  ![image-20220123164423959](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123164423959.png)

  - 将df的索引解析为时间日期类型`parse_dates=True`

  ```python
  df = pd.read_csv('./写文件.csv', index_col=[1], parse_dates=True) 
  df
  df.index
  
  ```

  ![image-20220123164514724](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123164514724.png)

- encoding 参数 指定编码格式

  常见的编码格式有：ASCII、GB2312、UTF8、GBK 等

  ```python
  pd.read_csv('../数据集/1960-2019全球GDP数据.csv', encoding='gbk').head()
  
  ```

  ![image-20220123164735677](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123164735677.png)

- sep参数, 指定字段之间的分隔符号

  默认的分隔符号为逗号, 当文件中的字段之间的分隔符号不是逗号的时候, 我们可以采用此参数来调整

  ```
  pd.read_csv('../数据集/csv示例文件.csv', sep='\t', index_col=[0])
  
  ```

  ![image-20220123165048778](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123165048778.png)

### 5.3 读写数据库

​		以MySQL数据库为例，**此时默认你已经在本地安装好了MySQL数据库**。如果想利用pandas和MySQL数据库进行交互，需要先安装与数据库交互所需要的python包

```python
pip install pymysql==1.0.2
# 如果后边的代码运行提示找不到sqlalchemy的包，和pymysql一样进行安装即可
#pip install sqlalchemy==1.4.31

```

![image-20220125205916505](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220125205916505.png)

- 准备要写入数据库的数据

```python
import pandas as pd 
df = pd.read_csv('../数据集/csv示例文件.csv', sep='\t', index_col=[0]) 
df

```

![image-20220123165416808](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123165416808.png)

- 创建数据库操作引擎对象并指定数据库

```python
# 需要安装pymysql，部分版本需要额外安装sqlalchemy
# 导入sqlalchemy的数据库引擎
from sqlalchemy import create_engine

# 创建数据库引擎，传入uri规则的字符串
engine = create_engine('mysql+pymysql://root:123456@127.0.0.1:3306/test?charset=utf8')
# mysql+pymysql://root:chuanzhi@127.0.0.1:3306/test?charset=utf8
# mysql 表示数据库类型
# pymysql 表示python操作数据库的包
# root:chuanzhi 表示数据库的账号和密码，用冒号连接
# 127.0.0.1:3306/test 表示数据库的ip和端口，以及名叫test的数据库
# charset=utf8 规定编码格式

```

- 将数据写入MySQL数据库

```python
# df.to_sql()方法将df数据快速写入数据库
df.to_sql('test_pdtosql', engine, index=False, if_exists='append')
# 第一个参数为数据表的名称
# 第二个参数engine为数据库交互引擎
# index=False 表示不添加自增主键
# if_exists='append' 表示如果表存在就添加，表不存在就创建表并写入

```

- 此时我们就可以在本地test库的test_pdtosql表中看到写入的数据

  ![image-20220123165745328](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123165745328.png)



- 从数据库中加载数据:

  - 读取整张表, 返回dataFrame

  ```python
  # 指定表名，传入数据库连接引擎对象
  pd.read_sql('test_pdtosql', engine)
  
  ```

  - 使用SQL语句获取数据，返回dataframe

  ```python
  # 传入sql语句，传入数据库连接引擎对象
  pd.read_sql('select name,AKA from test_pdtosql', engine)
  
  ```

  ![image-20220123165932157](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220123165932157.png)



可能出现的问题:

![image-20220125211715019](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day07/2%20%E7%AC%94%E8%AE%B0/pandas%E8%AF%BE%E4%BB%B6.assets/image-20220125211715019.png)

```python
说明: 
	sqlalche 库版本过低导致的

解决方案:
	先删除原有版本: 
		pip uninstall sqlalchemy
	
	重新安装:
		pip install sqlalchemy==1.4.31

```



# day09 pyspark笔记

今日内容:

- 1- Spark ON hive (参数配置完成)
- 2- Spark SQL 分布式执行引擎 (会使用)
- 3- Spark SQL的运行机制 (理解)
- 4- 综合案例 (作业)





## 0 作业答案:

数据说明:

```properties
_c0,对手,胜负,主客场,命中,投篮数,投篮命中率,3分命中率,篮板,助攻,得分
0,勇士,胜,客,10,23,0.435,0.444,6,11,27
1,国王,胜,客,8,21,0.381,0.286,3,9,28
2,小牛,胜,主,10,19,0.526,0.462,3,7,29
3,火箭,负,客,8,19,0.526,0.462,7,9,20
4,快船,胜,主,8,21,0.526,0.462,7,9,28
5,热火,负,客,8,19,0.435,0.444,6,11,18
6,骑士,负,客,8,21,0.435,0.444,6,11,28
7,灰熊,负,主,10,20,0.435,0.444,6,11,27
8,活塞,胜,主,8,19,0.526,0.462,7,9,16
9,76人,胜,主,10,21,0.526,0.462,7,9,28
```

需求说明: 要求每一个都要使用 自定义函数方式

```properties
1- 助攻这列 +10 操作:  自定义 UDF

2- 篮板 + 助攻 的次数:  自定义 UDF
 
3- 统计 胜负的平均分:  自定义 UDAF
```

代码实现:

```properties
第一步: 将资料中 data.csv 数据放置到 spark sql的项目的data目录下


第二步: 编码实现操作:
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions  as F
import os

# 目的: 锁定远端操作环境, 避免存在多个版本环境的问题
os.environ["SPARK_HOME"] = "/export/server/spark"
os.environ["PYSPARK_PYTHON"] = "/root/anaconda3/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/root/anaconda3/bin/python"

if __name__ == '__main__':
    print("pd 函数的案例")

    # 1) 创建 sparkSession对象
    spark = SparkSession.builder.master("local[*]").appName("pd_udf").getOrCreate()

    # 开启 arrow方案:
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)

    # 2) 读取数据:
    df = spark.read.format("csv").option("header",True).option("inferSchema",True).load("file:///data/data.csv")

    # 3) 为每一个需求 定义一个函数, 并通过装饰者方案注册即可:
    # 需求1 - 助攻这列 + 10  UDF
    @F.pandas_udf(returnType=LongType())
    def add_zg(n:pd.Series) -> pd.Series:
        return n + 10
    # 需求2 - 篮板 + 助攻的次数:   udf
    @F.pandas_udf(returnType=LongType())
    def add_lb_zg(lb:pd.Series,zg:pd.Series) -> pd.Series:
        return lb + zg
    # 需求3 - 统计胜负的平均分:   UDAF
    @F.pandas_udf(returnType=DoubleType())
    def mean_sf(score:pd.Series) -> float:
        return score.mean()

    # 4) 使用函数完成对应需求
    df.withColumn('助攻+10',add_zg('助攻')).show()

    df.withColumn('助攻+篮板',add_lb_zg('篮板','助攻')).show()

    df.groupby(df['胜负']).agg(mean_sf('得分')).show()
```





## 1- Spark On Hive

![image-20220602093904338](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220602093904338.png)



### 1.1 集成原理说明

说明:

```properties
HIVE的HiveServer2 本质上作用: 接收SQL语句 将 SQL 翻译为  MR程序 , 需要使用到相关元数据的时候, 连接metastore来获取即可


Spark on HIVE的本质:    将HIVE的MR执行引擎替换为 Spark RDD


思考: 对于HIVE来说, 本质上将SQL翻译为 MR . 这个操作是有hiveServer2来负责的, 所以说Spark On HIve主要目的, 将HiveServer2替换掉, 将其更换为Spark所提供的SparkServer2


认为Spark on HIVE本质:  替换掉HIVE中HiveServer2 让Spark提供一个spark的HiveServer2, 对接metastore 从而完成将SQL翻译为Spark RDD


好处: 
	1- 对于Spark来说, 也不需要自己维护元数据, 可以利用hive的metastore来维护
	2- 集成HIVE后, 整个HIVE的执行效率也提高了

spark集成目的: 抢占HIVE的市场
	所以Spark后续会提供一个分布式执行引擎, 此引擎就是为了模拟hiveServer2, 一旦模拟后, 会让用户感觉跟操作原来HIVE基本上雷同的, 比如说; 端口号, 连接的方式, 配置的方式全部都一致



思考: HIVE不启动hiveserver2是否可以执行呢? 可以的 通过 ./hive  
	启动hiveserver2的目的: 为了可能让更多的外部客户端连接,比如说 datagrip
```





### 1.2 配置操作

大前提:  要保证之前hive的配置没有问题

```properties
建议:
	在on hive配置前, 尝试先单独启动hive 看看能不能启动成功, 并连接

启动hive的命令:
cd /export/server/hive/bin
启动metastore: 
	nohup ./hive --service metastore &
启动hiveserver2:
	nohup ./hive --service hiveserver2 &
	
基于beeline连接: 
	./beeline 进入客户端
	输入: !connect jdbc:hive2://node1:10000
	输入用户名: root
	输入密码: 密码可以不用输入

注意:
	启动hive的时候, 要保证 hadoop肯定是启动良好了
	启动完成后, 如果能正常连接, 那么就可以退出客户端, 然后将HIVE直接通过 kill -9 杀掉了	

```



配置操作:

```properties
1) 确保 hive的conf目录下的hive-site.xml中配置了metastore服务地址
	<property>
        <name>hive.metastore.uris</name>
        <value>thrift://node1:9083</value>
    </property>

2) 需要将hive的conf目录下的hive-site.xml 拷贝到 spark的 conf 目录下
	如果spark没有配置集群版本, 只需要拷贝node1即可 
	如果配置spark集群, 需要将文件拷贝每一个spark节点上
cp /export/server/hive/conf/hive-site.xml /export/server/spark/conf

3) 启动 hive的metastore服务:  
	cd /export/server/hive/bin
	nohup ./hive --service metastore &
	
	启动后, 一定要看到有runjar的出现
	
4) 启动 hadoop集群, 以及spark集群(如果采用local模式, 此集群不需要启动)

5) 使用spark的bin目录下: spark-sql 脚本 或者 pyspark 脚本打开一个客户端, 进行测试即可
/export/server/spark/bin 这个目录中spark-sql pyspark

测试小技巧:
	同时也可以将hive的hiveserver2服务启动后, 然后通过hive的beeline连接hive, 然后通过hive创建一个库, 在 spark-sql 脚本 或者 pyspark 脚本 通过 show databases 查看, 如果能看到, 说明集成成功了...


测试完成后, 可以将hive的hiveserver2 直接杀掉即可, 因为后续不需要这个服务:

首先查看hiveserver2服务的进程id是多少: 
	ps -ef | grep hiveserver2  或者 jps -m
	查看后,直接通过 kill -9  杀死对应服务即可
```



### 1.3 如何在代码中集成HIVE

```properties
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("演示 spark on hive的集成, 代码连接")
    # 1- 创建SparkSession对象
    spark = SparkSession\
        .builder\
        .master('local[*]')\
        .appName('udf_01')\
        .config('spark.sql.shuffle.partitions','4')\
        .config("spark.sql.warehouse.dir",'hdfs://node1:8020/user/hive/warehouse')\
        .config("hive.metastore.uris","thrift://node1:9083")\
        .enableHiveSupport()\
        .getOrCreate()
    # spark.sql.warehouse.dir : 指定 默认加载数据的路径地址,  在spark中, 如果不设置, 默认为本地路径
    # hive.metastore.uris : 指定 metastore的服务地址
    # enableHiveSupport() : 开启 和 hive的集成
    
    # 2- 构建数据集 
    spark.sql("show databases").show()
```

注意：

```properties
启动命令行spark-sql 或者 pyspark需要检查是哪个路径下的命令
因为安装了anaconda，anaconda也会带有spark-sql和pyspark命令，如果启动的是这两个，查不到beeline中创建的数据库和数据表的

# 确认命令路径
1 which spark-sql
2 which pyspark
# 如果没有配置spark的环境变量，则需要进入到该目录中，启动pyspark或者spark-sql
3 cd /export/server/spark/bin
4 ./spark-sql
5 ./pyspark
```



## 2- Spark SQL分布式执行引擎

​		目前, 我们已经完成了spark集成hive的操作, 但是目前集成后, 如果需要连接hive, 此时需要启动一个spark的客户端(pyspark,spark-sql, 或者代码形式)才可以, 这个客户端底层, 相当于启动服务项, 用于连接hive服务, 进行处理操作,  一旦退出了这个客户端, 相当于这个服务也不存在了, 同样也就无法使用了

​		此操作非常类似于在hive部署的时候, 有一个本地模式部署(在启动hive客户端的时候, 内部自动启动了一个hive的hiveserver2服务项)

```properties
大白话: 
	目前后台没有一个长期挂载的spark的服务(spark hiveserver2 服务), 导致每次启动spark客户端,都行在内部构建一个服务项, 这种方式 ,仅仅适合于测试, 不适合后续开发

```



如何启动spark的分布式执行引擎呢?  这个引擎可以理解为 spark的hiveserver2服务

```properties
如果没有配置spark on hive，我们会启动hiveserver2服务，启动这个服务目的：可以使用各种客户端连接hive，写hql程序。但是现在配置了spark on hive,并且把hiveserver2杀掉了，此时各种客户端无法连接到hive，就无法写hql语句。

```



```properties
cd /export/server/spark

./sbin/start-thriftserver.sh \
--hiveconf hive.server2.thrift.port=10000 \
--hiveconf hive.server2.thrift.bind.host=node1 \
--hiveconf spark.sql.warehouse.dir=hdfs://node1:8020/user/hive/warehouse \
--master local[*]

```

![image-20220303205258096](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220303205258096.png)

启动后: 可以通过 beeline的方式, 连接这个服务, 直接编写SQL即可:

```
cd /export/server/spark/bin
./beeline

输入:
!connect jdbc:hive2://node1:10000

```

![image-20220303205624780](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220303205624780.png)

相当于模拟了一个HIVE的客户端, 但是底层运行是spark SQL 将其转换为RDD来运行的



方式二:  如何通过 datagrip 或者 pycharm 连接 spark进行操作:

![image-20220303205825474](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220303205825474.png)

可以直接download, 也可以直接选择提供的jar包, 下载比较慢一些

![image-20220303210152928](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220303210152928.png)

![image-20230527111439622](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/assets/image-20230527111439622.png)



注意事项:   在使用download下载驱动的时候, 可能下载比较慢, 此时可以通过手动方式, 设置一个驱动:

![image-20220303210417893](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220303210417893.png)

![image-20220303210604706](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220303210604706.png)

![image-20220303210630973](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220303210630973.png)

![image-20220303210728759](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220303210728759.png)

![image-20230527111530527](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/assets/image-20230527111530527.png)



## 3- Spark SQL的运行机制

回顾: Spark RDD中 Driver的运行机制

```properties
1- 当Driver启动后, 首先会先创建SparkContext对象, 此对象创建完成后, 在其底层同时创建: DAGScheduler 和 TaskScheduler

2- 当遇到一个action算子后, 就会启动一个JOB任务, 首先DAGScheduler将所有action依赖的RDD算子全部合并在一起, 然后从后往前进行回溯, 遇到RDD之间有宽依赖(shuffle),就会拆分为一个新的stage阶段, 通过这种方式形成最终的一个DAG执行流程图, 并且划分好stage, 根据每个阶段内部的分区的数量, 形成每个阶段需要运行多少个线程(Task), 将每个阶段的Task线程放置到一个TaskSet列表中, 最后将各个阶段的TaskSet列表发送给 TaskSchedule

3- TaskSchedule接收到DAGschedule传递过来TaskSet, 依次的运行每一个stage, 将其各个线程分配给各个executor来进行运行执行

4- 后续不断监控各个线程执行状态, 直到整个任务执行完成

```

​		Spark SQL 底层依然需要将SQL语句翻译为Spark RDD操作 所以, Spark SQL也是存在上述的流程的, 只不过在上述流程中加入了 从spark SQL 翻译为Spark RDD的过程

![image-20220602113353620](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220602113353620.png)

内部详细流程:  大白话梳理

```properties
1- 当客户端将SQL提交到Spark中, 首先会将Sql提交给catalyst优化器

2- 优化器首先会先解析SQL, 根据SQL的执行顺序形成一个逻辑语法树(AST)

3- 接着对这个逻辑语法树 加入相关的元数据(连接metastore服务), 标识出需要从那里读取数据, 数据的存储格式, 用到那些表, 表的字段类型 等等..., 形成一个未优化的逻辑计划

4- catalyst开始执行RBO(规则优化)工作 :  将未优化的逻辑计划根据spark提供的优化方案对计划进行优化操作,比如说可以执行谓词下推, 列值裁剪, 提前聚合....  优化后得到一个优化后的逻辑执行计划

5- 开始执行 CBO(成本优化)工作:  在进行优化的时候, 由于优化的规则比较多, 可能匹配出多种优化方案, 最终可能会产生多个优化后的逻辑执行计划,导致形成多个物理执行计划,  此时通过代价函数(选最优), 选择出一个效率最高的物理执行计划

6- 将物理计划, 通过代码生成器工具 将其转换为 RDD程序

7- 将RDD提交到集群中运行, 后续的运行流程 跟之前RDD运行流程就完全一致了

```

专业术语:

```properties
1- sparkSQL底层解析是有RBO 和 CBO优化完成的
2- RBO是基于规则优化, 对于SQL或DSL的语句通过执行引擎得到未执行逻辑计划, 在根据元数据得到逻辑计划, 之后加入列值裁剪或谓词下推等优化手段形成优化的逻辑计划
3- CBO是基于优化的逻辑计划得到多个物理执行计划, 根据代价函数选择出最优的物理执行计划
4- 通过codegenaration代码生成器完成RDD的代码构建
5- 底层依赖于DAGScheduler 和TaskScheduler 完成任务计算执行

```

```properties
在Spark SQL中，逻辑计划（Logical Plan）和物理计划（Physical Plan）是查询执行过程中的两个关键概念。

逻辑计划是指查询被解析和转换为逻辑结构的过程。它表示了查询的逻辑操作和关系，但并不涉及具体的执行细节。逻辑计划是一个抽象的表示，描述了查询的原始逻辑意图。通常，逻辑计划以类似于关系代数的形式表示，使用操作符（例如选择、投影、连接等）来描述查询操作。逻辑计划不依赖于具体的数据存储格式或执行引擎，因此可以在不同的执行引擎上进行优化和转换。

物理计划是指将逻辑计划转换为实际执行的物理操作的过程。它描述了如何在实际的计算资源上执行查询。物理计划涉及数据的存储和访问方式、操作的执行顺序、并行度、数据分区等方面的决策。物理计划通常是与具体执行引擎密切相关的，它会考虑底层执行引擎的特性和限制，以及可用的硬件资源。物理计划的目标是生成一个高效执行的计划，以最小的时间和资源成本来执行查询。

在Spark SQL中，查询首先被转换为逻辑计划，然后经过一系列的优化和转换步骤，最终生成物理计划。逻辑计划可以通过调用explain方法来查看，而物理计划可以通过调用explain(true)方法来查看。这些计划的展示可以帮助开发人员理解查询的执行计划，并进行性能优化和调试。

```



如何查看物理执行计划呢?

- 方式一: 通过spark thrift server的服务界面: 大概率是 4040界面

![image-20220602115644571](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220602115644571.png)

![image-20220602115803986](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220602115803986.png)



- 方式二:  通过SQL的命令

```sql
explain SQL语句

```

![image-20220602115919042](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20220602115919042.png)

## 4. 综合案例-今日作业

### 4.0 准备工作

1 上传目录ECommerce到/export/data

2 `pip install -i https://mirrors.ustc.edu.cn/pypi/web/simple bottle `

3 启动程序：`cd /export/data/workspace/ECommerce/main` `sh run.sh`

### 4.1 新零售综合案例

数据结构介绍:  

```properties
InvoiceNo  string  订单编号(退货订单以C 开头)
StockCode  string  产品代码
Description string  产品描述
Quantity integer  购买数量(负数表示退货)
InvoiceDate string   订单日期和时间   12/1/2010 8:26
UnitPrice  double  商品单价
CustomerID  integer  客户编号
Country string  国家名字

字段与字段之间的分隔符号为 逗号

```

E_Commerce_Data.csv

<img src="G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211116111119327.png" alt="image-20211116111119327" style="zoom:80%;" />

拿到数据之后, 首先需要对数据进行过滤清洗操作:  清洗目的是为了得到一个更加规整的数据

```properties
清洗需求:
	需求一: 将客户id(CustomerID) 为 0的数据过滤掉 
	需求二: 将商品描述(Description) 为空的数据过滤掉
	需求三: 将日期格式进行转换处理:
		原有数据信息: 12/1/2010 8:26
		转换为: 2010-12-01 08:26

```

相关的需求(DSL和SQL):

```properties
(1) 客户数最多的10个国家
(2) 销量最高的10个国家
(3) 各个国家的总销售额分布情况
(4) 销量最高的10个商品
(5) 商品描述的热门关键词Top300
(6) 退货订单数最多的10个国家
(7) 月销售额随时间的变化趋势
(8) 日销量随时间的变化趋势
(9) 各国的购买订单量和退货订单量的关系
(10) 商品的平均单价与销量的关系

```

#### 2.1.1 完成数据清洗过滤的操作

清洗需求:

```properties
清洗需求:
	需求一: 将客户id(CustomerID) 为 0的数据过滤掉 
	需求二: 将商品描述(Description) 为空的数据过滤掉
	需求三: 将日期格式进行转换处理:
		原有数据信息: 12/1/2010 8:26
		转换为: 2010-12-01 08:26

将清洗的结果写出到HDFS上 /xls/output

```

代码实现操作: 

- 1- 创建一个子项目用于实现新零售的案例需求

![image-20230527143511640](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/assets/image-20230527143511640.png)

- 2- 将E_Commerce_Data.csv 放置到data目录下



- 3- 进行数据清洗代码实现

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'

if __name__ == '__main__':
    print("完成新零售的相关需求")

    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('xls_clear').getOrCreate()

    # 2- 读取外部文件数据
    df_init = spark.read.csv(
        path='file:///tmp/pycharm_project_956/day09/ECommerce/data/E_Commerce_Data.csv',
        header=True,
        inferSchema=True
    )

    # 3- 对数据执行清洗操作

    df_clear = df_init.where('CustomerID != 0 and Description is not null')
    df_clear = df_clear.withColumn(
        'InvoiceDate',
        F.from_unixtime(F.unix_timestamp(df_clear['InvoiceDate'],'M/d/yyyy H:mm'),'yyyy-MM-dd HH:mm')
    )

    # 4- 需要将清洗转换后的结果写出到HDFS上: /xls/output
    df_clear.write.mode('overwrite').csv(
        path='hdfs://node1:8020/xls/output',
        sep='\001'
    )

    # 5- 关闭spark对象
    spark.stop()

```



#### 2.1.2 需求统计分析操作

- 需求一: 客户数最多的10个国家: 统计每个国家的客户数, 取出前10位

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


def xuqiu_1():
    # SQL
    spark.sql("""
        select
            Country,
            count(distinct CustomerID) as cnt_cid
        from xls_t1
        group by Country order by cnt_cid desc limit 10
    """).show()
    # DSL:
    df_init.groupby(df_init['Country']).agg(
        F.countDistinct('CustomerID').alias('cnt_cid')
    ).orderBy('cnt_cid', ascending=False).limit(10).show()


if __name__ == '__main__':
    print("完成新零售案例的需求实现操作")

    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('xls_clear').getOrCreate()

    # 2- 读取数据
    # 定义schema方案
    schema = StructType()\
        .add('InvoiceNo',StringType()) \
        .add('StockCode', StringType()) \
        .add('Description', StringType()) \
        .add('Quantity', IntegerType()) \
        .add('InvoiceDate', StringType()) \
        .add('UnitPrice', DoubleType()) \
        .add('CustomerID',IntegerType()) \
        .add('Country', StringType())


    df_init = spark.read.csv(
        path='hdfs://node1:8020/xls/output',
        sep='\001',
        schema=schema
    )

    # 3- 对数据进行处理
    df_init.createTempView('xls_t1')
    # 3.1 客户数最多的10个国家: 统计每个国家的客户数, 取出前10位
    xuqiu_1()


```

- (2) 销量最高的10个国家 : 
  - 统计每个国家的销售的数量, 取前10位
- (3) 各个国家的总销售额分布情况
  - 统计各个国家的销售金额

```properties
def xuqiu_3():
    # SQL:
    spark.sql("""
        select
          Country,  
          round(sum(Quantity * UnitPrice),2) as total_price
        from xls_t1  where InvoiceNo not like 'C%'
        group by Country order by total_price desc
    """).show()
    # DSL:
    df_init.where('InvoiceNo not like "C%"').groupby('Country').agg(
        F.round(F.sum(df_init['Quantity'] * df_init['UnitPrice']), 2).alias('total_price')
    ).orderBy('total_price', ascending=False).show()



```

- (4) 销量最高的10个商品
  - 说明:  统计各个商品的销售的数量, 取前10个
- (5) 商品描述的热门关键词Top300
  - 说明:  统计每个关键词的数量, 取出前300个

```properties
def xuqiu_5():
    # SQL
    spark.sql("""
        select
            words,
            count(1) as  cnt_words
        from xls_t1 lateral view explode(split(Description,' ')) t2 as words
        group by  words order by cnt_words desc limit 300;
    """).show(300)
    # DSL
    df_init.withColumn('words', F.explode(F.split('Description', ' '))).groupby('words').agg(
        F.count('words').alias('cnt_words')
    ).orderBy('cnt_words', ascending=False).limit(300).show(300)

```

- (6) 退货订单数最多的10个国家
  - 说明: 统计每个国家的退货的订单数量, 找出前10为
- (7) 月销售额随时间的变化趋势
  - 说明: 统计每个月的销售额,
- (8) 日销量随时间的变化趋势
  - 说明: 统计每天的销售的数量
- (9) 各国的购买订单量和退货订单量的关系
  - 说明: 统计每个国家的订单量和退货订单量

```properties
def xuqiu_9():
    # SQL
    spark.sql("""
        select
            Country,
            count(distinct InvoiceNo) as cnt_oid,
            count( distinct if(InvoiceNo like 'C%' ,InvoiceNo , NULL)) as c_cnt_oid
        from xls_t1
        group by Country order by cnt_oid desc
    """).show()
    # DSL:
    # 说明: 如果要书写一些表达式, 比如 if  case when 这样的表达式, 需要通过 F.expr('书写表达式')
    df_init.groupby('Country').agg(
        F.countDistinct('InvoiceNo').alias('cnt_oid'),
        F.countDistinct(F.expr('if(InvoiceNo like "C%" ,InvoiceNo , NULL)')).alias('c_cnt_oid')
    ).orderBy('cnt_oid', ascending=False).show()

```

- (10) 商品的平均单价与销量的关系
  - 说明: 统计每个商品的 销售数量 和 平均单价



以下本次完整的代码

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'


def xuqiu_1():
    # SQL
    spark.sql("""
        select
            Country,
            count(distinct CustomerID) as cnt_cid
        from xls_t1
        group by Country order by cnt_cid desc limit 10
    """).show()
    # DSL:
    df_init.groupby(df_init['Country']).agg(
        F.countDistinct('CustomerID').alias('cnt_cid')
    ).orderBy('cnt_cid', ascending=False).limit(10).show()


def xuqiu_3():
    # SQL:
    spark.sql("""
        select
          Country,  
          round(sum(Quantity * UnitPrice),2) as total_price
        from xls_t1  where InvoiceNo not like 'C%'
        group by Country order by total_price desc
    """).show()
    # DSL:
    df_init.where('InvoiceNo not like "C%"').groupby('Country').agg(
        F.round(F.sum(df_init['Quantity'] * df_init['UnitPrice']), 2).alias('total_price')
    ).orderBy('total_price', ascending=False).show()


def xuqiu_5():
    # SQL
    spark.sql("""
        select
            words,
            count(1) as  cnt_words
        from xls_t1 lateral view explode(split(Description,' ')) t2 as words
        group by  words order by cnt_words desc limit 300;
    """).show(300)
    # DSL
    df_init.withColumn('words', F.explode(F.split('Description', ' '))).groupby('words').agg(
        F.count('words').alias('cnt_words')
    ).orderBy('cnt_words', ascending=False).limit(300).show(300)


def xuqiu_9():
    # SQL
    spark.sql("""
        select
            Country,
            count(distinct InvoiceNo) as cnt_oid,
            count( distinct if(InvoiceNo like 'C%' ,InvoiceNo , NULL)) as c_cnt_oid
        from xls_t1
        group by Country order by cnt_oid desc
    """).show()
    # DSL:
    # 说明: 如果要书写一些表达式, 比如 if  case when 这样的表达式, 需要通过 F.expr('书写表达式')
    df_init.groupby('Country').agg(
        F.countDistinct('InvoiceNo').alias('cnt_oid'),
        F.countDistinct(F.expr('if(InvoiceNo like "C%" ,InvoiceNo , NULL)')).alias('c_cnt_oid')
    ).orderBy('cnt_oid', ascending=False).show()


if __name__ == '__main__':
    print("完成新零售案例的需求实现操作")

    # 1- 创建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('xls_clear').getOrCreate()

    # 2- 读取数据
    # 定义schema方案
    schema = StructType()\
        .add('InvoiceNo',StringType()) \
        .add('StockCode', StringType()) \
        .add('Description', StringType()) \
        .add('Quantity', IntegerType()) \
        .add('InvoiceDate', StringType()) \
        .add('UnitPrice', DoubleType()) \
        .add('CustomerID',IntegerType()) \
        .add('Country', StringType())


    df_init = spark.read.csv(
        path='hdfs://node1:8020/xls/output',
        sep='\001',
        schema=schema
    )

    # 3- 对数据进行处理
    df_init.createTempView('xls_t1')
    # 3.1 客户数最多的10个国家: 统计每个国家的客户数, 取出前10位
    # xuqiu_1()

    #3.3  各个国家的总销售额分布情况 : 统计各个国家的总销售额
    #xuqiu_3()

    # 3.5 商品描述的热门关键词Top300  说明:  统计每个关键词的数量, 取出前300个
    #xuqiu_5()

    # 3.9  各国的购买订单量和退货订单量的关系   说明: 统计每个国家的订单量和退货订单量
    xuqiu_9()

```



### 4.2 在线教育案例

数据结构基本介绍:

```properties
student_id  string  学生id
recommendations string   推荐题目(题目与题目之间用逗号分隔)
textbook_id  string  教材id
grade_id  string   年级id
subject_id string  学科id
chapter_id strig   章节id
question_id string  题目id
score  integer  点击次数
answer_time  string  答题时间
ts  timestamp   时间戳


字段与字段之间的分隔符号为 \t

```

![image-20211116164814546](G:/2023%E5%A4%A7%E6%95%B0%E6%8D%AE%E5%B0%B1%E4%B8%9A%E7%8F%AD/06%E3%80%81%E9%98%B6%E6%AE%B5%E5%9B%9B%20Spark/spark/day09/2%20%E7%AC%94%E8%AE%B0/day09_pyspark%E8%AF%BE%E7%A8%8B%E7%AC%94%E8%AE%B0.assets/image-20211116164814546.png)

需求:

```properties
需求一: 找到TOP50热点题对应科目. 然后统计这些科目中, 分别包含几道热点题目
top50热点题目（按照题目分组聚合得到每个题目的点击此总和，根据这个指标进行查找）
再找top50对应的科目，把top50和原始表进行合并（目的是获取科目id），按照科目分组聚合即可得到结果

需求二: 找到Top20热点题对应的推荐题目. 然后找到推荐题目对应的科目, 并统计每个科目分别包含推荐题目的条数

```

数据存储在 资料中: eduxxx.csv



需求一: 找到TOP50热点题对应科目. 然后统计这些科目中, 分别包含几道热点题目

```properties
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# 锁定远端python版本:
os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

if __name__ == '__main__':
    print("在线教育案例需求实现")

    # 1- 构建SparkSession对象
    spark = SparkSession.builder.master('local[*]').appName('edu_project').getOrCreate()

    # 2- 读取数据
    df_init = spark.read.csv(
        path='file:///export/data/workspace/sz30_pyspark_parent/_05_edu_project/data/eduxxx.csv',
        sep='\t',
        inferSchema=True,
        header=True
    )
    df_init.createTempView('edu_t1')
    # 3- 处理数据
    # 需求一: 找到TOP50热点题对应科目. 然后统计这些科目中, 分别包含几道热点题目
    # SQL :
    # 第一步: 找到TOP50热点题
    df_top50 = spark.sql("""
        select
            question_id,
            sum(score) as total_question
        from edu_t1
        group by question_id order by total_question desc limit 50
    """)
    df_top50.createTempView('top50_q')
    # 第二步: 找到热点题对应的学科, 根据学科进行分组, 统计每个学科下有几道热点题
    spark.sql("""
        select 
            edu_t1.subject_id,
            count(distinct  top50_q.question_id) as sub_cnt
        from top50_q join edu_t1 on top50_q.question_id = edu_t1.question_id
        group by  edu_t1.subject_id
    """).show()

    # DSL
    df_top50 = df_init.groupby('question_id').agg(
        F.sum('score').alias('total_question')
    ).orderBy('total_question',ascending=False).limit(50)

    df_top50.join(df_init,'question_id').groupby('subject_id').agg(
        F.countDistinct(df_top50['question_id']).alias('sub_cnt')
    ).show()

```

# spark结构化流实时

## C01 认识实时计算

### 批处理

- 批处理：对`有界数据流`的处理通常被称为批处理。批处理不需要有序地获取数据。在批处理模式下，首先将数据流持久化到存储系统（文件系统或对象存储）中，然后对整个数据集的数据进行读取、排序、统计或汇总计算，最后输出结果

  > **有界数据流：**有界数据流有明确定义的开始和结束，可以在执行任何计算之前通过获取所有数据来处理有界流，处理有界流不需要有序获取，因为可以始终对有界数据集进行排序，有界流的处理也称为批处理
  >
  > - 有开始有结束
  > - 全部数据
  > - 可以得出最终结果
  > - 比如：数据文件、mysql中的表数据、hdfs上的文件数据-磁盘IO

### 流处理

- 实时流处理：对于`无界数据流`，通常在数据生成时进行实时处理。因为无界数据流的数据输入是无限的，所以必须持续地处理。数据被获取后需要立刻处理，不可能等到所有数据都到达后再进行处理。处理无界数据流通常要求以特定顺序（如事件发生的顺序）获取事件，以便能够保证推断结果的完整性

  > **无界数据流**：有一个开始但是没有结束，不会在生成时终止并提供数据，必须连续处理无界流，也就是说必须在获取后立即处理event。对于无界数据流我们无法等待所有数据都到达，因为输入是无界的，并且在任何时间点都不会完成。处理无界数据流通常要求以特定顺序（例如事件发生的顺序）获取event，以便能够推断结果完整性。
  >
  > - 有开始没有结束
  > - 得出的是部分结果
  > - 处理程序一直运行
  > - 比如：Kafka中的数据-网络IO

### 批流对比

- 批处理程序的容错不使用检查点。因为数据有限，所以恢复可以通过“完全重播”（重新处理）来实现。这种处理方式会降低常规处理的成本，因为它可以避免检查点
- 批处理的特点是有界、持久、大量，非常适合需要访问全套记录才能完成的计算工作，一般用于离线统计。
- 流处理的特点是无界、实时，无需针对整个数据集执行操作，而是对通过系统传输的每个数据项执行操作，一般用于实时统计。

### 流计算框架

- storm第一代实时流计算引擎  JStorm   毫秒
- spark streaming 微批计算引擎 - rdd  秒级
- spark structrue streaming 结构化流计算引擎 - df 毫秒
- flink 流批一体计算引擎 亚秒

## C02 结构化流概述

### 简介

![1774603014463](C:\Users\19471\AppData\Roaming\Typora\typora-user-images\1774603014463.png)

Spark2.0之前只有Spark Streaming   

- 基于RDD的   
- API比较复杂 2.0之后停止维护
- 延迟较高

Structured Streaming

- 基于SparkSQL引擎，`自动优化`
- 使用简单可以直接写SQL语句，增量查询模式
- 支持端到端精确一次的语义
- 支持事件时间（水印时间）

消息的三种语义：

> - at most once 至多一次 - 数据有可能丢失
> - at least once 至少一次 - 数据有可能重复
> - exactly once 精确一次 - 数据不多不少

### WordCount

```python
# coding:utf8
"""
通过Spark Structured Streaming统计单词数量
1、创建入口环境对象
2、通过网络端口读取数据流并转换成DF
3、处理数据
4、输出结果
"""

from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F

os.environ["SPARK_HOME"] = "/export/server/spark"
os.environ["PYSPARK_PYTHON"] = "/root/anaconda3/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/root/anaconda3/bin/python"

# spark环境入口对象
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("WordCount") \
    .getOrCreate()

# 监听node1:9999端口，获取数据并转换为DF
line_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "node1") \
    .option("port", 9999) \
    .load()

# 处理数据（单词 单词 单词）
word_count_df = line_df.select(F.explode(
    # 通过explode函数split数据，转换为列并取别名word
    F.split("value", " ")).alias("word")).groupby("word").count()

# 输出结果
word_count_df.writeStream.outputMode("complete").format("console").start().awaitTermination()
```

### 编程模型

![image-20220710143742958](https://markdownotepic.oss-cn-hangzhou.aliyuncs.com/imgs/image-20220710143742958.png?images)

SQL-structrue query language 结构化查询语言

先把数据流转化为无边界表，在这个表进行连续查询

## C03 结构化流的使用

### 开发的基础架构

无论是离线计算还是实时流计算：

- 数据输入-Source
- 数据处理-Process、Operation（与SparkSQL使用一样）
- 数据输出-Sink

### 内置的Source

- source读取代码模板

```python
# 创建环境入口对象
spark = SparkSession.master().\
	appName().\
    getOrCreate
    
# 读取数据
spark.readStream.format(数据输入格式).\
    option(属性，值).\
    option(属性，值).\
    schema(设置表结构).\
    load
```

- 读取文件数据

> - 参数
>
> | option参数         | 描述说明                                                     |
> | ------------------ | ------------------------------------------------------------ |
> | maxFilesPerTrigger | 每次触发时要考虑的最大新文件数 (默认: no max)                |
> | latestFirst        | 是否先处理最新的新文件, 当有大量文件积压时有用 (默认: false) |
> | fileNameOnly       | 是否检查新文件只有文件名而不是完整路径（默认值：false）将此设置为 `true` 时，<br />以下文件将被视为同一个文件，因为它们的文件名“dataset.txt”相同：<br /> “file:///dataset.txt” “s3://a/数据集.txt " <br />"s3n ://a/b/dataset.txt" <br />"s3a://a/b/c/dataset.txt" |
>
> - 样例
>
> ```python
> schema = StructType().add('name', StringType()).add('age', IntegerType()).add('hobby', StringType())
> 
> fileDf = spark.readStream.csv(path='file:///export/data/workspace/StructuredStreaming_Parent/data', schema=schema, sep=';', header=False)
> ```

- Rate数据输入

> - 参数
>
> | option参数    | 描述说明                                                     |
> | ------------- | ------------------------------------------------------------ |
> | rowsPerSecond | 每秒应该生成多少行 : （例如 100，默认值：1）                 |
> | rampUpTime    | 在生成速度变为rowsPerSecond. 使用比秒更细的粒度将被截断为整数秒。 （例如 5s，默认值：0s）: 每条数据的间隔时间 |
> | numPartitions | 生成行的分区号: （例如 10，默认值：Spark 的默认并行度）      |
>
> - 样例
>
> ```python
> rateDf = spark.readStream.format('rate').option("rowsPerSecond","10").option('numPartitions',2).load()
> ```

- Socket数据输入

> - 首先开启端口监听（使用nc工具）
> - yum install nc -y

- Kafka数据输入

### Operations

- DSL方式:

![img](https://markdownotepic.oss-cn-hangzhou.aliyuncs.com/imgs/wps1.jpg?images) 

- SQL的方式:

![img](https://markdownotepic.oss-cn-hangzhou.aliyuncs.com/imgs/wps2.jpg?images)

### 内置的Sink

#### 输出模式

- append模式

> - 追加输出：每次输出新追加的结果
>
> ![image-20230531164734996](https://markdownotepic.oss-cn-hangzhou.aliyuncs.com/imgs/image-20230531164734996.png?images)
>
> - 当计算过程中有聚合计算时不能使用append
> - 一般用于无状态计算（比如数据的读取、清洗、加载）
> - append输出模型是默认模型

- complete模式

> - 完整输出：每次输出都所有数据的结果
>
> ![image-20230531165431469](https://markdownotepic.oss-cn-hangzhou.aliyuncs.com/imgs/image-20230531165431469.png?images)
>
> - complete模式只能用于有聚合的计算
> - 一般用于有状态计算（新的结果需要用到之前的数据结果）

- update模式

> - 更新输出：每次输出新的数据结果
>
> ![image-20230531170244460](https://markdownotepic.oss-cn-hangzhou.aliyuncs.com/imgs/image-20230531170244460.png?images)
>
> - update模型每次只输出新来的数据的结果，不输出之前数据结果
>
> ![image-20230531170553222](https://markdownotepic.oss-cn-hangzhou.aliyuncs.com/imgs/image-20230531170553222.png?images)
>
> - update模式在有聚合计算时不支持排序

### 今日总结

- 结构化流的基本概念
  - 流处理的无界数据，批处理的是有界的全量数据
  - 流处理程序一直运行，批处理完程序就结束了
  - 流处理得到结果不是最终结果，批处理得到完整的结果
  - 支持SQL
  - 运行模型是无界表 unbounded table
- 结构化流处理实现
  - source
    - 代码模版：spark.readStream.format().option()......
    - socket：监听网络端口获取数据
    - file：监听文件目录获取数据
      - 一个文件只能被读取一次，文件内容发生变化不会影响处理结果
      - 只能监听目录下的文件
    - rate：自己产生测试数据
  - operation
    - dsl
    - sql
  - sink
    - 输出模式
      - append：追加模式，每次输出新追加的数据结果，计算过程不能有聚合
      - complete：完整模式，每次都输出完整的数据结果，计算过程必须有聚合
      - update：更新模式，每次输出被更新的数据结果，旧结果数据不被输出，支持有聚合和没有聚合，不能有排序



#### 输出位置

- console  Sink
- File Sink

> 文件接收器, 将输出存储到目录 , 仅支持`追加模式`
>
> - 可选项: 
>
> Path: 输出目录的路径，必须指定
>
> retention: 输出文件的生存时间 (TTL)。提交的批次早于 TTL 的输出文件最终将被排除在元数据日志中。这意味着`读取接收器file source`输出目录的读取器查询可能不会处理它们。可以将值提供为时间的字符串格式。（如“12h”、“7d”等）默认情况下它是禁用的。
>
> - 样例
>
> ```python
> # 输出到文件支持追加操作，但是每次只能追加当前批次的，由于spark底层是微批操作一个批次一个文件
> # 必须需要设置checkpoint
>  rate_df.writeStream \
>      .outputMode('append') \
>      .format('csv') \
>      .option('path', 'file:///root/tmp/OnlineBase/data/filesink') \
>      .option("checkpointLocation", "hdfs://node1:8020/spark/checkpoint") \
>      .start() \
>      .awaitTermination()
> ```



- Kafka Sink

> 将输出存储到kafka中的一个或多个主题, 三种输出模式均支持
>
> ```python
> writeStream
>  .format("kafka")
>  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
>  .option("topic", "updates")
>  .start()
> ```
>
> 

- 自定义 Sink

> jdbc
>
> foreach和foreachBatch操作允许您在流式查询的输出上应用任意操作和编写逻辑。它们的用例略有不同——虽然foreach 允许在每一行上自定义写入逻辑，但foreachBatch允许在每个微批次的输出上进行任意操作和自定义逻辑。
>
> - foreach函数：foreach 允许在每一行上自定义写入逻辑
>
> 方式一:
>
> ```python
> def process_row(row):
> 	# Write row to storage  
> 	pass
> 
> query = streamingDF.writeStream.foreach(process_row).start() 
> ```
>
> 
>
> 方式二: 
>
> ```python
> class ForeachWriter:
>  def open(self, partition_id, epoch_id):
>      # 进行初始化相关的操作
> 		pass
>  def process(self, row):
>      # 进程方法, 每次处理数据都会执行
> 		pass
>  def close(self, error):
>      # 关闭方法, 一般来说, 仅当JVM 或者python进程再中间崩溃的时候, 会被调度
> 		pass
> query = streamingDF.writeStream.foreach(ForeachWriter()).start()
> ```
>
> - foreachBatch函数：foreachBatch允许在每个微批次的输出上进行任意操作和自定义逻辑
>
> ```python
> def foreach_batch_function(df, epoch_id):
> 	# Transform and write batchDF    
> 	pass
> 
> streamingDF.writeStream.foreachBatch(foreach_batch_function).start() 
> 
> ```
>
> ![image-20230601111402960](https://markdownotepic.oss-cn-hangzhou.aliyuncs.com/imgs/image-20230601111402960.png?images)
>
> `spark与pyspark在调用foreachBatch方法时有冲突，需要注释spark_home，注释需要增加java_home`

- memory输出

> 输出作为内存表存储在内存中。支持追加和完成输出模式。 由于存储在内存中, 仅适用于`小数据量`, 慎重使用
>
> 经常作为小表被提前放入内存提高计算时效性
>
> ```python
> writeStream.format("memory").queryName("tableName").start()
> 
> ```
>
> 可以给每个查询Query设置名称Name，必须是唯一的，直接调用DataFrameWriter中queryName方法即可：
>
> ![img](https://markdownotepic.oss-cn-hangzhou.aliyuncs.com/imgs/wps3.jpg?images)

#### 触发器trigger

触发器Trigger决定了多久执行一次查询并输出结果

| 触发类型                                                     | 描述                                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| unspecified (default)不指定,默认方案                         | 如果没有明确指定触发设置，则默认情况下，查询将以微批处理模式执行，其中微批处理将在前一个微批处理完成后立即生成。 |
| Fixed interval micro-batches固定间隔微批次                   | 查询将以微批处理模式执行，其中微批处理将以用户指定的时间间隔启动。l 如果前一个微批处理在间隔内完成，则引擎将等待间隔结束，然后再启动下一个微批处理。l 如果前一个微批处理的完成时间比间隔时间长（即如果错过了一个间隔边界），那么下一个微批处理将在前一个完成后立即开始（即它不会等待下一个间隔边界）。l 如果没有新数据可用，则不会启动微批处理。 |
| One-time micro-batch一次性微批量                             | 仅执行一次, 在定期启动集群、处理自上一时期以来可用的所有内容然后关闭集群的情况下很有用。在某些情况下，这可能会显着节省成本。 |
| Continuous with fixed checkpoint interval (experimental)以固定检查点间隔连续, 目前为实验性 | 查询将以新的低延迟、连续处理模式执行, 不支持聚合操作         |

- 默认模式

```python
# 默认触发器
df.writeStream \
  .format("console") \
  .start()

```

- 固定间隔

```python
# 固定间隔微批次
df.writeStream \
  .format("console") \
  .trigger(processingTime='2 seconds') \
  .start()

```

- 一次性模式

```python
# 一次性微批
df.writeStream \
  .format("console") \
  .trigger(once=True) \
  .start()

```

- 连续模式

```python
# 以固定检查点间隔连续
df.writeStream
  .format("console")
  .trigger(continuous='1 second')
  .start()

```

![image-20230601113610957](https://markdownotepic.oss-cn-hangzhou.aliyuncs.com/imgs/image-20230601113610957.png?images)

`连续间隔不能有聚合操作`

### 检查点CheckPoint

#### `不同程序的checkpoint目录不能设置为一个`

在Structured Streaming中使用Checkpoint 检查点进行故障恢复。如果实时应用发生故障，可以恢复之前的查询的进度和状态，并从停止的地方继续执行。

如果设置了检查点位置，那么查询将所有进度信息（即每个触发器中处理的偏移范围）和运行聚合（例如词频统计wordcount）保存到检查点位置。检查点位置必须是HDFS或兼容文件系统

检查点设置在HDFS上可以保证文件数据安全，也可以保证远程访问

- 两种方式设置checkpoint local位置:   

```python
# 方式一: 基于DataStreamWrite设置
streamDF.writeStream.option("checkpointLocation", "path")

# 方式二: SparkConf设置
sparkConf.set("spark.sql.streaming.checkpointLocation", "path")

参数优先：代码算子>代码全局>提交设置>配置文件

```

- 检查点目录包含以下几个内容:

> 1、偏移量目录【offsets】：记录每个批次中的偏移量。为了保证给定的批次始终包含相同的数据，在处理数据前将其写入此日志记录。此日志中的第 N 条记录表示当前正在已处理，第 N-1 个条目指示哪些偏移已处理完成。
>
> 2、提交记录目录【commits】：记录已完成的批次，重启任务检查完成的批次与 offsets 批次记录比对，确定接下来运行的批次；
>
> 3、元数据文件【metadata】：metadata 与整个查询关联的元数据，目前仅保留当前job id
>
> 4、数据源目录【sources】：sources 目录为数据源(Source)时各个批次读取详情
>
> 5、数据接收端目录【sinks】：sinks 目录为数据接收端(Sink)时批次的写出详情
>
> 6、记录状态目录【state】：当有状态操作时，如累加聚合、去重、最大最小等场景，这个目录会被用来记录这些状态数据，根据配置周期性地生成.snapshot文件用于记录状态。

## C04 整合Kafka读写

### 准备工作

- 引入相关依赖

jar包下载地址：https://mvnrepository.com/

> - 将jar包上传到spark的jars目录下（/export/server/spark/jars/）适用于spark-submit on standalone
> - 将jar包上传到pyspark的jars目录下（/root/anaconda3/lib/python3.8/site-packages/pyspark/jars）
> - 将jar包上传到hdfs上的jars目录下（hdfs://node1:8020/spark/jars）适用于spark-submit on yarn

### 从Kafka读

- 必选参数

| 选项                    | 值                                                           | 说明                                                         |
| ----------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| assign                  | 通过一个Json 字符串的方式来表示: {"topicA":[0,1],"topicB":[2,4]} | 设置使用特定的TopicPartitions,Kafka源代码只能指定一个"assign"， "subscribe"或"subscribePattern"选项。 |
| subscribe               | 以逗号分隔的主题列表                                         | 要订阅的主题列表,Kafka源代码只能指定一个"assign"， "subscribe"或"subscribePattern"选项。 |
| subscribePattern        | 正则表达式字符串                                             | 订阅匹配符合条件的Topic。Kafka源代码只能指定一个“assign”，“subscribe”或“subscribePattern”选项。 |
| kafka.bootstrap.servers | 以逗号分隔的host:port列表                                    | 指定kafka服务的地址                                          |

- 读取kafka后返回的df的schema

| ***\*列名\****     | ***\*类型\**** |
| ------------------ | -------------- |
| key                | binary         |
| value              | binary         |
| topic              | string         |
| partition          | int            |
| offset             | long           |
| timestamp          | timestamp      |
| timestampType      | int            |
| headers (optional) | array          |

- 读取时的可选参数



### 往Kafka写

- 有一个写入Kafka的df

| ***\*列\****     | ***\*类型\****   |
| ---------------- | ---------------- |
| key (可选)       | string or binary |
| value (必填)     | string or binary |
| headers (可选)   | array            |
| topic (*可选)    | string           |
| partition (可选) | int              |

- 写入参数

必备配置:

| 选项                    | 价值                     | 意义                           |
| ----------------------- | ------------------------ | ------------------------------ |
| kafka.bootstrap.servers | 逗号分隔的主机列表：端口 | Kafka“bootstrap.servers”配置。 |

 可选配置

| 选项           | 值      | 默认     | 查询类型         | 意义                                                         |
| -------------- | ------- | -------- | ---------------- | ------------------------------------------------------------ |
| topic          | string  | 没有任何 | 流式传输和批处理 | 设置将在 Kafka 中写入所有行的主题。此选项会覆盖数据中可能存在的任何主题列。 |
| includeHeaders | boolean | 错误的   | 流式传输和批处理 | 是否在行中包含 Kafka 标头。                                  |

![image-20230601154315787](https://markdownotepic.oss-cn-hangzhou.aliyuncs.com/imgs/image-20230601154315787.png?images)

`spark写入Kafka必须得设置检查点`









## D05 物联网应用模拟

对物联网设备状态信号数据，实时统计分析:

1）、信号强度大于30的设备；

2）、各种设备类型的数量；

3）、各种设备类型的平均信号强度；

select   count(deviceType) as count, avg(deviceSignal) as avg  from device where deviceSignal > 30 group by deviceType

- 数据schema

> deviceID     设备ID
>
> deviceType  设备类型
>
> deviceSignal 设备信号强度

`有些时候spark必须与pyspark的版本一致， 相关依赖的版本也得与pyspark的版本一致`



