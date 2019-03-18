//package cn.qm
//
//import org.apache.spark.mllib.clustering.KMeans
//import org.apache.spark.mllib.linalg.{Vector, Vectors}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//object caipinKMeans_Demo {
//  def main(args: Array[String]): Unit = {
//    // 设置运行环境
//    val conf = new SparkConf().setAppName("Kmeans").setMaster("local[4]")
//    val sc = new SparkContext(conf)
//    //装载数据集
//    val data = sc.textFile("D:\\hadoop\\data\\sparkmllib\\kmeans_data.txt", 1)
//    val parsedData: RDD[Vector] = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble)))
//    //将数据集聚类，2个类，20次迭代，形成数据模型
//    val numClusters = 2
//    val numIterations = 20
//    val model = KMeans.train(parsedData, numClusters, numIterations)
//    //数据模型的中心点
//    println("Cluster centers:")
//    for (c <- model.clusterCenters) {
//      println(" " + c.toString)
//    }
//    model.
//
//
//  }
//}
