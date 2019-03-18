package cn.qm

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn

object caipinALS_Demo {
  def main(args: Array[String]): Unit = {
    //准备数据
    println("======数据准备阶段=====")
    //拿到用户对菜品偏好的rdd（用户，菜品id，次数）和菜品rdd（菜品id,菜品名称）
    val (ratings, moviesTitle) = prepareData()
    //训练模型
    println("======训练阶段=====")
    println("开始使用" + ratings.count() + "条评比数据进行训练模型")
    //计算5个模型的方差，最小的那个记为最佳模型
    //这里的迭代次数要根据各自集群机器的硬件来选择，由于我的机器不行我迭代次数为20，再多就会内存溢出
    //ALS交替最小二乘法
    /*
    参数1：对应的是隐因子的个数，这个值设置越高越准，但是也会产生更多的计算量。一般将这个值设置为10-200；

    参数2：对应迭代次数，一般设置个10就够了；交替最小二乘法的迭代次数
    参数3：该参数控制正则化过程，其值越高，正则化程度就越深。一般设置为0.01。防止过度拟合
      正则化参数一般是可以从0.0001 ，0.0003，0.001，0.003，0.01，0.03，0.1，0.3，1，3，10这样每次大概3倍的设置，
      先大概看下哪个值效果比较好，然后在那个比较好的值（比如说0.01）前后再设置一个范围，比如（0.003，0.3）之间，
      间隔设置小点，即0.003，0.005，0.007，0.009，0.011，，，，。当然，如果机器性能够好，而且你够时间，可以直接设
      置从0到100，间隔很小，然后一组参数一组的试试也是可以的。
     */
    val model = ALS.train(ratings, 5, 20, 0.1)

    //推荐阶段
    println("======推荐阶段=====")
    recommend(model, moviesTitle)
    println("======完成！=====")
    //完成
  }
  //创建数据准备
  def prepareData(): (RDD[Rating], Map[Int, String]) = {
    val sc = new SparkContext(new SparkConf().setAppName("Recommend").setMaster("local[4]"))
    print("开始读取用户评分数据")
    val rawUserData: RDD[String] = sc.textFile("D:\\hadoop\\data\\sparkmllib\\Ratings.dat.parquet")
    val ratingsRDD: RDD[Rating] = rawUserData.map(line => {
      val s = line.split("::")
      //用户ID,菜品ID,次数
      //      Rating(s(0).toInt, s(1).toInt, s(2).toDouble)
      ( s(0).toInt+"::"+s(1).toInt,1)
    }).reduceByKey(_+_)
      .map(line => {
        val s = line._1.split("::")
        //用户ID,菜品ID,次数
        Rating(s(0).toInt, s(1).toInt, line._2.toDouble)
      })
    println("共计：" + ratingsRDD.count.toString() + "条ratings")
    //创建菜品ID与名称对照表
    println("开始读取菜品数据中...")
    val itemRDD = sc.textFile("D:\\hadoop\\data\\iTudou\\Movies.dat.parquet")
    //id，名称
    val movieIDAndTitle = itemRDD.map(line => line.split("::")).map(m => (m(0).toInt, m(1))).collect().toMap

    //显示数据记录数
    val numRatings = ratingsRDD.count()
    val numUsers = ratingsRDD.map(_.user).distinct().count()
    val numMovies = ratingsRDD.map(_.product).distinct().count()
    println("共计：ratings" + numRatings + " User " + numUsers + " Movie " + numMovies)
    (ratingsRDD, movieIDAndTitle)
  }
  def recommend(model: MatrixFactorizationModel, movieTitle: Map[Int, String]) = {
    var choose = ""
    while (choose != "3") {
      //如果选择3离开，纠结束运行程序
      println("请选择要推荐类型: 1、针对用户推荐电影 2、针对电影推荐给感兴趣的用户 3、离开？")
      choose = StdIn.readLine() //读取用户输入
      if (choose == "1") {
        //如果输入1
        print("请输入用户id:")
        val inputUserID = StdIn.readLine() //读取用户id
        recommendMovies(model, movieTitle, inputUserID.toInt) //针对此用户推荐电影
      } else if (choose == "2") {
        //如果输入2
        print("请输入电影id:")
        val inputMovieID = StdIn.readLine() //读取MovieID
        recommendUsers(model, movieTitle, inputMovieID.toInt) //针对此电影推荐用户
      }
    }
  }
  //针对用户推荐电影程序代码
  def recommendMovies(model: MatrixFactorizationModel, movieTitle: Map[Int, String], inputUserID: Int) = {
    val RecommendMovie: Array[Rating] = model.recommendProducts(inputUserID, 10)
    var i = 1
    println("针对用户id-" + inputUserID + "-推荐下列电影：")
    RecommendMovie.foreach { r =>
      println(i.toString() + "." + movieTitle(r.product) + "  评分:" + r.rating.toString())
      i += 1
    }
  }

  //针对电影推荐用户程序代码
  def recommendUsers(model: MatrixFactorizationModel, movieTitle: Map[Int, String], inputMovieID: Int) = {
    val RecommendUser = model.recommendUsers(inputMovieID, 10) //针对movieID推荐前10位用户
    var i = 1
    println("针对电影id-" + inputMovieID + "-电影名：" + movieTitle(inputMovieID.toInt) + "-推荐下列用户")
    RecommendUser.foreach { r =>
      println(i.toString() + "用户id:" + r.user + "评分:" + r.rating.toString())
      i = i + 1
    }
  }

}
