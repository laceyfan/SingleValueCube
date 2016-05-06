import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext, Partitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
/**
 * Created by sa on 2015/9/24/0024.
 */
object ComputeBUCParkOrder {
  System.setProperty("hadoop.home.dir", "d:\\hadoop-common-2.2.0-bin-master\\")
  def main(args: Array[String]) {
    val child_D_map = new mutable.HashMap[Int, Int]() //child_map中键为子节点序号，值为生成该子节点的子节点时需要的值（该数值逐层减一，同一层兄弟之间减一）
    val child_parent_map = new mutable.HashMap[Int, Int]()
    val current_child_D_map = new mutable.HashMap[Int, Int]()
    current_child_D_map += (1 -> 4)
    child_D_map+=(1 -> 4)
    child_parent_map+=(1 -> 0)

    /*生成BUC处理树*/
    for (i <- 4 to(1, -1)) {
      println(i + "-------" + current_child_D_map.isEmpty)
      while (!current_child_D_map.isEmpty) {
        val delete_ID = current_child_D_map.head._1
        val parent_ID = (current_child_D_map.head._1) + 1 //该层第一个孩子节点序号，其兄弟节点的序号依赖于第一个孩子节点
        val delete_value = current_child_D_map.head._2
        val D_value = (current_child_D_map.head._2) - 1 //该层第一个孩子节点的“值”，“值”在生成兄弟节点时需要
        if (delete_value > 0) {
          println("delete value:" + delete_ID)
          val first_child = (parent_ID -> D_value) //生成第一个孩子节点
          child_parent_map += (parent_ID -> delete_ID)
          child_D_map += first_child
          current_child_D_map += first_child
          var pow_value = 0;
          var D_value_tmp = D_value;
          for (j <- D_value to(1, -1)) {
            pow_value = pow_value + math.pow(2, j).toInt
            val new_value = parent_ID + pow_value
            D_value_tmp = D_value_tmp - 1
            child_D_map += (new_value -> D_value_tmp)
            current_child_D_map += (new_value -> D_value_tmp)
            child_parent_map += (new_value -> delete_ID)
          }
        }
        current_child_D_map -= (delete_ID)
      }
    }
    println(child_D_map)
    println("child parent map"+child_parent_map)
    val p_list=child_parent_map.toList
    val child_parent_sort=p_list.sortWith((s1,s2)=>s1._1<s2._1).toMap
    val c_list=child_D_map.toList
    val child_D_sort=c_list.sortWith((s1,s2)=>s1._1<s2._1).toMap
    println("child parent map sorted:"+child_parent_sort)
    println("child D map sorted:"+child_D_sort)

    /*BUC计算*/
    val file_path = "./data_100K_4" //=args(0)//输入文件路径
    val dimension_in = 4 //=args(1)//输入数据维度
    val dimension = dimension_in.toInt
    val min_sup = 100 //=args(2)//最小支持度
    val partition = 8 //=args(3)
    //val spark=new SparkContext("spark://192.168.1.125:7077","BUC")
    val sc = new SparkContext("local", "BUC")
    val infile = sc.textFile(file_path)
    val tree_node_sum = math.pow(2, dimension).toInt
    val rdd_new_array = new Array[RDD[(String, String)]](tree_node_sum+1)
    rdd_new_array(1) = infile.map((" ",_))
    //val result_RDD=new Array[RDD[String]](tree_node_sum)
    for (i <- 2 to tree_node_sum) {
      println(i + "-----start-----" + tree_node_sum)
      val parent_index = child_parent_sort(i)
      val D_index = child_D_sort(i)
      val drop_index=child_D_sort(parent_index)-D_index
      /*println("parent index : "+parent_index)
      println("D index : "+D_index)
      println("drop index : "+drop_index)
      println("first : "+rdd_new_array(parent_index).first())*/
      val first_process = rdd_new_array(parent_index).map{
        line =>
          val key = line._1
          val value = line._2.split(" ")
          var KEY=""
          KEY = key + "#" + value(drop_index-1)
          //println("value length : "+value.length+" value:"+value.toList+" line:"+line)
          //val new_value = value.drop(drop_index+1).mkString(" ")
          val new_value = value.drop(drop_index).mkString(" ")
          val new_K_V=(KEY,new_value)
          new_K_V
      }.cache()
      val minsup=min_sup.toInt
      val key_value_map=first_process.countByKey().filter(_._2>100).keySet
      rdd_new_array(i)=first_process.filter(line=>key_value_map.contains(line._1))
      println(i+" : "+rdd_new_array(i).count())
    }
  }
}
