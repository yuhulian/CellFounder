package com.simon

/**
  * Created by simon&wqj on 2017/7/27.
  */

import java.text.{DecimalFormat, SimpleDateFormat}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object CellFounder {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("CellFounder")
    val sc: SparkContext = new SparkContext(conf)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dfd = new DecimalFormat("#.00000")

    val crt_val:Double = 0.00001
    val joureyInputPath = args(0)
    val cell_refInputPath = args(1)
    val pointInputPath = args(2)
    val outputPath = args(3)

    def diff_Int=(t1:Int,t2:Int)=>(t1 - t2)
    def diff_Double=(d1:Double,d2:Double)=>(d1 - d2 + crt_val)

    //step1. get some impossible journeys
    //filter journey with distance over than 10km and speed than 5, which means they could be impossible ones
    //(-9127230254832528338-0,11335|2.51219)
    val jourey = sc.textFile(joureyInputPath).map(_.split("\\|")).map(x => {
      var key = x(1) + "-" + x(0) //user_id-journey_id
      var start_time = x(2)
      var end_time = x(3)
      var begin = sdf.parse(start_time)
      var end = sdf.parse(end_time)
      var duration = ((end.getTime - begin.getTime()) / 1000) + crt_val
      var distance = x(12)
      var speed2 = (distance.toDouble / duration.toDouble)
      var speed: String = dfd.format(speed2)
      (key, distance, speed)
    }).filter(x => (x._2.toInt > 10000 && x._3.toDouble < 5)).map(x => { (x._1, x._2 + "|" + x._3) })

    //prepare cell_ref to get coordinate so that speed can be calculated
    //映射cell_ref为(k,v),k is ci-lac,value is longitude|latitude
    val cell_ref_data = sc.textFile(cell_refInputPath).map(x => {
      var o = x.split("\t");
      var key = o(1) + "-" + o(0)
      var value = o(2) + "|" + o(3)
      (key, value)
    })


    //extract jvp for key and some useful attributes
    //映射journey_via_point为(k,v)
    val point_data1 = sc.textFile(pointInputPath).map(x => {
      var o = x.split("\\|");
      var key = o(5)
      var value = o(0) + "|" + o(1) + "|" + o(2) + "|" + o(3) + "|" + o(4)
      (key, value)
    })

    var data = point_data1.join(cell_ref_data).map(x => {
      x._2._1 + "|" + x._2._2  + "|" +x._1   //新添加x._1
    }).distinct()

    //data.coalesce(56).saveAsTextFile(outputPath+"/data")
    var point = data.map(x => {
      var o = x.split("\\|");
      var key = o(2) + "-" + o(1) //主要可以
      var value = (o(0).toInt, sdf.parse(o(3)).getTime() / 1000, (o(5).toDouble, o(6).toDouble),o(7))
      (key, value)
    })
    //(1922121077349371563-4,4-3|110095.66535)
    //point数据集和自己关联，合并后一个point与前一个point，计算时间和人距离。
    var point_data = point.join(point).filter(x => {
      var flag = diff_Int(x._2._1._1.toInt,x._2._2._1.toInt); flag == 1
    }).map(x => {
      var count_time = diff_Double(x._2._1._2.toDouble,x._2._2._2.toDouble)
      var x1=x._2._1._3._1
      var y1=x._2._1._3._2
      var x2=x._2._2._3._1
      var y2=x._2._2._3._2
      var lacAndCI =x._2._1._4 +"|"+x._2._2._4
      var distance = BallDistance.LantitudeLongitudeDist(x1, y1, x2, y2)
      var speed = Math.abs(distance / count_time)
      (x._1, x._2._1._1 + "-" + x._2._2._1 + "|" + dfd.format(speed)+"|"+lacAndCI)

    })
    //point_data.coalesce(56).saveAsTextFile(outputPath+"/point_data")
    //jourey关联point_data得到最终结果
    //-6188270518652038107-6|47-46|10.04292|25401|1.82243
    var result = jourey.join(point_data).map(x => {
      x._1 + "|" + x._2._2 + "|" + x._2._1
    })

    result.distinct().coalesce(56).saveAsTextFile(outputPath+"/result")
  }
}
