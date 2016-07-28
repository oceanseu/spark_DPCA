
import java.lang.annotation.Target
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import scopt.OptionParser

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Set, Map}
import scala.collection.mutable.Set
import scala.math.BigDecimal.RoundingMode

/**
 * Created by ocean on 2015/5/15.
 */
object Driver_dest_prefer {

  val PI=3.1415926
  val EARTH_RADIUS=6378.137
  val time_zone=List(List("07","08"),List("17","18"),List("21","22"),List("07","08","17","18","21","22"))

  val df = new DecimalFormat()
  df.setMaximumFractionDigits(4)


  val MAX_DISTANCE=3000.0
  val MIN_DC=2000.0


  case class Params(
                     input: String = null,
                     output: String = null
                     )

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("Driver") {
      head("Driver dest prefer")
      opt[String]("input")
        .required()
        .text("input data of driver orders")
        .action((x, c) ⇒ c.copy(input = x))
      opt[String]("output")
        .text("output data of driver prefer")
        .action((x, c) ⇒ c.copy(output = x))
    }

    parser.parse(args, defaultParams).map { params ⇒
      run(params)
    } getOrElse {
      sys.exit(1)
    }
  }

  def  OrderFun(line:Row):Order={
    new Order(
    line.getString(0).toLong,
    line.getString(1).toLong,
    line.getString(2),
    line.getString(3),
    line.getString(4),
    line.getString(5),
    line.getString(6),
    line.getString(7)
    )
  }

  def rad(d:Double):Double={
       return d*PI/180.0;
  }

  def sphere_distance(lng1:Double,lat1:Double,lng2:Double,lat2:Double): Double ={
       var radlat1=rad(lat1)
       var radlat2=rad(lat2)
       var a=radlat1-radlat2
       var b=rad(lng1)-rad(lng2)
       var s=2*math.asin(math.sqrt(math.pow(math.sin(a/2),2)+math.cos(radlat1)*math.cos(radlat2)*math.pow(math.sin(b/2),2)))
       s=Math.round(s*EARTH_RADIUS*1000)
       return Math.max(s,-s)
  }

  def DPCA(data:Iterable[Order]):Array[Prefer]={
      val d=data.toArray[Order]
      var result=ArrayBuffer[Prefer]()
      time_zone.foreach(time=>{
        var time_data=ArrayBuffer[Order]()
        var start_hour=time.head
        var end_hour=time.last
        data.foreach(order=>{
          var order_time=order.strive_time
          var hour=order_time.split(" ")(1).split(":")(0)
          //if (start_hour<=hour && hour<=end_hour){
          if(time.contains(hour)){
            time_data+=order
          }
        })
        var temp=DPCA_TIME_DATA(time_data.toArray,start_hour,end_hour)
        result =result++temp
      })
    result.toArray
    //result
  }


  def Select_RHO(size:Int):Int={    //确定密度峰的密度阈值
     var RHO=1
    if (size<20){
      RHO=Math.min(Math.max(size/3,2),2)
    }else{
      RHO=Math.min(Math.max(3,size/20),8)  //由10改为15
      //加上max.min
    }
    RHO
  }

  def  Cal_type(data:Array[Order],distance:Array[Array[Double]],RHO:Int):Array[(Int,Int)]={ //计算每个订单对应坐标点的类型，_1代表类型，1代表核心点，2代表非核心点，3代表噪声点
    var order_type=new Array[(Int,Int)](data.length)                                        //_2对于不同类型的点有不同的意义，核心点代表密度，非核心点代表离其最近的核心点下标，噪声点代表其密度值
    for(i<-0 to order_type.length-1){
      order_type(i)=(0,0)
    }
    for(i<-0 to data.length-1){
      var count=0
      for(j<-0 to data.length-1){
        if(distance(i)(j)<MIN_DC)
          count+=1
      }
      if(count>=RHO){   //由>改为>=
        order_type(i)=(1,count)
      }
      else{
        order_type(i)=(0,count)
      }
    }
    var i=0
    while(i<data.length){
      if(order_type(i)._1==0){
        var j=0
        var min_distance=MIN_DC
        while( j<data.length){
          if(order_type(j)._1==1 && distance(i)(j)<MIN_DC && distance(i)(j)<min_distance){
            order_type(i)=(2,j)
            min_distance=distance(i)(j)
          }
          j+=1
        }
        if(order_type(i)._1==0)
          order_type(i)=(3,order_type(i)._2)
      }
      i+=1
    }
       order_type
  }

  def Merge_Core(order_type:Array[(Int,Int)],distance:Array[Array[Double]]): Array[Int] ={  //合并核心点，返回核心点和非核心点、噪声点所指向的目标核心点
    var Merge_Order=new Array[Int](order_type.length)
    var i=0
    while(i<order_type.length){
      if(order_type(i)._1==2){
       // Merge_Order+=((i,order_type(i)._2))
        Merge_Order(i)=order_type(i)._2
      }
      if(order_type(i)._1==3){
        Merge_Order(i)= -1   //噪声点
      }
      if(order_type(i)._1==1){
        var j=0
        var min_distance=MAX_DISTANCE
        var target=i
        while(j<order_type.length){
          if(order_type(j)._1==1 && i!=j && distance(i)(j)<MAX_DISTANCE && distance(i)(j)<min_distance){
              if(order_type(i)._2<order_type(j)._2)
                {
                  target=j
                  min_distance=distance(i)(j)
                }
              if(order_type(i)._2==order_type(j)._2 && i<j){
                 target=j
                 min_distance=distance(i)(j)
              }
          }
          j+=1
        }
        //Merge_Order+=((i,target))
        Merge_Order(i)=target
      }
      i+=1
    }
    Merge_Order.toArray
  }

  def find_set(index:Int,Target_node:Array[Int]):Int={

        var result=index
        while(result!=Target_node(result)){
          result=Target_node(result)
        }
        return result
  }

  def DPCA_TIME_DATA(data:Array[Order],start_hour:String,end_hour:String):Array[Prefer] ={
    var result=ArrayBuffer[Prefer]()

    if(data.length<5){
      return result.toArray
    }

    val RHO=Select_RHO(data.length)

    val distance=Cal_Distances(data)

    val order_type=Cal_type(data,distance,RHO)  //以tuple数组形式保存每个订单的信息，_1代表订单类型，_2对于核心点和噪声点为密度值，对于非核心点为离其最近的核心点

    val Target_node=Merge_Core(order_type,distance)  //计算核心点和非核心点所指向的目标节点的订单下标，得到这个结果后就可以方便的进行归并操作，合并得到聚类结果

    var Cluster=new Array[Int](Target_node.length)
    for(i<-0 to Target_node.length-1){
       if(Target_node(i)== -1)
          Cluster(i)= -1
       else
          Cluster(i)=find_set(i,Target_node)
    }

    var Order_Cluster=Map[Int,Set[Int]]()
    for(i<-0 to Cluster.length-1){
      if(Cluster(i)!= -1) {
        if (Order_Cluster.contains(Cluster(i)))
          Order_Cluster(Cluster(i)) += i
        else
          Order_Cluster += (Cluster(i) -> Set(i))
      }
    }

    val Order_Count=data.length.toDouble
    val driver_id=data(0).driver_id
    Order_Cluster.foreach(C=>{
      val orders_index=C._2
      val cluster=C._1
      val driver_type=data(cluster).driver_type
      val dest_prefer=data(cluster).lng+","+data(cluster).lat
      val dest_rho=order_type(cluster)._2
      var dest_area=0.0
      var distance_avg=0.0
      orders_index.foreach(oid=>{
        dest_area+=distance(cluster)(oid)
        distance_avg+=data(oid).distance.toDouble
      })
      dest_area=dest_area/orders_index.size

      var  dest_bias=0.0
      orders_index.foreach(oid=>{
        dest_bias+=Math.pow(distance(cluster)(oid)-dest_area,2)
      })
      dest_bias=Math.sqrt(dest_bias/orders_index.size)

      val dest_count=orders_index.size
      val dest_rate=dest_count.toDouble/Order_Count
      distance_avg=distance_avg/orders_index.size
      var distance_variance=0.0
      var bonus=0.0
      var bonus_count=0.0
      orders_index.foreach(oid=>{
        distance_variance+=Math.pow(data(oid).distance.toDouble-distance_avg,2)
        if(data(oid).bonus.toDouble!=0.0){
          bonus_count+=1.0
          bonus+=data(oid).bonus.toDouble
        }
      })
      distance_variance=Math.sqrt(distance_variance/orders_index.size)

      var bonus_avg=0.0
      if(bonus_count!=0)
        bonus_avg=bonus/bonus_count
      //val bonus_rate=bonus_count/orders_index.size
     //val bonus_rate=bonus_count
     val prefer_rate=2.0/(1.0+Math.exp(dest_count*dest_rate*(-1.0)/40))-1.0
      val one_prefer=new Prefer(driver_id,driver_type,start_hour,end_hour,dest_prefer,dest_rho.toString,Math.floor(dest_area).toString,Math.floor(dest_bias).toString,dest_count.toString,(df.format(dest_rate)).toString,Math.floor(distance_avg).toString,Math.floor(distance_variance).toString,Math.ceil(bonus_avg).toString,bonus_count.toString,df.format(prefer_rate).toString)
      println(driver_id,dest_prefer,dest_area,dest_count,dest_rate)
      result+=one_prefer
    })

    result.toArray
  }

  def Cal_Distances(data:Array[Order]):Array[Array[Double]]={   //用订单数组的下标来标识每一个订单，用二维数组来保存订单之间的距离
    var res=Array.ofDim[Double](data.length,data.length)
    for(i<-0 to data.length-1;j<-i+1 to data.length-1){
           var a=data(i)
           var b=data(j)
           var distance=sphere_distance(a.lng.toDouble,a.lat.toDouble,b.lng.toDouble,b.lat.toDouble)
           res(i)(j)=distance
           res(j)(i)=distance
    }
    res
  }


  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"Driver_dest_prefer")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    assert(params.input!=null)
    val hiveContext=new HiveContext(sc)
    import hiveContext._
    val inputdata:DataFrame=sql(params.input).repartition(sc.defaultParallelism)
    inputdata.schema.printTreeString()

    var data=inputdata.map(line=>OrderFun(line)).groupBy(_.driver_id).repartition(sc.defaultParallelism).cache()

    println("司机数目为"+data.count())

    var  result=data.map(a=>(a._1,DPCA(a._2))).flatMap(a=>a._2).cache()


    var driver_count=result.map(a=>(a.driver_id)).distinct().count()

    println("形成聚类中心司机数"+driver_count)
    println("形成聚类中心共"+result.count())
    println("司机人均形成聚类中心数"+result.count.toDouble/driver_count.toDouble)



    var gulf_driver_all=data.filter(x=>{x._2.toIterator.next().driver_type=="gulf"}).count()
    var gulf_driver_result=result.filter(x=>x.driver_type=="gulf").cache()
    var gulf_driver_count=gulf_driver_result.map(a=>(a.driver_id)).distinct().count()

    println("专车司机数"+gulf_driver_all)
    println("专车形成聚类中心司机数"+gulf_driver_count)
    println("专车形成聚类中心共"+gulf_driver_result.count())
    println("专车司机人均形成聚类中心数"+gulf_driver_result.count.toDouble/gulf_driver_count.toDouble)

    var taxi_driver_all=data.filter(x=>{x._2.toIterator.next().driver_type=="taxi"}).count()
    var taxi_driver_result=result.filter(x=>x.driver_type=="taxi").cache()
    var taxi_driver_count=taxi_driver_result.map(a=>(a.driver_id)).distinct().count()

    println("出租车司机数"+taxi_driver_all)
    println("出租车形成聚类中心司机数"+taxi_driver_count)
    println("出租车形成聚类中心共"+taxi_driver_result.count())
    println("出租车司机人均形成聚类中心数"+taxi_driver_result.count.toDouble/taxi_driver_count.toDouble)

    result.map(line=>s"${line.driver_id}\t${line.driver_type}\t${line.time_start}\t${line.time_end}\t${line.dest_prefer}\t${line.dest_rho}\t${line.dest_area}\t${line.dest_bias}\t${line.dest_count}\t" +
      s"${line.dest_rate}\t${line.distance_avg}\t${line.distance_variance}\t${line.bonus_avg}\t${line.bonus_count}\t${line.prefer_rate}").saveAsTextFile(params.output)

    sc.stop()
  }
}

