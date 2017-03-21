package org.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result


object ReadingAndWritingHBaseDataUsingSpark{
  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("Spark-HBase");
    val sc = new SparkContext(sparkConf);
    
    val hbaseConfig = HBaseConfiguration.create();
    val table = "employee"
    
    //Configuration setting for getting hbase data
    hbaseConfig.set(TableInputFormat.INPUT_TABLE,table)
    hbaseConfig.set("zookeeper.znode.parent ","/hbase-unsecure")
    hbaseConfig.addResource("/home/spark/Documents/softwares/hbase-1.2.4/conf/hbase-site.xml")
    
    //Check if table present
    val hbaseAdmnin = new HBaseAdmin(hbaseConfig)
    if(!hbaseAdmnin.isTableAvailable(table)){
      val tableDesc = new HTableDescriptor(table)
      tableDesc.addFamily(new HColumnDescriptor("personal data"))
      tableDesc.addFamily(new HColumnDescriptor("professional data"))
    }
  //Insert Data into Table
    val employeeTable = new HTable(hbaseConfig,table)
    var empRecord1 = new Put(new String("1").getBytes())
    empRecord1.add("personal data".getBytes(), "fName".getBytes(),new String("Subbarao").getBytes())
    empRecord1.add("personal data".getBytes(), "lName".getBytes(),new String("Bandi").getBytes())
    empRecord1.add("personal data".getBytes(), "city".getBytes(),new String("Ongole").getBytes())
    empRecord1.add("personal data".getBytes(), "state".getBytes(),new String("AP").getBytes())
    empRecord1.add("personal data".getBytes(), "country".getBytes(),new String("INDIA").getBytes())
    empRecord1.add("professional data".getBytes(), "dept".getBytes(),new String("IT").getBytes())
    empRecord1.add("professional data".getBytes(), "designation".getBytes(),new String("Hadoop developer").getBytes())
    empRecord1.add("professional data".getBytes(), "Salary".getBytes(),new String("100000").getBytes())
    empRecord1.add("professional data".getBytes(), "Company".getBytes(),new String("LexixNexis").getBytes())
    employeeTable.put(empRecord1)
    
    var empRecord2 = new Put(new String("2").getBytes())
    empRecord2.add("personal data".getBytes(), "fName".getBytes(),new String("Neelima").getBytes())
    empRecord2.add("personal data".getBytes(), "lName".getBytes(),new String("Mukkala").getBytes())
    empRecord2.add("personal data".getBytes(), "city".getBytes(),new String("Guntur").getBytes())
    empRecord2.add("personal data".getBytes(), "state".getBytes(),new String("AP").getBytes())
    empRecord2.add("personal data".getBytes(), "country".getBytes(),new String("INDIA").getBytes())
    empRecord2.add("professional data".getBytes(), "dept".getBytes(),new String("IT").getBytes())
    empRecord2.add("professional data".getBytes(), "designation".getBytes(),new String("SQL developer").getBytes()
    empRecord2.add("professional data".getBytes(), "salary".getBytes(),new String("130000").getBytes())
    empRecord2.add("professional data".getBytes(), "Company".getBytes(),new String("CTS").getBytes())
    employeeTable.put(empRecord2)
   
    employeeTable.flushCommits();
    
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConfig,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    
    //create RDD of result
    val resultRDD = hbaseRDD.map(x=>x._2)
    
    //read individual column information from RDD    
     val employeeRDD = resultRDD.map(result => ((Bytes.toString(result.getValue(Bytes.toBytes("personal data"), Bytes.toBytes("fname")))),
      (Bytes.toString(result.getValue(Bytes.toBytes("personal data"), Bytes.toBytes("lname")))),
      (Bytes.toString(result.getValue(Bytes.toBytes("personal data"), Bytes.toBytes("city")))),
      (Bytes.toString(result.getValue(Bytes.toBytes("personal data"), Bytes.toBytes("state")))),
      Bytes.toString(result.getValue(Bytes.toBytes("personal data"), Bytes.toBytes("country")))))
      
      //Filter record from rdd 
      val result = employeeRDD.filter(result=>result._8.toInteger<100000)
      
      //Save output to Hadoop
      result.saveAsTextFile("hdfs://localhost:9000/user/hbase/output")
      sc.stop()
    
  }
}