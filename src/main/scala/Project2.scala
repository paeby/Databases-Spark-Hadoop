package main.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Project2 {
  def main(args: Array[String]) {
    
    //Check arguments
    if(args.length != 3) {
       println("USAGE: Project1 inputdir newoutputdir")
       System.exit(1)
    }  

    val conf = new SparkConf().setAppName("Project1").setMaster("yarn-cluster")

    val sc = new SparkContext(conf)

    val customer = sc.textFile(args(0)+"/customer.tbl")
    val order = sc.textFile(args(0)+"/orders.tbl")
    
    //SQL querry in English: counts the number of customers (custdist) which have the same number of orders (o_count) which are not of the special format 
    
    // from ORDER we need: O_CUSTKEY and O_COMMENT
    val orders = order.map(line => line.split("\\|")).map(a => (a(1).toInt, a(8)))
    
    // we just want the orders without the special comment '%special%requests%' and we add the value 1 instead of the comment
    // we also do the union with the customers to be sure to have all customers
    var filter_orders = orders.filter({case (customer, comment) => !comment.matches("(?s)special.*requests")}).map(t => (t._1,1))
    
    //we need to do the union with customers if the flag is set to 1
    if(args(2) == "1") {
        // from CUSTOMER we need: C_CUSTKEY, in fact, we need all the customer keys
        // as in the SQL querry there is a left outer join, the CUSTOMER table being the left one
        // we already add a value zero to each customer, we will use it to count the total number of comments afterwards
        // if the flag is set to 2 we don't need this part as we are not interested in the customers
        // that don't have any order
        val customers = customer.map(line => (line.split("\\|")(0).toInt, 0))
        filter_orders = filter_orders.union(customers)
    }

    // now we count the number of orders per customer, we will just keep the number of orders and 1 to execute the final count afterwards
    val orders_per_customer = filter_orders.reduceByKey(_ + _).map(t => (t._2, 1))
    
    //now we have the numbers of order per customer and we want to count how many customers have made the same number of orders
    val final_result = orders_per_customer.reduceByKey(_ + _).map(t => (t._1.toString()+"|"+t._2.toString + "|"))
    final_result.saveAsTextFile(args(1).concat("/out_" + args(2).toString()))

    sc.stop()
  }
}