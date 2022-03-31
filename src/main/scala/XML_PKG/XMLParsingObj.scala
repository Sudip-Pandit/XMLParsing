package XML_PKG
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.io.Source._
object XMLParsingObj {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ComplexDataProcessing").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder()
      .config("fs.s3a.access.key", "")
      .config("fs.s3a.secret.key", "")
      .getOrCreate()
    spark.conf.set("spark.sql.repl.eagerEval.enabled", "true")

    val df = spark.read.format("com.databricks.spark.xml")
      .option("rowTag","POSLog")
      .load("file:///D:/data1/transactions.xml")

    df.show()
    df.printSchema()

    val flatdf = df.withColumn("Transaction", expr("explode(Transaction)"))
      .withColumn("LineItem", expr("explode(Transaction.RetailTransaction.LineItem)"))
      .withColumn("Total", expr("explode(Transaction.RetailTransaction.Total)"))
    flatdf.show()
    flatdf.printSchema()

    val flattendf = flatdf.select(
      col("Transaction.BusinessDayDate"),
      col("Transaction.ControlTransaction.OperatorSignOff.*"),
      col("Transaction.ControlTransaction.ReasonCode"),
      col("Transaction.ControlTransaction._Version"),
      col("Transaction.CurrencyCode"),
      col("Transaction.EndDateTime"),
      col("Transaction.OperatorID.*"),
      col("Transaction.RetailStoreID"),
      col("Transaction.RetailTransaction.ItemCount"),
      col("Transaction.RetailTransaction.PerformanceMetrics.*"),
      col("Transaction.RetailTransaction.ReceiptDateTime"),
      col("Transaction.RetailTransaction.TransactionCount"),
      col("Transaction.RetailTransaction._Version"),
      col("Transaction.SequenceNumber"),
      col("Transaction.WorkstationID"),
      col("LineItem.Sale.Description"),
      col("LineItem.Sale.DiscountAmount"),
      col("LineItem.Sale.ExtendedAmount"),
      col("LineItem.Sale.ExtendedDiscountAmount"),
      col("LineItem.Sale.ItemID"),
      col("LineItem.Sale.Itemizers.*"),
      col("LineItem.Sale.MerchandiseHierarchy.*"),
      col("LineItem.Sale.OperatorSequence"),
      col("LineItem.Sale.POSIdentity.*"),
      col("LineItem.Sale.Quantity"),
      col("LineItem.Sale.RegularSalesUnitPrice"),
      col("LineItem.Sale.ReportCode"),
      col("LineItem.Sale._ItemType"),
      col("LineItem.Tax.*"),
      col("LineItem.Tender.Amount"),
      col("LineItem.Tender.Authorization.*"),
      col("LineItem.Tender.OperatorSequence"),
      col("LineItem.Tender.TenderID"),
      col("LineItem.Tender._TenderDescription"),
      col("LineItem.Tender._TenderType"),
      col("LineItem.Tender._TypeCode"),
      col("LineItem._EntryMethod"),
      col("LineItem._weightItem"),
      col("Total._TotalType"),
      col("Total._VALUE"),
      col("Total.*")

    )
    flattendf.show()
    flattendf.printSchema()

  }
}