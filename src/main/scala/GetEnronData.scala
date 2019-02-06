import java.io.File

import org.apache.spark.sql.SparkSession

object GetEnronData {

  case class Email(id: String, date: String, from: String, to: String, message: String)

  val spark = SparkSession.builder()
    .master("local")
    .appName("GetEnronData")
    .getOrCreate()

  def getListOfFiles(dir: File):List[File] = {
    if (dir.exists && dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getListOfSubDirectories(dir: File): List[File] = {
    if (dir.exists && dir.isDirectory) {
      dir.listFiles
        .filter(_.isDirectory)
        .toList
    } else {
      List[File]()
    }
  }

  def readEmail(file: File) : Email = {

    val messageIdRegEx = ".*Message-ID:.*<(.*)>.*".r
    val dateRegEx = ".*Date:\\s*(.*)\\s*".r
    val toRegEx = ".*To:\\s*(.*)\\s*".r
    val fromRegEx = ".*From:\\s*(.*)\\s*".r

    val emailFile = spark.read.textFile(file.getAbsolutePath)
    val id = emailFile.filter(line => line.contains("Message-ID:")).first match { case messageIdRegEx(temp) => temp }
    val datel = emailFile.filter(line => line.contains("Date:")).first match { case dateRegEx(temp) => temp }
    val from = emailFile.filter(line => line.contains("From:")).first match { case fromRegEx(temp) => temp }
    val to = emailFile.filter(line => line.contains("To:")).first match { case toRegEx(temp) => temp }
    val entireMessage = emailFile.reduce((line1, line2) => line1 + "\n" + line2)
    //emailFile.rdd.saveAsTextFile("hdfs:///tmp/debug_email_1")
    Email(id, datel,from,to,entireMessage)
  }

  def main(args: Array[String]) {

    val path = "C:\\Users\\ketan\\OneDrive\\Desktop\\big data technology\\BUILDING THE DATA PIPELINE\\week 2\\enron_2015_sample\\maildir"
    //val path = "hdfs:///user/hadoop/enron_small/maildir"
    val dir = new File(path)

    val peopleFolders = getListOfSubDirectories(dir) // names of ppl
    //peopleFolders.foreach(f => println(f.getName))

    val listEmailFolders = peopleFolders.flatMap(getListOfSubDirectories) // inbox, sent items.....
    //listEmailFolders.foreach(f => println(f.getName))

    val listEmails = listEmailFolders.flatMap(getListOfFiles) // list 1,2,3,4,,.....

    val emails = listEmails.map(readEmail) /// send each item to readEmail function.
    import spark.implicits._
    val emailsDs = emails.toDF()
    emailsDs.write.csv("C:\\Users\\ketan\\OneDrive\\Desktop\\big data technology\\BUILDING THE DATA PIPELINE\\week 2\\enron_2015_sample\\output_small_five.csv")

    //val outputPath = if (args(0) == null) "hdfs:///tmp//output_two.csv" else args(0)
   // emailsDs.write.csv(outputPath)
    //spark.sparkContext.parallelize(emails).saveAsTextFile("hdfs:///tmp/debug_emails")
  }
}
