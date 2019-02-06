import java.io.File

import org.apache.spark.sql.SparkSession

object MySample extends App {

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

  case class Email(id: String, date: String, from: String, to: String, message: String)

  val spark = SparkSession.builder()
    .master("local")
    .appName("MySample")
    .getOrCreate()

  val sc = spark.sparkContext

  val messageIdRegEx = ".*Message-ID:.*<(.*)>.*".r
  val dateRegEx = ".*Date:\\s*(.*)\\s*".r
  val toRegEx = ".*To:\\s*(.*)\\s*".r
  val fromRegEx = ".*From:\\s*(.*)\\s*".r


  def readEmail(file: File) : Email = {
    val emailFile = sc.textFile(file.getAbsolutePath())
   // val msgId = message.collect().filter(line => line.contains("Message-ID"))
   // println(x = msgId)

    //val email = new Email("this is fake id", "fake date", "abckd", "fafa", message.toString())
    //val message = emailFile.filter(line => line.contains("Message-ID"))
    val id = emailFile.filter(line => line.contains("Message-ID")).first()
    val datel = emailFile.filter(line => line.contains("Date")).first()
    val from = emailFile.filter(line => line.contains("From")).first()
    val to = emailFile.filter(line => line.contains("To")).first()
    val entireMessage = emailFile.collect()
    return new Email(id, datel,from,to,entireMessage.toString)
   // val email = new Email("message.collect().filter(line => line.contains("Message-ID"))", "fake date", "abckd", "fafa", message.toString())
    //print(email)
    //return email
  }

  var path = "C:\\Users\\ketan\\OneDrive\\Desktop\\big data technology\\BUILDING THE DATA PIPELINE\\week 2\\enron_2015_sample\\maildir_small"
  val dir = new File(path)

  val peopleFolders = getListOfSubDirectories(dir)
  peopleFolders.foreach(f => println(f.getName))

  var listEmailFolders = peopleFolders.flatMap(getListOfSubDirectories(_))
  listEmailFolders.foreach(f => println(f.getName))

  var listEmails = listEmailFolders.flatMap(getListOfFiles(_))

  val emails = listEmails.map(f => readEmail(f))

  //val emailsRdd = sc.parallelize(emails)
  import spark.implicits._
  val emailsDf = emails.toDF()
  emailsDf.write.csv("C:\\Users\\ketan\\OneDrive\\Desktop\\big data technology\\BUILDING THE DATA PIPELINE\\week 2\\enron_2015_sample\\output_small_one.csv")


/*
  val population = spark.read.option("inferSchema","true")
    .option("header", "true")
    .csv("file:///C://Users//ketan//OneDrive//Desktop//big data technology//INTRODUCTION TO DATA ENGINEERING//final project//donorDB//tables//statewise_population.csv")
    .toDF()

  population.show()
*/

  //var sampleEmailPath = "C:\\Users\\ketan\\OneDrive\\Desktop\\big data technology\\BUILDING THE DATA PIPELINE\\week 2\\enron_2015_sample\\maildir\\allen-p\\inbox\\1"
  //val email = spark.sparkContext.textFile(sampleEmailPath)

  //var lines = email.collect()
  //lines.foreach(l => println(l))

//message.collect().filter(line => line.contains("Message-ID"))

}

