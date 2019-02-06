import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object HadoopTry {

  case class Email(id: String, date: String, from: String, to: String, message: String)

  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("GetEnronData")
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext

  def readEmail(fileStatus: FileStatus) : Email = {

    val messageIdRegEx = ".*Message-ID:.*<(.*)>.*".r
    val dateRegEx = ".*Date:\\s*(.*)\\s*".r
    val toRegEx = ".*To:\\s*(.*)\\s*".r
    val fromRegEx = ".*From:\\s*(.*)\\s*".r

    val emailFile = spark.read.textFile(fileStatus.getPath.toString)
    val id = emailFile.filter(line => line.contains("Message-ID:")).first match { case messageIdRegEx(temp) => temp }
    val datel = emailFile.filter(line => line.contains("Date:")).first match { case dateRegEx(temp) => temp }
    val from = emailFile.filter(line => line.contains("From:")).first match { case fromRegEx(temp) => temp }
    val to = emailFile.filter(line => line.contains("To:")).first match { case toRegEx(temp) => temp }
    val entireMessage = emailFile.reduce((line1, line2) => line1 + "\n" + line2)

    Email(id, datel,from,to,entireMessage)
  }

  def main(args: Array[String]) {

    val path = "hdfs:////user/hadoop/enron/maildir"

    val people = FileSystem.get(sc.hadoopConfiguration).listStatus(new Path(path)).filter(f => f.isDirectory)
    people.foreach(f => println(f.getPath.toString))

    val peopleDirs = people.flatMap(p => FileSystem.get(sc.hadoopConfiguration).listStatus(p.getPath)).filter(f => f.isDirectory)
    peopleDirs.foreach(f => println(f.getPath.toString))

    val emailFiles = peopleDirs.flatMap(p => FileSystem.get(sc.hadoopConfiguration).listStatus(p.getPath)).filter(f => f.isFile)
    emailFiles.foreach(f => println(f.getPath.toString))

    val emails = emailFiles.map(readEmail)
    val emailsRdd = sc.parallelize(emails)
    emailsRdd.saveAsTextFile("hdfs:///tmp/ReadingOneEmail")
  }

}