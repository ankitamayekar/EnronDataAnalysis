package Readdata

object ReadData extends App {
  println("Hello, World!")

  val date = "(\\d{4})-(\\d{2})-(\\d{2})".r

  val dates = "Important dates in history: 2004-01-20, 1958-09-05, 2010-10-06, 2011-07-15"
  val found = date.findFirstIn(dates).getOrElse("No date found.")
  println(found)


  date.findAllIn(dates).foreach(println)

  val messageIdRegEx = "Message-ID:.*<(.*)>.*".r
  val msgIdStr = "Message-ID: <16159836.1075855377439.JavaMail.evans@thyme>"
  val foundMsgId = messageIdRegEx.findFirstIn(msgIdStr).getOrElse("No MsgId Found")
  println(foundMsgId)

  messageIdRegEx.findAllIn(msgIdStr).foreach(println)

  msgIdStr match {
    case messageIdRegEx(one) => println(one)
  }

  val fromRegEx = ".*From:\\s*(.*)\\s*".r
  " From: heather.dunton  @enron.com  " match {
    case fromRegEx(two) => println(two)
  }

}