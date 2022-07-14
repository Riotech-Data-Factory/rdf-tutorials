package udfs

import org.apache.spark.sql.api.java.UDF1

class ChangeStringToLongUDF extends UDF1[String, Option[Long]]{
  override def call(id: String): Option[Long] = {
    val idWithoutRubbish: String = id.replace("[", "")
      .replace("\"", "")
      .replace("]","")
    if(idWithoutRubbish.forall(c=> c.isDigit)){
      Option(idWithoutRubbish.toLong)
    }else{
      Option.empty[Long]
    }
  }
}
