import com.mongodb.spark.MongoSpark
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.bson.Document
import scala.collection.Seq
import java.util.*

data class Data(val id: String, val item: String, val pages: Int)

const val DATABASE_NAME = "norm_db"
const val COLLECTION_NAME = "papers"

fun main(args: Array<String>) {
//    val mongoClient = MongoClient("localhost", 27017)
//    val db = mongoClient.getDatabase("my_db")
//    val q = BasicDBObject().apply {
//        put("item", "journal")
//    }
//    val cols = db.getCollection("insert").find().forEach {
//        println()
//    }

    val sparkSession = SparkSession.builder()
        .master("local")
        .appName("gay")
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/$DATABASE_NAME.$COLLECTION_NAME")
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/$DATABASE_NAME.$COLLECTION_NAME")
        .orCreate

    val javaSparkContext = JavaSparkContext(sparkSession.sparkContext())
    val res = MongoSpark.load(javaSparkContext)
    val filtered = res.withPipeline(listOf(Document.parse("{ \$sort : { pages: -1 } }")))
    filtered.foreach {
        println(it)
    }
//    val df = res.toDF(Data::class.java)

    javaSparkContext.close()
    println()
}