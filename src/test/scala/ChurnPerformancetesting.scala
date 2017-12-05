import com.holdenkarau.spark.testing.{DataframeGenerator, SharedSparkContext}
import example.Churn
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers


class ChurnPerformancetesting extends FunSuite with SharedSparkContext with Checkers{

  val schema = Churn.schema
  val spark: SparkSession = SparkSession.builder().master("local").appName("churn").getOrCreate()
  val sQLContext = spark.sqlContext
  val dataframegen = DataframeGenerator.arbitraryDataFrame(sQLContext,schema)
  val tsvWithHeaderOptions: Map[String, String] = Map(
    ("delimiter", ","),
    ("header", "false"))
  var metrics:Double = 0.0

  test("Churning model performance benchmarking"){
    val property = forAll(dataframegen.arbitrary){ dataframe => {

        val Array(train, test) = dataframe.randomSplit(Array(0.8, 0.2))
        val trainingFile = train.coalesce(1).write.mode(SaveMode.Overwrite ).options(tsvWithHeaderOptions )
          .csv("/home/abhay/Downloads/SquareOne/Spark-unit-testing/src/test/resources/datafiles/Utesting-training.csv")

        val testingFile = test.coalesce(1).write.mode(SaveMode.Overwrite ).options(tsvWithHeaderOptions )
          .csv("/home/abhay/Downloads/SquareOne/Spark-unit-testing/src/test/resources/datafiles/Utesting-testing.csv")

        val trainingFilePath = getClass.getResource("/Utesting-training.csv").getPath
        val testingFilePath = getClass.getResource("/Utesting-testing.csv").getPath
        metrics = Churn.churnModel(trainingFilePath,testingFilePath)
      }
      metrics > 0.65
    }
    println(property.getClass)
    println(property.toString().size)
    property.check
  }
}
