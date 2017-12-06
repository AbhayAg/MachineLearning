import java.io.IOException

import com.holdenkarau.spark.testing.PerTestSparkContext
import example.Churn
import org.scalatest.FunSuite
import org.scalatest.exceptions.DiscardedEvaluationException
import org.scalatest.prop.Checkers


class ChurnPerformancetesting extends FunSuite with PerTestSparkContext with Checkers {
  val model = Churn

  try {
        test( "Churning model performance benchmarking" ) {
        val property = {
          val metrics = model.churnModel( 0.29, 0.99)
          println(metrics)
          metrics
        }
          assert(property>=0.75 && property<0.80)
      }
    }
  catch {
    case ioe:IOException => ioe.printStackTrace()
    case e:Exception => e.printStackTrace()
    case n:NullPointerException => n.printStackTrace()
    case p:DiscardedEvaluationException => p.printStackTrace()
    case m:MatchError => m.printStackTrace()
  }
}
