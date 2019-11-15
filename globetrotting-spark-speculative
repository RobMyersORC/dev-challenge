// master import variable

val input = "France|2000"

val speculation_factor = 10 // 10 is good balance, lower is less speculative but will take longer


// starting variables

    // starting city
    val starting_country =  input.split('|')(0)

    // distance we can travel
    val max_distance_km = input.split('|')(1).toInt
    val max_distance = max_distance_km * 1000

    // performance tuning
    val number_cores = 110


// additional library imports
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions.broadcast

// load JSON country data
sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", REMOVED )
sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", REMOVED)


val path = "s3n://orc.testdata/countries_json_rob.js"
val countries_json = {spark.read
                        .option("multiLine",true)
                        .option("mode", "PERMISSIVE")
                        .json(path)}
                        
                        
// filter to just countries with capitals in the dataset
val countries_with_capital = countries_json.filter("capitalCity <> ''")


// define UDF for distance calc
// n.b input is in degrees and output is in meters
import org.apache.spark.sql.functions.udf
def udfDistance=udf(
    (lat1: Double, lon1: Double, lat2: Double, lon2: Double)  => {

        var φ1 = lat1.toRadians
        var φ2 = lat2.toRadians
        var Δλ = (lon2-lon1).toRadians
        var R = 6371e3; // gives d in metres
        var d = Math.acos( Math.sin(φ1)*Math.sin(φ2) + Math.cos(φ1)*Math.cos(φ2) * Math.cos(Δλ) ) * R;

        d
        }
    )

// create dataset of all possible journeys with distance
import org.apache.spark.sql.expressions.Window

val all_journeys = {countries_with_capital
                    .as("a")
                    .crossJoin(broadcast(countries_with_capital).as("b"))
                    .filter("a.name<>b.name")
                    .withColumn("distance",udfDistance(col("a.latitude"),col("a.longitude"),col("b.latitude"),col("b.longitude")))
                    .filter(col("distance")<= max_distance)
                    .withColumn("route_taken",array($"a.capitalCity",$"b.capitalCity"))
                    .select("a.capitalCity","a.name","a.latitude","a.longitude","b.capitalCity","b.latitude","b.longitude","distance")
                    .withColumn("inv_distance",lit(1)/ pow(col("distance"),2))
                    .withColumn("clumpiness",sum($"inv_distance").over(Window.partitionBy("b.capitalCity")))
                    .cache
                    }
                    
                    
// create intial dataset of all possible trips within max range
val one_journey = {all_journeys
                    .filter($"a.name"=== starting_country)
                    .withColumn("clumpiness_quartile",ntile(speculation_factor).over(Window.partitionBy("a.capitalCity").orderBy("clumpiness")))
                    .withColumn("distance_quartile",ntile(speculation_factor).over(Window.partitionBy("a.capitalCity").orderBy("inv_distance")))
                    .withColumn("score",$"clumpiness_quartile" + $"distance_quartile")
                    .withColumn("score_quartile",ntile(speculation_factor).over(Window.partitionBy("a.capitalCity").orderBy("score")))
                    .withColumn("route_taken",array($"a.capitalCity",$"b.capitalCity"))
                    .withColumn("total_distance",$"distance")
                    .filter("score_quartile >= " + speculation_factor)
                    .select("route_taken","b.capitalCity","b.latitude","b.longitude","total_distance")
                    .cache
                    }
                    


if( one_journey.count == 0 ){
        // catch for if not even enough distance for one trip
        no_more_journeys = true
      }
                    
// variable to control loop
var no_more_journeys = false
var number_journeys =  1

// creating DS here outside loop to stop fun and games with circular references
var last_journey = one_journey

// first timestamp
 val currentTime=new SimpleDateFormat("HH:mm:ss").format(System.currentTimeMillis())
 println("journey: " + number_journeys + "    solutions: " + one_journey.count + "    time: " + currentTime)



// ideally wouldnt be a while loop...
while (no_more_journeys == false)
{
   
   val next_journey = {last_journey
                    .as("x")
                    .join(all_journeys, col("x.capitalCity") === col("a.capitalCity"),"inner")
                    .filter(!array_contains (col("x.route_taken"), $"b.capitalCity")) // remove any joins where city has already been visited
                    .withColumn("inv_distance",lit(1)/ pow(col("distance"),2))
                    .withColumn("clumpiness_quartile",ntile(speculation_factor).over(Window.partitionBy("a.capitalCity").orderBy("clumpiness")))
                    .withColumn("distance_quartile",ntile(speculation_factor).over(Window.partitionBy("a.capitalCity").orderBy("inv_distance")))
                    .withColumn("score",$"clumpiness_quartile" + $"distance_quartile")
                    .withColumn("score_quartile",ntile(speculation_factor).over(Window.partitionBy("a.capitalCity").orderBy("score")))
                    .filter("score_quartile >= " + speculation_factor)
                    .withColumn("new_total_distance",$"total_distance" + $"distance")
                    .filter(col("new_total_distance")<= max_distance) // drop if over limit
                    .withColumn("new_route_taken",array_union($"x.route_taken",array(lit($"b.capitalCity")))) // add to array of destinations visited
                    .select("new_route_taken","b.capitalCity","b.latitude","b.longitude","new_total_distance")
                    .withColumnRenamed("new_route_taken","route_taken")
                    .withColumnRenamed("new_total_distance","total_distance")
                    .cache
       
   }
   
   
   if( next_journey.count == 0 ){
        // break if join produces no new valid journeys
        no_more_journeys = true

      }
      else
      {
          last_journey = next_journey 
          number_journeys = number_journeys + 1
          val currentTime=new SimpleDateFormat("HH:mm:ss").format(System.currentTimeMillis())
          println("journey: " + number_journeys + "    solutions: " + next_journey.count + "    time: " + currentTime)
      }
      
}

                    

println("total number of solutions: " + last_journey.count)
println("number of cities: " + (number_journeys+1).toString)

last_journey.orderBy(desc("total_distance")).select("route_taken").show(1,false)
  
