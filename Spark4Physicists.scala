/*Analysing billion-objects catalogue interactively:Apache Sparkfor physicists 
=====================================================================================*/
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.Column
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
  
 //Find the elapsed time (minute)
 def elapsedTime( begin:Double,  end:Double):Unit={
    println("Elapsed time (Minute): "+ (end-begin)/1000000000)
    
 }
  
 // Reading the data 
 var start = System.nanoTime()
 var gal=spark.read.format("fits").option("hdu",1).load("file:///home/alia/a.fits")
 //building the gal dataframe, selecting  the "RA" and "dec" columns and building a new redshift column called "z" on the flight.
 gal=gal.withColumn("Z" ,$"DZ_RSD"+$"Z_COSMO").select("RA","DEC","Z")
 var finish = System.nanoTime()
 //Print the elapsed time for reading fits file, selecting  the "RA" and "dec" columns and building a new redshift column called "z"
  elapsedTime(start,finish) 
 
 //print the gal dataframe schema 
  gal.printSchema

 
 
 //To  print 5 samples
 start = System.nanoTime()
 gal.show(5)
 finish = System.nanoTime()
 elapsedTime(start,finish) 
 
 
 //Adding an extra column that performs gaussian smearing on the "z" column. We use the Spark "rand" function that is highly optimized
 start = System.nanoTime()
 gal=gal.withColumn("zrec", (col("z")+1)*0.03*randn()+col("Z") )
 gal.show(5)
 finish = System.nanoTime()
 elapsedTime(start,finish) 
 
 //Put data in cache
start = System.nanoTime()
println("Number Of Galaxies:"+gal.persist(StorageLevel.MEMORY_AND_DISK).count())
finish = System.nanoTime()
elapsedTime(start,finish) 

/**
 * Basic statistics 
 */

//some simple statistics on a single column  gal.describe( "Z" , "zrec").show()
start = System.nanoTime()
gal.describe( "Z").show()
finish = System.nanoTime()
elapsedTime(start,finish) 

//some simple statistics on all the columns:
start = System.nanoTime()
gal.describe( ).show()
finish = System.nanoTime()
elapsedTime(start,finish) 

// Minimum and Maximum  spark functions
 start = System.nanoTime()
 val minMax= gal.select(min("z"),max("z") ).first() 
 val zMax  =minMax(1).asInstanceOf[Float]
 val zMin   =minMax(0).asInstanceOf[Float]
 println("Minimum Value:"+ zMin+ "\t"+ "Maximum Value:"+ zMax)
 finish = System.nanoTime()
 elapsedTime(start,finish) 

//Adding the zbin number column (labelled “bin”) 
 start = System.nanoTime()
 val Nbins=100
 var dz=(zMax-zMin)/Nbins
 val zBin=gal.select("Z").withColumn("bin", ((col("Z")-zMin-dz/2)/dz).cast(IntegerType)  ) 

//Grouping by the bin column, counting its membersand sorting in ascending order
var h=zBin.groupBy("bin").count.orderBy("bin")
//add the bin locations  and drop the bin numer
h=h.withColumn("loc", col("bin")*dz+zMin+dz/2 ).drop("bin")   
h=h.select("loc","count")
h.show()
finish = System.nanoTime()
elapsedTime(start,finish) 
