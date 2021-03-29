package SparkCourseWork;

import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import com.sun.xml.bind.v2.schemagen.xmlschema.List;

//import code for the tuplets
import org.apache.commons.lang3.tuple.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import static org.apache.spark.sql.functions.avg;
import org.apache.spark.sql.Encoders;

public class csc20242018coursework {

	// A path to the resources folder
	private static String PATH = "./resources/"; //$NON-NLS-1$

	static SparkConf conf = new SparkConf().setMaster("local").setAppName("My App"); //$NON-NLS-1$ //$NON-NLS-2$
	static JavaSparkContext sc = new JavaSparkContext(conf);
	static SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example") //$NON-NLS-1$
			.config("spark.some.config.option", "some-value").getOrCreate(); //$NON-NLS-1$ //$NON-NLS-2$
	
	
	private static String pathToMoviesCSZ = PATH + "movies.csv"; 
	
	private static String pathToRatingCSC = PATH + "ratings.csv"; 
	
	private static String pathToGenresCSV = PATH + "moviesGenres.csv"; 

	public static void main(String[] args) {

		// Removes a bunch of the error text
		spark.sparkContext().setLogLevel("ERROR");

		task1();

		task2();

		task3();

		task4();

		task5();

		task6();

		task7();

	}

	/*
	 * Task 1 Load files MOVIES and RATINGS into its own DataFrame (DF) using the
	 * same names as the original filenames. To show your work: Print the schema for
	 * each dataframe. The dataframes should have the following structure:
	 */
	private static void task1() {

		System.out.println("--- TASK 1 ---");

		// calls a methoded used to get the ratings
		Dataset<Row> ratings = getRatings();

		// calls a methoded used to get the movies
		Dataset<Row> movies = getMovies();

		// Task 1 - Ratings printSchema
		System.out.println(" - Ratings Schema - ");
		ratings.printSchema();

		// Task 1 - Movies printSchema
		System.out.println(" - Movies Schema - ");
		movies.printSchema();

	}

	// task 1
	// creating movies data frame
	private static Dataset<Row> getMovies() {
		Dataset<Row> createMovies = spark.read().option("inferSchema", true).option("inferSchema", true)
				.option("header", true).option("multLine", true).option("mode", "DROPMALFORMED")
				.option("timeStampFormat", "yyyy/MM/dd HH:mm:ss ZZ").csv(pathToMoviesCSZ);
		return createMovies;

	}

	// task 1
	// creating ratings data frame
	private static Dataset<Row> getRatings() {
		// Task 1 - Ratings data frame
		Dataset<Row> createRatings = spark.read().option("inferSchema", true).option("header", true)
				.option("multLine", true).option("mode", "DROPMALFORMED")
				.option("timeStampFormat", "yyyy/MM/dd HH:mm:ss ZZ").csv(pathToRatingCSC);

		return createRatings;
	}

	/*
	 * Task 2 From the Movies DataFrame: for each Row extract its genres from the
	 * genres column, split the genres into a list [genre_1, genre_2,...], and save
	 * all pairs <movieId, genre_1>, <movieId, genre_2>... into a CSV file called
	 * movieGenres.csv. Note that one Movie will have more than one record, one for
	 * each Genre associated with the Movie.
	 */
	private static void task2() {

		System.out.println("--- TASK 2 ---");

		// set the print writer to null
		PrintWriter printWriterGeners = null;

		// try creating the printwriter or throw an error
		try {
			printWriterGeners = new PrintWriter(new File(pathToGenresCSV));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Create a new data set from movies using its movieId and genres coloum
		Dataset<Row> movies = getMovies().select(col("movieId"), col("genres"));

		// create a new string builder
		StringBuilder stringBuilderGeners = new StringBuilder();

		appendGenres(printWriterGeners, stringBuilderGeners, "movieId", "genres");

		// for every row in movies
		for (Row R : movies.collectAsList()) {
			// get split up all the generes into separate rows
			for (String s : R.get(1).toString().split("\\|")) {
				// add the generes with the movie Id to the csv file
				appendGenres(printWriterGeners, stringBuilderGeners, R.get(0).toString(), s);
			}

		}

		printWriterGeners.close();

		System.out.println("Task 2 completed" + "\n");
	}

	// Task 2
	// A method used to append strings to my string builder.
	public static void appendGenres(PrintWriter printWriterGeners, StringBuilder stringBuilderForMethod, String wordOne,
			String wordTwo) {

		// code for building the string
		stringBuilderForMethod.setLength(0);
		stringBuilderForMethod.append(wordOne);
		stringBuilderForMethod.append(',');
		stringBuilderForMethod.append(wordTwo);
		stringBuilderForMethod.append('\n');
		// code for writing it to the print writer
		printWriterGeners.write(stringBuilderForMethod.toString());

	}

	/*
	 * Task 3 Load the movieGenres.csv file into a new DataFrame called movieGenres.
	 * Show that it has the expected schema: movieGenres(movieId, genre)
	 */
	private static void task3() {

		System.out.println("--- TASK 3 ---");
		// get the movie Genres data frame
		Dataset<Row> movieGenres = getMovieGenres();

		// show the print schema
		movieGenres.printSchema();

		// sort the data via movie Id descending
		movieGenres.sort(desc("movieId")).show(50);

	}

	// task 3
	// creating movie genres data frame
	private static Dataset<Row> getMovieGenres() {
		// Task 3 - Ratings data frame
		Dataset<Row> createMovieGenres = spark.read().option("inferSchema", true).option("header", true)
				.option("multLine", true).option("mode", "DROPMALFORMED")
				.option("timeStampFormat", "yyyy/MM/dd HH:mm:ss ZZ").csv(pathToGenresCSV);
		return createMovieGenres;
	}

	/*
	 * Task 4 Using the movieGenres DF you produced in (3), create a new DF:
	 * genrePopularity(genre, moviesCount) containing all genres along with the
	 * numbers of movies that are associated to that genre (popularity),
	 */
	private static void task4() {

		System.out.println("--- TASK 4 ---");

		// get the movie genres but count every genre that appears, call that ne column
		// moviesCount
		Dataset<Row> movieRating = getMovieGenres().groupBy(("genres")).count().withColumnRenamed("count",
				"moviesCount");
		// movieRating.printSchema();
		// Order
		movieRating.orderBy(col("moviesCount").desc()).show(10);

	}

	/*
	 * Task 5 Consider the top 10 genres identified in (4). For each genre G in this
	 * list, find the user U who has rated the highest number of movies that have
	 * genre G.
	 */

	private static void task5() {

		System.out.println("--- TASK 5 ---" + "\n");

		// Sort the movie genres by via rating like task 4
		Dataset<Row> movieRatingDesc = getMovieGenres().groupBy(("genres")).count()
				.withColumnRenamed("count", "moviesCount").orderBy(col("moviesCount").desc());

		// Merge movieRatingDesc with movie genres and movie ratings.
		// Count the number of ratings 
		// order by the number of ratings and get the genres and movie columns.
		//get the first user for each genre
		// order by movieCount descending and drop nOfR and movie count
		Dataset<Row> task5 = movieRatingDesc.join(getMovieGenres(), "genres").join(getRatings(), "movieId")
				.groupBy(col("genres"), col("userId"), col("moviesCount")).count().withColumnRenamed("count", "nOfR")
				.orderBy((col("nOfR").desc())).groupBy(col("genres"), col("moviesCount"))
				.agg(first("userId").alias("userId"), first("nOfR").alias("nOfR")).orderBy(col("moviesCount").desc())
				.drop("nOfR").drop("moviesCount");

		task5 = task5.limit(10);

		//create a tuplet pair 
		ArrayList<Pair<String, Integer>> task5Tuple = new ArrayList<Pair<String, Integer>>();

		//for every row in the task5 data frame
		for (Row R : task5.collectAsList()) {
			//add the information to the task 5 tuplet
			task5Tuple.add(new ImmutablePair<String, Integer>(R.getString(0), R.getInt(1)));
		}

		//Go through the task5Tuple and print every value to a seperste line
		for (Pair<String, Integer> task5TupleToLine : task5Tuple) {

			System.out.println(task5TupleToLine);
		}

		System.out.println("\n");
	}

	/*
	 * Task 6 For each userId in the ratings DataFrame, compute the number of movies
	 * that have been rated by that user (ratingsCount).
	 */
	private static void task6() {

		System.out.println("--- TASK 6 ---" + "\n");

		// Get the Ratings data frame
		Dataset<Row> t6Rating = getRatings();

		// Get another copy of the Ratings data frame
		Dataset<Row> t6Rating2 = getRatings();

		// Get geners data frame
		Dataset<Row> t6Geners = getMovieGenres();

		// Join the geners and the rating data frames via movieId
		Dataset<Row> joinedDF = t6Geners.join(t6Rating2, t6Geners.col("movieId").equalTo(t6Rating2.col("movieId")));

		// Count the amount of times a userId appears, call that column ratings Count.
		t6Rating = t6Rating.groupBy(col("userId")).count().withColumnRenamed("count", "ratingCount");

		// Count the number of times a genres appear
		joinedDF = joinedDF.groupBy("userId", "genres").count().withColumnRenamed("count", "genersCount");

		// get the first genre for each userID, this should be the value with the
		// highest genre count.
		joinedDF = joinedDF.orderBy(col("genersCount").desc()).groupBy("userId")
				.agg(first("genres").alias("mostCommonGenre"), first("genersCount").alias("genersCount"));

		// Merge the tables so that it will be every userID will
		Dataset<Row> joinedSorted = joinedDF.join(t6Rating, "userId");

		// remove geners Count as its not needed
		joinedSorted = joinedSorted.drop(col("genersCount"));

		// oder by ratings count descending
		joinedSorted = joinedSorted.orderBy(col("ratingCount").desc()).limit(10);

		// Creat a tuplet triple Array
		ArrayList<Triple<Integer, Long,  String>> task6Tuple = new ArrayList<Triple<Integer, Long, String>>();

		// for every row in the new dataframe
		for (Row R : joinedSorted.collectAsList()) {
			// add it to the tuplet
			task6Tuple.add(new ImmutableTriple<Integer, Long, String>(R.getInt(0), R.getLong(2), R.getString(1)));

		}

		// print out every value in tuplet triple Array
		for (Triple<Integer, Long, String> task6TupleToLine : task6Tuple) {

			System.out.println(task6TupleToLine);
		}

		System.out.println("\n");

	}

	/*
	 * Task 7 For each movie, compute its average rating and the variance. Report a
	 * list with the top 10 movies by average rating, along with their average
	 * rating and the variance.
	 */
	private static void task7() {

		System.out.println("--- TASK 7 ---");

		// get the ratings data frame
		Dataset<Row> t7Rating = getRatings();
		// get the movies data frame
		Dataset<Row> t7movies = getMovies();

		// join the two toghter
		Dataset<Row> joinedSorted = t7Rating.join(t7movies, "movieId");

		// group by the movieId and get the average rating for each movie, call this
		// column averageRating
		// Group by the movieId and the average Ratings and get the variance of the
		// average ratings
		joinedSorted = joinedSorted.groupBy("movieId").avg("rating").withColumnRenamed("avg(rating)", "averageRating")
				.groupBy("movieId", "averageRating").agg(variance("averageRating").as("variance"));

		// order by the average rating descending.
		joinedSorted.orderBy(col("averageRating").desc()).show(10);

	}

}
