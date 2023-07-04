from pyspark.sql.functions import avg, max, min, from_unixtime
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

def process_data(spark):
    # read the movies and ratings data
    movies_original = spark.read.format("csv").option("delimiter", "::").load("ml-1m/movies.dat")
    ratings_original = spark.read.format("csv").option("delimiter", "::").load("ml-1m/ratings.dat")

    # rename columns and store in new df
    movies_new = movies_original.withColumnRenamed("_c0", "movie_id").withColumnRenamed("_c1", "title").withColumnRenamed("_c2", "genre")
    ratings_new = ratings_original.withColumnRenamed("_c0", "user_id").withColumnRenamed("_c1", "movie_id").withColumnRenamed("_c2", "rating").withColumnRenamed("_c3", "timestamp")
    
    # Convert the timestamp column to a human-readable format (e.g. 3 July 2023 00:00:00)
    ratings_new = ratings_new.withColumn('rated_on', from_unixtime('timestamp', "d MMM yyyy HH:mm:ss"))
    # Drop timestamp column as it is not required anymore
    ratings_new = ratings_new.drop('timestamp')

    # new df containing movies data with 3 additional columns max, min, avg for each movie from ratings
    movies_ratings = ratings_new.groupBy("movie_id").agg(avg("rating").alias("avg_rating"), max("rating").alias("max_rating"), min("rating").alias("min_rating"))
    movies_ratings = movies_new.join(movies_ratings, "movie_id", "inner")

    # using window function to filter and partition data per user.
    # First I am ordering the partitioned data by rating.
    # As many movies can have same rating, I am then ordering by rated_on date.
    # There were few cases where the rated_on date and time was same for same user and same rating. (Ideally this should not happen)
    # So as a final step I am ordering by movie_id to get the same result as expected in the assignment.
    window = Window.partitionBy(ratings_new['user_id']).orderBy(ratings_new['rating'].desc(), ratings_new['rated_on'].desc(), ratings_new['movie_id'])

    # Assigning a new df to run the window function on ratings_new df
    user_top_movies = ratings_new.select('*', rank().over(window).alias('rank'))

    # Joining the user_top_movies df with movies_new df to get the movie title and genre to get better understanding of the data
    user_top_movies = user_top_movies.join(movies_new, "movie_id", "inner")
    # Filtering the data to get only top 3 movies per user
    user_top_movies = user_top_movies.filter(user_top_movies['rank'] <= 3)
    # Ordering the data by user_id, rank and rated_on date and time in descending order. Also dropping the rank column as it is not required anymore.
    user_top_movies = user_top_movies.orderBy('user_id', 'rank', ratings_new['rated_on'].desc()).drop('rank')
    # Dropping the movie_id column as it is not required anymore.
    user_top_movies = user_top_movies.drop('movie_id')

    # Write out the original and new dataframes in parquet format
    # I feel parquet format is better than csv as it is columnar and compressed.
    # Also, it is easy to read and write data in parquet format.
    movies_original.write.parquet("movies.parquet", mode="overwrite")
    ratings_original.write.parquet("ratings.parquet", mode="overwrite")
    movies_ratings.write.parquet("movies_ratings.parquet", mode="overwrite")
    user_top_movies.write.parquet("user_top_movies.parquet", mode="overwrite")

    # show head of each df
    movies_original.show(5)
    ratings_original.show(5)
    movies_ratings.show(10)
    user_top_movies.show(10)
