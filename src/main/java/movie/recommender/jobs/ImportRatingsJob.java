package movie.recommender.jobs;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.Rating;

import com.google.inject.Inject;

import movie.recommender.io.CassandraIo;
import movie.recommender.io.RatingsFileIo;
import movie.recommender.model.RawRating;

public class ImportRatingsJob implements Job {
	private JavaSparkContext sparkContext;
	private Logger logger = Logger.getLogger(ImportRatingsJob.class);
	private CassandraIo<RawRating> ratingCassandraIo;

	@Inject
	public ImportRatingsJob(JavaSparkContext sparkContext, CassandraIo<RawRating> ratingCassandraIo) {
		this.sparkContext = sparkContext;
		this.ratingCassandraIo = ratingCassandraIo;
	}

	@Override
	public void execute() {
		RatingsFileIo ratingsIo = new RatingsFileIo();
		ratingsIo.setSparkContext(sparkContext);
		JavaRDD<Rating> ratings = ratingsIo.readInput();
		JavaRDD<RawRating> rawRatingRdd = ratings.map(rating -> RawRating.fromSparkRating(rating));
		saveToCassandra(rawRatingRdd);
	}

	private void saveToCassandra(JavaRDD<RawRating> rdd) {
		logger.info("Start saving data to cassandra");
		ratingCassandraIo.writeOutput(rdd);
		logger.info("Done saving to cassandra");
	}

	@Override
	public String getName() {
		return "ratings.import";
	}
}