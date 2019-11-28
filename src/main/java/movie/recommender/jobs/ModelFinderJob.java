package movie.recommender.jobs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.Rating;

import com.google.inject.Inject;
import com.movierecommender.spark.als.ModelFinder;

import movie.recommender.io.CassandraIo;
import movie.recommender.model.RawRating;

public class ModelFinderJob implements Job {
    private ModelFinder modelFinder;
    private CassandraIo<RawRating> ratingCassandraIo;

    @Inject
    public ModelFinderJob(ModelFinder modelFinder, CassandraIo<RawRating> ratingCassandraIo) {
        this.modelFinder = modelFinder;
        this.ratingCassandraIo = ratingCassandraIo;
    }

    @Override
    public void execute() {
        JavaRDD<Rating> ratingJavaRDD = ratingCassandraIo.readInput()
                .map(rawRating -> rawRating.toSparkRating());
        System.out.println("find best model for ratings " + ratingJavaRDD.count());
        modelFinder.findBestModel(ratingJavaRDD);
    }

    @Override
    public String getName() {
        return "model.find";
    }
}