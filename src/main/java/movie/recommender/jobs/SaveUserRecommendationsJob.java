package movie.recommender.jobs;

import com.google.inject.Inject;
import com.movierecommender.spark.als.TrainConfig;
import com.movierecommender.spark.als.TrainedModel;

import movie.recommender.io.CassandraIo;
import movie.recommender.model.RawRating;
import movie.recommender.model.UserRecommendations;
import movie.recommender.spark.RecommendationEngine;

public class SaveUserRecommendationsJob implements Job {
    private RecommendationEngine recommendationEngine;
    private CassandraIo<RawRating> ratingCassandraIo;

    @Inject
    public SaveUserRecommendationsJob(RecommendationEngine recommendationEngine, CassandraIo<RawRating> ratingCassandraIo) {
        this.recommendationEngine = recommendationEngine;
        this.ratingCassandraIo = ratingCassandraIo;
    }

    @Override
    public void execute() {
        TrainConfig trainConfig = new TrainConfig(10, 4);
        TrainedModel model = recommendationEngine.train(trainConfig, ratingCassandraIo);
        CassandraIo<UserRecommendations> recommendationsIo = new CassandraIo<>(UserRecommendations.class, "dev", "user_recommendations");
        recommendationEngine.saveUserRecommendations(model, recommendationsIo);
    }

    @Override
    public String getName() {
        return "model.trainAndSave";
    }
}