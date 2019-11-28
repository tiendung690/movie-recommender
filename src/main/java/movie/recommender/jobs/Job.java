package movie.recommender.jobs;

public interface Job {
    String DEFAULT_COMMAND = "streaming";

    void execute();
    String getName();
}