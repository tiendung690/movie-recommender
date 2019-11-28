package movie.recommender.jobs;

import java.util.Set;

import org.apache.log4j.Logger;

import com.google.inject.Inject;

import movie.recommender.TimeKeeper;

public class JobExecutor {
	private Set<Job> jobs;
	private Logger logger = Logger.getLogger(JobExecutor.class);

	@Inject
	public JobExecutor(Set<Job> jobs) {
		this.jobs = jobs;
	}

	public void execute(String jobName) {
		TimeKeeper timeKeeper = new TimeKeeper();
		jobs.forEach(job -> {
			if (!job.getName().equals(jobName)) {
				return;
			}
			timeKeeper.start();
			logger.info("Executing job " + job.getClass().getName());
			job.execute();
			timeKeeper.end().print(logger, "Finish executing job" + job.getClass().getName()).reset();
		});
	}
}