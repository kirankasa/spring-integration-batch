package com.dbs.cloudtask;

import java.io.File;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.integration.annotation.Transformer;
@Component
public class FileMessageToJobRequestTransformer {

	private Job job;

	private String fileParameterName;

	public FileMessageToJobRequestTransformer(Job job, @Value("${file.parameter.name}") String fileParameterName) {
		this.job = job;
		this.fileParameterName = fileParameterName;
	}
	

	@Transformer
	public JobLaunchRequest toRequest(Message<File> message) {
		JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
		jobParametersBuilder.addString(fileParameterName, "file:"+message.getPayload().getAbsolutePath());
		return new JobLaunchRequest(job, jobParametersBuilder.toJobParameters());
	}
}
