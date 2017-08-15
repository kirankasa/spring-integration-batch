package com.dbs.cloudtask;

import java.io.File;

import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.integration.launch.JobLaunchingMessageHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.integration.handler.LoggingHandler;

@Configuration
public class IntegrationConfiguration {
	
	private final  String DIRECTORY_NAME= "C:\\Users\\kiranreddykasa\\projects\\data\\import";

	@Bean
	@ServiceActivator(inputChannel = "jobChannel", outputChannel = "nullChannel")
	protected JobLaunchingMessageHandler launcher(JobLauncher jobLauncher) {
		return new JobLaunchingMessageHandler(jobLauncher);
	}

	@Bean
	@InboundChannelAdapter(value = "fileInputChannel", poller = @Poller(errorChannel = "errorChannel", fixedDelay = "1000", maxMessagesPerPoll = "1"))
	public MessageSource<File> fileReadingMessageSource() {
		CompositeFileListFilter<File> filters = new CompositeFileListFilter<>();
		filters.addFilter(new SimplePatternFileListFilter("*.csv"));
		filters.addFilter(new AcceptOnceFileListFilter<>());

		FileReadingMessageSource source = new FileReadingMessageSource();
		source.setAutoCreateDirectory(true);
		source.setDirectory(new File(DIRECTORY_NAME));
		source.setFilter(filters);
		return source;
	}

	@Bean
	public IntegrationFlow processFileFlow(FileMessageToJobRequestTransformer fileMessageToJobRequestTransformer) {
		return IntegrationFlows.from("fileInputChannel").transform(fileMessageToJobRequestTransformer)
				//.handle(new LoggingHandler("INFO"))
				.channel("jobChannel")
				.get();
	}

	@Bean
	public IntegrationFlow errorFlow() {
		return IntegrationFlows.from("errorChannel").handle(new LoggingHandler("ERROR")).get();
	}

	@Bean
	public FileToStringTransformer fileToStringTransformer() {
		return new FileToStringTransformer();
	}

}
