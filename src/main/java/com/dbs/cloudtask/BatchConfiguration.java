package com.dbs.cloudtask;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
//import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import com.dbs.cloudtask.domain.Person;


@Configuration
//@EnableTask
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Bean
	public Job job(ItemReader<Person> itemReader, ItemWriter<Person> itemWriter) {
		Step step1 = stepBuilderFactory.get("file-db").<Person, Person>chunk(100).reader(itemReader).writer(itemWriter)
				.processor(item -> {
					item.setFirstName(item.getFirstName().toUpperCase());
					return item;
				}).build();
		Job job = jobBuilderFactory.get("etl").incrementer(new RunIdIncrementer()).start(step1).build();
		return job;
	}

	@Bean
	@StepScope
	public FlatFileItemReader<Person> reader(@Value("#{jobParameters['fileName']}") Resource fileName) {
		FlatFileItemReader<Person> itemReader = new FlatFileItemReader<Person>();
		itemReader.setName("file-reader");
		itemReader.setLineMapper(lineMapper());
		itemReader.setResource(fileName);
		return itemReader;
	}

	@Bean
	public LineMapper<Person> lineMapper() {
		DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<Person>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_COMMA);
		lineTokenizer.setNames(new String[] { "firstName", "age", "email" });
		BeanWrapperFieldSetMapper<Person> fieldSetMapper = new BeanWrapperFieldSetMapper<Person>();
		fieldSetMapper.setTargetType(Person.class);
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;
	}

	@Bean
	public ItemWriter<Person> writer(DataSource dataSource) {
		JdbcBatchItemWriter<Person> itemWriter = new JdbcBatchItemWriter<Person>();
		itemWriter.setSql("INSERT INTO PEOPLE (AGE, FIRST_NAME, EMAIL) VALUES (:age, :firstName, :email)");
		itemWriter.setDataSource(dataSource);
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
		return itemWriter;
	}

}
