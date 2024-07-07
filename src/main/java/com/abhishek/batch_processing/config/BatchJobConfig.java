package com.abhishek.batch_processing.config;

import com.abhishek.batch_processing.models.Person;
import com.abhishek.batch_processing.processors.ImpPersonItemProcessor;
import com.abhishek.batch_processing.processors.PersonItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchJobConfig {

    @Bean
    public Job importUserJob(JobRepository jobRepository, @Qualifier("splitflow") Flow splitflow) {
        return new JobBuilder("importUserJob", jobRepository)
                .start(splitflow)
                .build()
                .build();
    }

    @Bean(name = "splitflow")
    public Flow splitFlow(@Qualifier("flow1") Flow flow1, @Qualifier("flow2") Flow flow2) {
        return new FlowBuilder<SimpleFlow>("splitFlow")
                .split(taskExecutor())
                .add(flow1, flow2)
                .build();
    }


    @Bean(name = "flow1")
    public Flow flow1(@Qualifier("step1") Step step1) {
        return new FlowBuilder<SimpleFlow>("flow1")
                .start(step1)
                .build();
    }

    @Bean(name = "flow2")
    public Flow flow2(@Qualifier("step2") Step step2) {
        return new FlowBuilder<SimpleFlow>("flow2")
                .start(step2)
                .build();
    }

    @Bean(name = "step1")
    public Step step1(JobRepository jobRepository, DataSourceTransactionManager transactionManager,
                      FlatFileItemReader<Person> reader, PersonItemProcessor processor, JdbcBatchItemWriter<Person> writer) {
        return new StepBuilder("step1", jobRepository)
                .<Person, Person> chunk(3, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean(name = "step2")
    public Step step2(JobRepository jobRepository, DataSourceTransactionManager transactionManager,
                      FlatFileItemReader<Person> reader, ImpPersonItemProcessor processor, JdbcBatchItemWriter<Person> writer) {
        return new StepBuilder("step2", jobRepository)
                .<Person, Person> chunk(3, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public FlatFileItemReader<Person> reader() {
    return new FlatFileItemReaderBuilder<Person>()
        .name("personItemReader")
        .resource(new FileSystemResource("src/main/resources/sample.csv"))
        .delimited()
        .names("firstName", "lastName")
        .targetType(Person.class)
        .linesToSkip(1)
        .build();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
                .dataSource(dataSource)
                .beanMapped()
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor("spring_batch");
    }

}
