package com.abhishek.batch_processing.processors;

import com.abhishek.batch_processing.models.Person;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class PersonItemProcessor implements ItemProcessor<Person, Person> {
    @Override
    public Person process(Person item) throws Exception {
        return new Person(item.firstName().toUpperCase(), item.lastName().toUpperCase());
    }
}
