package com.learnkafka.controller;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers},spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsControllerTest {
	
	@Autowired
	TestRestTemplate testRestTemplate;
	
	private Consumer<Integer, String> consumer;
	
	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;
	
	
	@BeforeEach
	void setUp()
	{
		Map<String, Object> config =new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
		consumer = new DefaultKafkaConsumerFactory(config, new IntegerDeserializer(),new StringDeserializer()).createConsumer();
		embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
	}
	
	@AfterEach
	void tearDown()
	{
		consumer.close();
	}

	@Test
	@Timeout(5)
	void testPostLibraryEvent() throws InterruptedException {
		
		Book book = Book.builder()
				.bookId(234)
				.bookAuthor("negi")  
				.bookName("Kafta using Spring Boot")
				.build();
		
		LibraryEvent libraryEvent = LibraryEvent.builder()
				.libraryEventId(null)
				.book(book)
				.build();
		HttpHeaders headers = new HttpHeaders();
		headers.set("content-type", MediaType.APPLICATION_JSON.toString());
		HttpEntity<LibraryEvent> requestEntity = new HttpEntity<>(libraryEvent,headers);
		
		ResponseEntity<LibraryEvent>  responseEntity= testRestTemplate.exchange("/v1/libraryevent", HttpMethod.POST, requestEntity, LibraryEvent.class);
		
		assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());
		
		ConsumerRecord<Integer, String> consumerRecord= KafkaTestUtils.getSingleRecord(consumer, "library-events");
		
		String value= consumerRecord.value();
		Thread.sleep(3000);
		String expectedRecord= "{\"libraryEventId\":null,\"book\":{\"bookId\":234,\"bookName\":\"Kafta using Spring Boot\",\"bookAuthor\":\"negi\"},\"libraryEventType\":\"NEW\"}";
			
		assertEquals(expectedRecord, value);
	}

}
