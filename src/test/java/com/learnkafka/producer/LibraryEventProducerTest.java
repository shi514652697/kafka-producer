package com.learnkafka.producer;

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutionException;

import javax.management.RuntimeErrorException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerTest {
	
	@InjectMocks
	LibraryEventProducer libraryEventProducer;
	
	@Mock
	KafkaTemplate<Integer,String> kafkaTemplate;
	
	@Spy
	ObjectMapper objectMapper;

	@Test
	void testSendLibraryEvent_Approach2_Failure() throws JsonProcessingException, InterruptedException, ExecutionException {
		Book book = Book.builder()
				.bookId(234)
				.bookAuthor("negi")  
				.bookName("Kafta using Spring Boot")
				.build();
		
		LibraryEvent libraryEvent = LibraryEvent.builder()
				.libraryEventId(null)
				.book(book)
				.build();
		SettableListenableFuture future = new SettableListenableFuture();
		future.setException(new RuntimeException("Exception calling kafka"));
		when(kafkaTemplate.send(ArgumentMatchers.any(ProducerRecord.class))).thenReturn(future);
		
		assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent).get());
	}
	
	
	@Test
	void testSendLibraryEvent_Approach2_Success() throws JsonProcessingException, InterruptedException, ExecutionException {
		Book book = Book.builder()
				.bookId(234)
				.bookAuthor("negi")  
				.bookName("Kafta using Spring Boot")
				.build();
		
		LibraryEvent libraryEvent = LibraryEvent.builder()
				.libraryEventId(null)
				.book(book)
				.build();
		String record = objectMapper.writeValueAsString(libraryEvent);
		RecordMetadata recordMetadata= new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, System.currentTimeMillis(), null, 1, 1);
		ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>("library-events", libraryEvent.getLibraryEventId(), record);
		SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);
		SettableListenableFuture future = new SettableListenableFuture();
		future.set(sendResult);
		when(kafkaTemplate.send(ArgumentMatchers.any(ProducerRecord.class))).thenReturn(future);
		
		ListenableFuture<SendResult<Integer, String>> listenableFuture = libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);
		SendResult<Integer, String> sendResult1=listenableFuture.get();
		
		assert sendResult1.getRecordMetadata().partition()==1;
		
	}

}
