package com.learnkafka.producer;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {

	@Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;
	
	@Autowired
	ObjectMapper objectMapper;
	
	String topic = "library-events";
	
	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException
	{
		final Integer key = libraryEvent.getLibraryEventId();
		final String value = objectMapper.writeValueAsString(libraryEvent);
		ListenableFuture<SendResult<Integer, String>> listenableFuture= kafkaTemplate.sendDefault(key, value);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

			public void onFailure(Throwable ex) {
				handleFailure(key,value,ex);
			}
		});
		
	 }
	
	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException
	{
		final Integer key = libraryEvent.getLibraryEventId();
		final String value = objectMapper.writeValueAsString(libraryEvent);
		SendResult<Integer, String> result = null;
		try {
			 result = kafkaTemplate.sendDefault(key, value).get(2, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	
	public ListenableFuture<SendResult<Integer, String>> sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException
	{
		final Integer key = libraryEvent.getLibraryEventId();
		final String value = objectMapper.writeValueAsString(libraryEvent);
		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key,value,topic);
		ListenableFuture<SendResult<Integer, String>> listenableFuture= kafkaTemplate.send(producerRecord);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

			public void onFailure(Throwable ex) {
				handleFailure(key,value,ex);
			}
		});
		return listenableFuture;
	 }
	
	
	
		private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
		List<Header> recordHeader = List.of
				(new RecordHeader("event-source", "scanner".getBytes()));
		return new ProducerRecord<Integer, String>(topic, null, key, value, recordHeader);
	}

		private void handleFailure(Integer key, String value, Throwable ex) {
			log.error("Error sending the message and the exception is {}", ex.getMessage());
		
	}
		private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
			log.info("Message sent successfully for the key : {} and the value is {}  partition is {}", key, value, result.getRecordMetadata().partition() );
		}
		
	}

 