package com.learnkafka.producer;

import java.util.concurrent.ExecutionException;

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
			 result = kafkaTemplate.sendDefault(key, value).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	
	public void sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException
	{
		final Integer key = libraryEvent.getLibraryEventId();
		final String value = objectMapper.writeValueAsString(libraryEvent);
		ListenableFuture<SendResult<Integer, String>> listenableFuture= kafkaTemplate.send("library-events", key, value)
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

			public void onFailure(Throwable ex) {
				handleFailure(key,value,ex);
			}
		});
		
	 }
	
	
	
		private void handleFailure(Integer key, String value, Throwable ex) {
			log.error("Error sending the message and the exception is {}", ex.getMessage());
		
	}
		private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
			log.info("Message sent successfully for the key : {} and the value is {}  partition is {}", key, value, result.getRecordMetadata().partition() );
		}
		
	}

 