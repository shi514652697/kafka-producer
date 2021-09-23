package com.learnkafka.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;

@RestController
public class LibraryEventsController {
	
	@Autowired
	LibraryEventProducer libraryEventProducer;
	
	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException
	{
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		libraryEventProducer.sendLibraryEvent(libraryEvent); // asyc call
		//libraryEventProducer.sendLibraryEventSynchronous(libraryEvent); //sync call
		//libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);  // aproach 2
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
		
	}
	
	@PutMapping("/v1/libraryevent")
	public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException
	{
		if(libraryEvent.getLibraryEventId() == null)
		{
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryId");
		}
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEventProducer.sendLibraryEvent(libraryEvent); // asyc call
		//libraryEventProducer.sendLibraryEventSynchronous(libraryEvent); //sync call
		//libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);  // aproach 2
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
		
	}

}
