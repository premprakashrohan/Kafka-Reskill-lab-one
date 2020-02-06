package com.ibm.kafka.util;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.kafka.model.CustomObject;

public class CustomDeserializer implements Deserializer<CustomObject> {
	public void configure(Map<String, ?> configs, boolean isKey) {
	}

	public CustomObject deserialize(String topic, byte[] data) {
		ObjectMapper mapper = new ObjectMapper();
		CustomObject object = null;
		try {
			object = mapper.readValue(data, CustomObject.class);
		} catch (Exception exception) {
			System.out.println("Error in deserializing bytes " + exception);
		}
		return object;
	}

	public void close() {
	}
}