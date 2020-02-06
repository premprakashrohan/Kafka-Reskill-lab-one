package com.action.reactive;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.ibm.kafka.common.ConsumerCreator;
import com.ibm.kafka.common.IKafkaConstants;
import com.ibm.kafka.common.ProducerCreator;
import com.ibm.kafka.model.CustomObject;
import com.opencsv.CSVReader;

@SpringBootApplication
public class KafkaReskillLabOneApplication implements CommandLineRunner {

	private static Logger logger = LoggerFactory.getLogger(KafkaReskillLabOneApplication.class);
	static Map<String, String> topicMap = Stream.of(new Object[][] { { "Singapore", "SINGAPORE" },
			{ "Cyprus", "CYPRUS" }, { "Hong Kong", "HONG_KONG" }, { "Portugal", "PORTUGAL" }, { "Iceland", "ICELAND" },
			{ "Malta", "MALTA" }, { "Greece", "GREECE" }, { "Saudi Arabia", "SAUDI_ARABIA" },
			{ "Netherlands", "NETHERLANDS" }, { "Sweden", "SWEDEN" }, { "Austria", "AUSTRIA" }, { "Poland", "POLAND" },
			{ "Brazil", "BRAZIL" }, { "France", "FRANCE" }, { "Lithuania", "LITHUANIA" }, { "RSA", "RSA" },
			{ "USA", "USA" }, { "Japan", "JAPAN" }, { "Channel Islands", "CHANNEL_ISLANDS" },
			{ "European Community", "EUROPEAN_COMMUNITY" }, { "United Kingdom", "UNITED_KINGDOM" },
			{ "United Arab Emirates", "UNITED_ARAB_EMIRATES" }, { "Unspecified", "UNSPECIFIED" },
			{ "Switzerland", "SWITZERLAND" }, { "Bahrain", "BAHRAIN" }, { "Spain", "SPAIN" }, { "Lebanon", "LEBANON" },
			{ "Canada", "CANADA" }, { "Czech Republic", "CZECH_REPUBLIC" }, { "Belgium", "BELGIUM" },
			{ "Norway", "NORWAY" }, { "EIRE", "EIRE" }, { "Finland", "FINLAND" }, { "Denmark", "DENMARK" },
			{ "Italy", "ITALY" }, { "Israel", "ISRAEL" }, { "Australia", "AUSTRALIA" }, { "Germany", "GERMANY" },

	}).collect(Collectors.toMap(data -> (String) data[0], data -> (String) data[1]));

	public static void main(String[] args) {
		SpringApplication.run(KafkaReskillLabOneApplication.class, args);
	}

	@Override
	public void run(String... args) {

		Producer<Long, CustomObject> producer = ProducerCreator.createProducer();
		CSVReader reader = null;
		try {
			reader = new CSVReader(new FileReader(IKafkaConstants.FILE_LOCATION));
			String[] line;
			while ((line = reader.readNext()) != null) {
				sendData(line, producer);
				System.out.println(line.toString());
			}
			reader.close();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		producer.flush();
		producer.close();
		System.out.println(topicMap);

		Consumer<Long, CustomObject> consumer = ConsumerCreator.createConsumer();
		topicMap.forEach((k, v) -> {
			reciveDataByTopic(v, consumer);
		});
		// consumer.commitAsync();
		// consumer.close();
	}

	public static void sendData(String[] str, Producer<Long, CustomObject> producer) {

		CustomObject c = null;
		try {
			c = new CustomObject(str);
		} catch (Exception e) {
			logger.error(Arrays.deepToString(str), e);
		}
		if (c != null) {

			final ProducerRecord<Long, CustomObject> record = new ProducerRecord<Long, CustomObject>(
					c.getCountrySpecificTopic(), c);
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {

					if (exception == null) {
						logger.info("Received new metadata. \n" + "Topic: " + metadata.topic() + "\n Partition: "
								+ +metadata.partition() + "\n Offset: " + metadata.offset() + "\n Time: "
								+ metadata.timestamp());
					} else {
						logger.error(exception.getMessage());
					}
				}
			});
		}
	}

	public static void reciveDataByTopic(final String TOPIC_NAME, Consumer<Long, CustomObject> consumer) {

		consumer.subscribe(Collections.singletonList(TOPIC_NAME));
		final ConsumerRecords<Long, CustomObject> consumerRecords = consumer.poll(1000);

		consumerRecords.forEach(record -> {
			logger.info("Received new record metadata. \n" + "Topic: " + record.topic() + "\n Partition: "
					+ +record.partition() + "\n Offset: " + record.offset() + "\n Time: " + record.timestamp());
			logger.info(record.key() + ": " + record.value());
		});
	}

}