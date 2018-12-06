/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.profsoft.kafka.messages;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
/**
 *
 * @author filip
 */
public class KafkaUtils implements Runnable {

	javax.swing.JTextPane jTextIn;

	public KafkaUtils(javax.swing.JTextPane jTextIn) {
		this.jTextIn = jTextIn;
	}

	private Thread thread;

	public static boolean checkConnection() {
		boolean reachable = false;
		try {
			InetAddress address = InetAddress.getByName("172.16.2.0");

			// Try to reach the specified address within the timeout
			// period. If during this period the address cannot be
			// reach then the method returns false.
			reachable = address.isReachable(10000);
			System.out.println("Is host reachable? " + reachable);
			return reachable;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return reachable;
	}

	private final static String TOPIC = "topic-test";
	private final static String BOOTSTRAP_SERVERS = "192.168.1.47:9092";

	private Consumer<Long, String> createConsumer() {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
			BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG,
			"test-consumer-group");
		/*props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
			LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
			StringDeserializer.class.getName());
		 */
		props.put("key.deserializer",
			"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
			"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		// Create the consumer using props.
		final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC));
		return consumer;
	}

	public void runConsumer(javax.swing.JTextPane jTextIn) throws InterruptedException {
		final Consumer<Long, String> consumer = createConsumer();

		final int giveUp = 100;
		int noRecordsCount = 0;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(2000));

			consumerRecords.forEach(record -> {
				if (record != null) {
					System.out.printf("Consumer Record:(%s)\n", record.value());
					//("Consumer Record: " + record.value());
					jTextIn.setText(record.value());
				}
			});

			consumer.commitAsync();
		}
		//consumer.close();
		//System.out.println("DONE");
	}

	@Override
	public void run() {
		try {
			runConsumer(this.jTextIn);
		} catch (InterruptedException ex) {
			Logger.getLogger(KafkaUtils.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public void start() {
		System.out.println("Starting kafka-consumer-listener");
		if (thread == null) {
			thread = new Thread(this, "kafka-consumer-listener");
			thread.start();
		}
	}

}
