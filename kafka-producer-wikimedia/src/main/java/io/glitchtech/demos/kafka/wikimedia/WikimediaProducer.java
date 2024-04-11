package io.glitchtech.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {
    public static void main(String[] args) throws StreamException, InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);

        // set Producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create a Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // create the topic
        String topic = "wikimedia.recentchange";

        // create an EventHandler
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder eventSource = new EventSource.Builder(URI.create(url));
        try (BackgroundEventSource backgroundEventSource = new BackgroundEventSource.Builder(eventHandler, eventSource).build()) {

            // start the producer in another thread
            backgroundEventSource.start();
        }

        // we produce for 10 minutes and block program
        TimeUnit.MINUTES.sleep(10);

    }
}
