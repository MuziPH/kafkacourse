package io.glitchtech.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {
    public static void main(String[] args) throws StreamException, InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // set Producer properties
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // set high throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //32KB
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // create a Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // create the topic
        String topic = "wikimedia.recentchange";

        // create an EventHandler
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder eventSource = new EventSource.Builder(URI.create(url));
        BackgroundEventSource backgroundEventSource =
                new BackgroundEventSource.Builder(eventHandler, eventSource).build();
        // start the producer in another thread
        backgroundEventSource.start();

        // we produce for 10 minutes and block program
        TimeUnit.MINUTES.sleep(10);

    }
}
