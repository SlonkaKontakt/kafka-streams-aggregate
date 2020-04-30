package kafka.streams.table.join;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaStreamsAggregateSampleTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, "foo", "bar");

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	@Autowired
	StreamsBuilderFactoryBean streamsBuilderFactoryBean;

	@Before
	public void before() {
		streamsBuilderFactoryBean.setCloseTimeout(0);
	}

	@BeforeClass
	public static void setUp() {
		System.out.println(embeddedKafka.getBrokersAsString());
		System.setProperty("spring.cloud.stream.kafka.streams.binder.brokers", embeddedKafka.getBrokersAsString());
	}

	@AfterClass
	public static void tearDown() {
		System.clearProperty("spring.cloud.stream.kafka.streams.binder.brokers");
	}

	@Test
	public void testKafkaStreamsWordCountProcessor() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);

		senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		Consumer<String, String> consumer = configureConsumer();
		try {
			KafkaTemplate<String, String> template = new KafkaTemplate<>(pf, true);
			template.setDefaultTopic("foo");
			template.sendDefault("1", "value1");

			ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(consumer, "bar");
			System.out.println(singleRecord);

		}
		finally {
			pf.destroy();
		}
	}

	private Consumer<String, String> configureConsumer() {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(consumerProps, StringDeserializer::new, StringDeserializer::new)
				.createConsumer();
		consumer.subscribe(Collections.singleton("bar"));
		return consumer;
	}
}
