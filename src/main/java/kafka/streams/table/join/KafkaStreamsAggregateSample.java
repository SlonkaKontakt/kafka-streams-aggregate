/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.streams.table.join;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Function;

@SpringBootApplication
public class KafkaStreamsAggregateSample {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsAggregateSample.class, args);
	}

	public static class KafkaStreamsAggregateSampleApplication {

		@Bean
		public Function<KStream<String, String>, KStream<String, String>> aggregate() {


			return input -> input.map(this::getKeyValue);
		}

		private KeyValue<String, String> getKeyValue(String a, String b) {
			System.out.println(a + b);
			return KeyValue.pair(a, b + a);
		}
	}

}
