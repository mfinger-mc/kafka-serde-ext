/*
 * Copyright 2018 Macronova.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.macronova.kafka.common.serialization.utils;

import kafka.common.KafkaException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;


public class KafkaEmbeddedHolder {
	private static EmbeddedKafkaRule embeddedKafka;
	private static boolean started = false;
	private static int topicId = 0;
	private static String topicPrefix = "topic";

	 public static EmbeddedKafkaRule getEmbeddedKafka() {
        if (!started) {
            try {

            	embeddedKafka = new EmbeddedKafkaRule(1, true).kafkaPorts(9092);

                embeddedKafka.before();
                ++topicId;
                embeddedKafka.getEmbeddedKafka().addTopics(topicName());
            }
            catch (Exception e) {
            	System.out.println("**************"+e);
                throw new KafkaException(e);
            }
            started = true;
        }
        System.out.println("***************"+embeddedKafka.toString());
        return embeddedKafka;
    }


	public static void destroy() {
		if ( started ) {
			try {
				embeddedKafka.getEmbeddedKafka().destroy();
			}
			catch ( Exception e ) {
				// Ignore.
			}
			embeddedKafka = null;
			started = false;
		}
	}

	public static String topicName() {


	 	return topicPrefix + topicId;
	}

	private KafkaEmbeddedHolder() {
		super();
	}
}
