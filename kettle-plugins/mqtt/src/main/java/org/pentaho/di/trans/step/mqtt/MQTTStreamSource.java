/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.step.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.pentaho.di.trans.streaming.common.BlockingQueueStreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static java.util.Collections.singletonList;

public class MQTTStreamSource extends BlockingQueueStreamSource<List<Object>> {
  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  private final String broker;
  private final List<String> topics;
  private final int qos;
  MqttClient mqttClient;
  MemoryPersistence persistence = new MemoryPersistence();
  MqttConnectOptions connOpts = new MqttConnectOptions();
  private MqttCallback callback = new MqttCallback() {
    @Override public void connectionLost( Throwable cause ) {
      error( cause );
    }

    @Override public void messageArrived( String topic, MqttMessage message ) throws Exception {
      acceptRows( singletonList( singletonList( new String( message.getPayload() ) ) ) );
    }

    @Override public void deliveryComplete( IMqttDeliveryToken token ) {

    }
  };

  public MQTTStreamSource( String broker, List<String> topics, int qualityOfService ) {
    this.broker = broker;
    this.topics = topics;
    this.qos = qualityOfService;
    // todo.  qos is apparently defined by topic.
  }


  @Override public void open() {
    try {
      // need to support udp?
      mqttClient = new MqttClient( "tcp://" + broker, "consumer", persistence );
      mqttClient.connect( connOpts );

      mqttClient.setCallback( callback );
      mqttClient.subscribe( topics.toArray( new String[0] ) );

    } catch ( MqttException e ) {
      logger.error( e.getMessage(), e );
    }
  }

  @Override public void close() {
    super.close();
    try {
      mqttClient.disconnect();
      mqttClient.close();
    } catch ( MqttException e ) {
      logger.error( e.getMessage(), e );
    }

  }
}
