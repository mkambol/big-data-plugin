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


import org.apache.activemq.broker.BrokerService;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.pentaho.di.trans.streaming.api.StreamSource;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith ( MockitoJUnitRunner.class )
public class MQTTStreamSourceTest {

  int port;
  private BrokerService brokerService;

  private ExecutorService executorService = Executors.newSingleThreadExecutor();


  @Before
  public void startBroker() throws Exception {
    port = findFreePort();
    brokerService = new BrokerService();
    brokerService.setDeleteAllMessagesOnStartup( true );
    brokerService.setPersistent( false );
    brokerService.addConnector( "mqtt://localhost:" + port );
    brokerService.start();
    brokerService.waitUntilStarted();
  }

  private int findFreePort() throws IOException {
    ServerSocket socket = new ServerSocket( 0 ); // 0 = allocate port automatically
    int freePort = socket.getLocalPort();
    socket.close();
    return freePort;
  }


  @Test
  public void testMqttStream() throws Exception {
    StreamSource<List<Object>> source =
      new MQTTStreamSource( "127.0.0.1:" + port, Arrays.asList( "mytopic" ), 2 );
    source.open();

    final String[] messages = { "foo", "bar", "baz" };
    publish( messages );

    Iterator<List<Object>> iter = source.rows().iterator();
    Future<List<List<Object>>> futureRows = executorService.submit( () -> {
      List<List<Object>> rows = new ArrayList<>();
      for ( int i = 0; i < 3; i++ ) {
        rows.add( iter.next() );
      }
      return rows;
    } );
    List<List<Object>> rows = getQuickly( futureRows );
    assertThat( messagesToRows( messages ), equalTo( rows ) );
    source.close();

  }

  private List<List<Object>> messagesToRows( String[] messages ) {
    return Arrays.stream( messages )
      .map( message -> (Object) message )
      .map( Collections::singletonList )
      .collect( Collectors.toList() );
  }


  private void publish( String... messages ) throws MqttException {
    MqttClient pub = null;
    try {
      pub = new MqttClient( "tcp://127.0.0.1:" + port, "producer",
        new MemoryPersistence() );
      pub.connect();
      for ( String msg : messages ) {
        pub.publish( "mytopic", new MqttMessage( msg.getBytes() ) );
      }
    } finally {
      pub.disconnect();
      pub.close();
    }

  }

  private <T> T getQuickly( Future<T> future ) {
    try {
      return future.get( 50, MILLISECONDS );
    } catch ( InterruptedException | ExecutionException | TimeoutException e ) {
      fail();
    }
    return null;
  }

}