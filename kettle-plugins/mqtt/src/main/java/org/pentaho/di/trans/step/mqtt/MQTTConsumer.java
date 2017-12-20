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

import com.google.common.base.Preconditions;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleMissingPluginsException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.SubtransExecutor;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepStatus;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorData;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorParameters;
import org.pentaho.di.trans.streaming.common.BaseStreamStep;
import org.pentaho.di.trans.streaming.common.FixedTimeStreamWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Throwables.propagate;

/**
 * An example step plugin for purposes of demonstrating a strategy for handling streams of data.
 */
public class MQTTConsumer extends BaseStreamStep implements StepInterface {

  private static Class<?> PKG = MQTTConsumer.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private MQTTConsumerMeta mqttConsumerMeta;

  public MQTTConsumer( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                       Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    Preconditions.checkNotNull( stepMetaInterface );
    mqttConsumerMeta = (MQTTConsumerMeta) stepMetaInterface;

    RowMeta rowMeta = new RowMeta();
    try {
      mqttConsumerMeta.getFields(
        rowMeta, getStepname(), null, null, this, repository, metaStore );
    } catch ( KettleStepException e ) {
      // todo something.
    }
    window = new FixedTimeStreamWindow<>( subtransExecutor, rowMeta, getDuration(), getBatchSize() );
    //todo source = new TailFileStreamSource( sourceFile );
    source = new MQTTStreamSource(
      mqttConsumerMeta.getMqttServer(), mqttConsumerMeta.getTopics(),  1 );
    return super.init( stepMetaInterface, stepDataInterface );
  }

  private String getFilePath( String path ) {
    try {
      final FileObject fileObject = KettleVFS.getFileObject( environmentSubstitute( path ) );
      if ( !fileObject.exists() ) {
        throw new FileNotFoundException( path );
      }
      return Paths.get( fileObject.getURL().toURI() ).normalize().toString();
    } catch ( URISyntaxException | FileNotFoundException | FileSystemException | KettleFileException e ) {
      propagate( e );
    }
    return null;
  }

  @Override public Collection<StepStatus> subStatuses() {
    return subtransExecutor != null ? subtransExecutor.getStatuses().values() : Collections.emptyList();
  }

}
