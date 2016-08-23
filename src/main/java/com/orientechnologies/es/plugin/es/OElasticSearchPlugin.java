/*
 *
 *  *  Copyright 2014 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.es.plugin.es;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;
import com.orientechnologies.es.command.OServerCommandESSync;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Elastic Search connector plugin.
 */
public class OElasticSearchPlugin extends OServerPluginAbstract implements ODatabaseLifecycleListener {
  private OServer                                                        server;
  private ConcurrentHashMap<String, OElasticSearchDatabaseConfiguration> clientConfigurations = new ConcurrentHashMap<String, OElasticSearchDatabaseConfiguration>();

  private boolean                                                        enabled              = false;

  @Override
  public void config(final OServer server, final OServerParameterConfiguration[] iParams) {
    this.server = server;

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (Boolean.parseBoolean(param.value))
          // ENABLE IT
          enabled = true;
      }
    }
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    Orient.instance().addDbLifecycleListener(this);

    final OServerNetworkListener listener = server.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
    if (listener == null)
      throw new OConfigurationException("HTTP listener not found");

    listener.registerStatelessCommand(new OServerCommandESSync(this));
  }

  @Override
  public void shutdown() {
    if (clientConfigurations != null) {
      for (OElasticSearchDatabaseConfiguration c : clientConfigurations.values())
        c.getClient().close();

      clientConfigurations.clear();
    }
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.LAST;
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
    onOpen(iDatabase);
  }

  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {
    final OElasticSearchDatabaseSync db = new OElasticSearchDatabaseSync(iDatabase.getName(), getESClient(iDatabase.getName()));
    iDatabase.registerHook(db);
  }

  @Override
  public void onClose(final ODatabaseInternal iDatabase) {
  }

  @Override
  public void onDrop(final ODatabaseInternal iDatabase) {
    getDatabase(iDatabase.getName()).drop();
  }

  @Override
  public void onCreateClass(final ODatabaseInternal iDatabase, final OClass iClass) {
  }

  @Override
  public void onDropClass(final ODatabaseInternal iDatabase, final OClass iClass) {
    getDatabase(iDatabase.getName()).dropClass(iClass.getName());
  }

  @Override
  public void onLocalNodeConfigurationRequest(final ODocument iConfiguration) {
  }

  public OElasticSearchDatabaseConfiguration getESClient(final String dbName) {
    OElasticSearchDatabaseConfiguration clientCfg = clientConfigurations.get(dbName);

    if (clientCfg == null) {
      final ODocument configuration = new ODocument();

      final File esConfig = new File(server.getDatabaseDirectory() + dbName + "/elastic-search-config.json");
      if (esConfig.exists()) {
        try {
          configuration.fromJSON(OIOUtils.readFileAsString(esConfig), "noMap");
        } catch (IOException e) {
          OLogManager.instance().error(this, "Error on loading JSON file: %s", e, esConfig);
        }
      }

      try {
        String esClusterName = "elasticsearch";
        String esServer = "localhost";
        int esPort = 9300;

        if (configuration.containsField("es")) {
          // READ SETTING FROM CONFIGURATION
          esServer = (String) configuration.eval("es.host");
          esPort = (Integer) configuration.eval("es.port");
          esClusterName = (String) configuration.eval("es.clusterName");
        }

        final Settings settings = Settings.settingsBuilder().put("cluster.name", esClusterName).build();
        clientCfg = new OElasticSearchDatabaseConfiguration(TransportClient.builder().settings(settings).build()
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esServer), esPort)), configuration);

        final OElasticSearchDatabaseConfiguration existentClient = clientConfigurations.putIfAbsent(dbName, clientCfg);
        if (existentClient != null) {
          // CONCURRENT INSERT: CLOSE CURRENT ONE AND USE THE EXISTENT
          clientCfg.getClient().close();
          clientCfg = existentClient;
        }
      } catch (UnknownHostException e) {
        OLogManager.instance().error(this, "Error on connecting to ES server", e);
      }

    }

    return clientCfg;
  }

  public OElasticSearchDatabaseSync getDatabase(final String dbName) {
    return new OElasticSearchDatabaseSync(dbName, getESClient(dbName));
  }

  @Override
  public String getName() {
    return "es-plugin";
  }

}
