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
package com.orientechnologies.es.command;

import com.orientechnologies.common.collection.OIterableObject;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.orientechnologies.es.plugin.es.OElasticSearchPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OServerCommandESSync extends OServerCommandAuthenticatedDbAbstract {
  private static final String[]      NAMES = { "GET|essync/*", "POST|essync/*" };

  private final OElasticSearchPlugin es;

  public OServerCommandESSync(final OElasticSearchPlugin es) {
    this.es = es;
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts = checkSyntax(iRequest.url, 1, "Syntax error: essync/<database>");

    String command = null;
    List<String> classes = null;
    List<String> clusters = null;

    if (iRequest.content != null && !iRequest.content.isEmpty()) {
      // CONTENT REPLACES TEXT
      if (iRequest.content.startsWith("{")) {
        // JSON PAYLOAD
        final ODocument doc = new ODocument().fromJSON(iRequest.content);
        command = doc.field("command");
        clusters = doc.field("clusters");
        classes = doc.field("classes");
      }
    }

    iRequest.data.commandInfo = "Elastic Search Sync";
    iRequest.data.commandDetail = command != null ? "command: " + command
        : classes != null ? "classes: " + classes.toString() : clusters != null ? "clusters: " + clusters.toString() : "database";

    ODatabaseDocument db = null;

    Object response;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      long syncItems = 0;

      OLogManager.instance().info(this, "ES plugin: synchronizing records", syncItems);

      if (command != null) {
        // COMMAND
        final OCommandRequestText cmd = (OCommandRequestText) OCommandManager.instance().getRequester("sql");
        cmd.setText(command);

        final OCommandExecutor executor = OCommandManager.instance().getExecutor(cmd);
        executor.setContext(cmd.getContext());
        executor.setProgressListener(cmd.getProgressListener());
        executor.parse(cmd);

        final Object result = db.command(cmd).execute();

        if (result instanceof OIdentifiable)
          syncItems += es.getDatabase(db.getName()).syncBatch(new OIterableObject<OIdentifiable>((OIdentifiable) result));
        else if (result instanceof Collection)
          syncItems += es.getDatabase(db.getName()).syncBatch(((Collection) result).iterator());
        else
          throw new IllegalArgumentException("The result of command '" + cmd + "' cannot be synchronized");

      } else if (classes != null) {
        // CLASSES
        for (String cl : classes) {
          syncItems += es.getDatabase(db.getName()).syncBatch(db.browseClass(cl));
        }

      } else if (clusters != null) {
        // CLUSTERS
        for (String cl : clusters) {
          syncItems += es.getDatabase(db.getName()).syncBatch(db.browseCluster(cl));
        }
      } else {
        // ENTIRE DATABASE BASED ON CFG
        clusters = new ArrayList<String>(db.getClusterNames());
        for (String cl : clusters) {
          syncItems += es.getDatabase(db.getName()).syncBatch(db.browseCluster(cl));
        }
      }

      OLogManager.instance().info(this, "ES plugin: synchronized %d records", syncItems);

      iResponse.writeResult("Synchronized " + syncItems + " records", null, null);

    } finally {
      if (db != null)
        db.close();
    }

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
