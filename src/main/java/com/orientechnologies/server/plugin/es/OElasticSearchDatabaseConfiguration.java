/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under Commercial license.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.server.plugin.es;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.elasticsearch.client.Client;

import java.util.*;

/**
 * Elastic Search database configuration.
 * 
 * @author Luca Garulli
 */
public class OElasticSearchDatabaseConfiguration {
  private Client                         client;
  private final Map<String, Set<String>> includeClasses  = new HashMap<String, Set<String>>();
  private final Map<String, Set<String>> includeClusters = new HashMap<String, Set<String>>();
  private final Set<String>              excludeClasses  = new HashSet<String>();
  private final Set<String>              excludeClusters = new HashSet<String>();

  public OElasticSearchDatabaseConfiguration(final Client client, final ODocument configuration) {
    this.client = client;

    if (configuration.eval("exclude.classes") != null)
      excludeClasses.addAll((Collection<String>) configuration.eval("exclude.classes"));

    if (configuration.eval("exclude.clusters") != null)
      excludeClusters.addAll((Collection<String>) configuration.eval("exclude.clusters"));

    if (configuration.eval("include.clusters") != null) {
      final ODocument incClusters = (ODocument) configuration.eval("include.clusters");
      for (String cl : incClusters.fieldNames()) {
        final Collection<String> clFields = incClusters.field(cl);
        if (clFields != null)
          includeClusters.put(cl, new HashSet<String>(clFields));
        else
          includeClusters.put(cl, new HashSet<String>());
      }
    }

    if (configuration.eval("include.classes") != null) {
      final ODocument incClasses = (ODocument) configuration.eval("include.classes");
      for (String cl : incClasses.fieldNames()) {
        final Collection<String> clFields = incClasses.field(cl);
        if (clFields != null)
          includeClasses.put(cl, new HashSet<String>(clFields));
        else
          includeClasses.put(cl, new HashSet<String>());
      }
    }
  }

  /**
   * Returns null if the record must be not synchronized, other wise the set of fields to synchronize. An empty set means all the
   * fields.
   */
  public Set<String> getSyncFields(final ODocument record) {
    final String className = record.getClassName();
    if (className == null)
      return null;

    if (excludeClasses.contains(className))
      return null;

    final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();

    final String clusterName = db.getClusterNameById(record.getIdentity().getClusterId());
    if (excludeClasses.contains(clusterName))
      return null;

    if (!includeClasses.isEmpty()) {
      if (includeClasses.containsKey(className))
        return includeClasses.get(className);
    }

    if (!includeClusters.isEmpty()) {
      if (includeClusters.containsKey(clusterName))
        return includeClusters.get(clusterName);
    }

    // SYNCHRONIZE ALL FIELDS
    return Collections.EMPTY_SET;
  }

  public Client getClient() {
    return client;
  }
}
