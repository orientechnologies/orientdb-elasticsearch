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
package com.orientechnologies.server.plugin.es;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.*;

/**
 * Elastic Search connector plugin.
 * 
 * @author Luca Garulli
 */
public class OElasticSearchDatabaseSync extends ODocumentHookAbstract {

  private String                                    dbName;
  private final OElasticSearchDatabaseConfiguration esClient;

  public OElasticSearchDatabaseSync(final String dbName, final OElasticSearchDatabaseConfiguration esClient) {
    this.dbName = dbName;
    this.esClient = esClient;
  }

  public long syncBatch(final Iterator<? extends OIdentifiable> iterator) {
    final BulkProcessor bulkProcessor = BulkProcessor.builder(esClient.getClient(), new BulkProcessor.Listener() {

      @Override
      public void beforeBulk(long l, BulkRequest bulkRequest) {
      }

      @Override
      public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
      }

      @Override
      public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
      }
    }).setBulkActions(10000).setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB)).setFlushInterval(TimeValue.timeValueSeconds(30))
        .build();

    long syncItems;
    for (syncItems = 0; iterator.hasNext(); syncItems++) {
      final OIdentifiable id = iterator.next();

      final ORecord record = id.getRecord();

      final Map<String, Object> map = getMap(record);

      if (map != null)
        bulkProcessor.add(
            new IndexRequest(getIndexName(), ((ODocument) record).getClassName(), record.getIdentity().toString()).source(map));
    }

    bulkProcessor.close();

    return syncItems;
  }

  protected Map<String, Object> getMap(final ORecord record) {
    Map<String, Object> map = null;

    if (record instanceof ODocument) {
      final ODocument doc = (ODocument) record;

      final Set<String> syncFields = esClient.getSyncFields(doc);
      if (syncFields == null)
        return null;

      map = new HashMap<String, Object>();

      map.put("@rid", doc.getIdentity());
      map.put("@class", doc.getClassName());

      for (String f : doc.fieldNames()) {
        if (!syncFields.isEmpty() && !syncFields.contains(f))
          // SKIP FIELD
          continue;

        final Object value = doc.field(f);

        if (value instanceof ORidBag) {
          final List<OIdentifiable> list = new ArrayList<OIdentifiable>();
          for (Iterator<OIdentifiable> it = ((ORidBag) value).rawIterator(); it.hasNext();) {
            list.add(it.next());
          }
          map.put(f, list);

        } else if (value instanceof ORecord && ((ORecord) value).getIdentity().isPersistent()) {
          map.put(f, ((ORecord) value).getIdentity());
        } else
          map.put(f, value);
      }
    }

    return map;
  }

  @Override
  public void onRecordAfterCreate(final ODocument iDocument) {
    esClient.getClient().prepareIndex(getIndexName(), iDocument.getClassName(), iDocument.getIdentity().toString())
        .setSource(iDocument.toMap()).get();
  }

  @Override
  public void onRecordAfterUpdate(ODocument iDocument) {
    esClient.getClient().prepareIndex(getIndexName(), iDocument.getClassName(), iDocument.getIdentity().toString())
        .setSource(iDocument.toMap()).get();
  }

  @Override
  public void onRecordAfterDelete(ODocument iDocument) {
    esClient.getClient().delete(new DeleteRequest(getIndexName(), iDocument.getClassName(), iDocument.getIdentity().toString()));
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.BOTH;
  }

  protected String getIndexName() {
    return dbName.toLowerCase();
  }

  public void dropClass(final String className) {
    SearchResponse scrollResponse = esClient.getClient().prepareSearch(getIndexName()).setTypes(className)
        .setSearchType(SearchType.SCAN).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100).get();
    final BulkRequestBuilder bulkRequestBuilder = esClient.getClient().prepareBulk().setRefresh(true);
    while (true) {
      if (scrollResponse.getHits().getHits().length == 0) {
        break;
      }

      for (SearchHit hit : scrollResponse.getHits().getHits()) {
        bulkRequestBuilder.add(esClient.getClient().prepareDelete(getIndexName(), className, hit.getId()));
      }

      scrollResponse = esClient.getClient().prepareSearchScroll(scrollResponse.getScrollId()).setScroll(new TimeValue(60000)).get();
    }
    if (bulkRequestBuilder.numberOfActions() > 0) {
      bulkRequestBuilder.get();
    }
  }

  public void drop() {
    esClient.getClient().admin().indices().delete(new DeleteIndexRequest(getIndexName())).actionGet();
  }
}
