## Installation

This is the manual procedure to install Elastic Search plugin in OrientDB.

### 1. Compiles the plugin

`mvn clean install`

### 2. Copy libraries under OrientDB's `lib` directory.

- Copy the generated `orientdb-elasticsearch-*.jar` file is under the target directory
- Copy the `elasticsearch-2.3.3.jar` file. You can find this jar on Sonatype repository online

### 3. Register the OrientDB Elastic Search plugin in OrientDB's `config/orientdb-config.xml` file under the `handlers` (Handler is a plugin) tag:

``xml
<handler class="com.orientechnologies.server.plugin.es.OElasticSearchPlugin">
    <parameters>
        <parameter value="true" name="enabled"/>
    </parameters>
</handler>
```

### 4. Configure the synchronization

The Elastic Search plugin creates this file under `databases/<your-db>/elastic-search-config.json`:

```json
{
  "es": {
    "host": "localhost",
    "port": 9220,
    "clusterName": "elasticsearch"
  },
  "include": {
    "classes": {
    },
    "clusters": {
    }
  },
  "exclude": {
    "classes": [],
    "clusters": []
  }
}
```

You can modify it to configure the synchronization.
