## Installation

This is the manual procedure to install Elastic Search plugin in OrientDB.

### 1. Compiles the plugin

`mvn clean install`

### 2. Copy libraries under OrientDB's `lib` directory.

- Copy the generated `orientdb-elasticsearch-*.jar` file is under the target directory into OrientDB lib directory
- Download Elastic Seatch 2.3.3 (or major) and copy all the libraries under lib directory into OrientDB lib directory

### 3. Register the OrientDB Elastic Search plugin

In OrientDB's `config/orientdb-config.xml` file under the `handlers` (Handler is a plugin) tag, add this XML snippet:

```xml
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

### 5. Execute a synchronization

You can execute a synchronization of classes, clusters or even the output of a command:
- classes
- clusters
- command

Example synchronizing the class "V":
```
curl -u admin:admin --data "{'classes':['V']}" http://localhost:2480/essync/GamesOfThrones
```
