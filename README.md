# Kafka → Snowflake Lab

A hands-on activity that streams events from a local Apache Kafka cluster (Docker) into a Snowflake table using the **Snowflake Connector for Kafka v4** (Snowpipe Streaming).

```
producer.py ──► Kafka topic "orders" ──► Kafka Connect + Snowflake sink ──► SF_KAFKA_DB.STREAMING.ORDERS_RAW
```

**Status:** end-to-end pipeline verified. A test record sent to the `orders` topic appeared in Snowflake almost instantly.

## Versions used

| Component | Version |
|---|---|
| Kafka / Kafka Connect | Confluent Platform `7.8.2` (KRaft mode, no ZooKeeper) |
| Snowflake Kafka connector | `4.2.0` |
| Bouncy Castle FIPS (required by the connector) | `bc-fips 2.1.0`, `bcpkix-fips 2.1.8` |
| Kafka UI | `provectuslabs/kafka-ui:latest` |

## Prerequisites

- Docker Desktop
- A Snowflake account (trial is fine) with `ACCOUNTADMIN` access for the one-time setup
- OpenSSL (included with Git Bash on Windows)
- Python 3 with `pip` (for the producer)
- A bash shell. The commands below were run in **Git Bash**. PowerShell and `cmd` will not run the `cat > file << EOF` examples.

## Repository layout

```
kafka-snowflake-lab/
├── docker-compose.yml
├── connector.example.json   # committed template (placeholder key)
├── connector.json           # your real config (gitignored)
├── producer.py
├── .gitignore
├── keys/                    # RSA key pair (gitignored)
└── plugins/
    └── snowflake-kafka-connector/   # 3 JAR files (gitignored)
```

`.gitignore`:

```
plugins/
keys/
*.p8
*.pem
connector.json
```

---

## Step 1: Start Kafka with Docker Compose

Create `docker-compose.yml` (the file name must be exactly `docker-compose.yml` or `compose.yml`):

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.8.2
    hostname: kafka
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
      KAFKA_LISTENERS: PLAINTEXT://kafka:29092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk

  connect:
    image: confluentinc/cp-kafka-connect:7.8.2
    hostname: connect
    container_name: connect
    depends_on:
      - kafka
    ports:
      - "8083:8083"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka:29092
      CONNECT_REST_ADVERTISED_HOST_NAME: connect
      CONNECT_REST_PORT: 8083
      CONNECT_GROUP_ID: connect-cluster
      CONNECT_CONFIG_STORAGE_TOPIC: _connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: _connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: _connect-status
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
      CONNECT_PLUGIN_PATH: /usr/share/java,/usr/share/confluent-hub-components,/plugins
    volumes:
      - ./plugins:/plugins

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
      - connect
    ports:
      - "8081:8080"   # host 8081 -> container 8080 (8080 was taken by another lab)
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME: connect
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: http://connect:8083
```

What each service does:

- **kafka**: a single broker in KRaft mode.
- **connect**: Kafka Connect, the runtime that hosts the Snowflake sink connector. It mounts `./plugins` so connector JARs can be dropped in.
- **kafka-ui**: a browser dashboard for topics, messages and connectors, at http://localhost:8081.

Start it:

```bash
docker compose up -d
docker compose ps
```

Smoke test:

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic orders --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

## Step 2: Generate an RSA key pair

The connector authenticates to Snowflake with key-pair authentication only.

```bash
mkdir keys
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out keys/rsa_key.p8 -nocrypt
openssl rsa -in keys/rsa_key.p8 -pubout -out keys/rsa_key.pub
```

Print the public key as one line without the header and footer:

```bash
grep -v "KEY" keys/rsa_key.pub | tr -d '\r\n'
```

The private key (`rsa_key.p8`) never leaves your machine.

## Step 3: Set up Snowflake

Run in a worksheet. This lab uses these objects:

| Object | Name |
|---|---|
| Database | `SF_KAFKA_DB` |
| Schema | `STREAMING` |
| Warehouse | `SF_KAFKA_WH` |
| Role | `SF_KAFKA_SYSADMIN` |
| Service user | `KAF_SVC_ACC` |
| Target table | `SF_KAFKA_DB.STREAMING.ORDERS_RAW` |

```sql
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS SF_KAFKA_DB;
CREATE SCHEMA   IF NOT EXISTS SF_KAFKA_DB.STREAMING;
CREATE WAREHOUSE IF NOT EXISTS SF_KAFKA_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- Role for the connector
CREATE ROLE IF NOT EXISTS SF_KAFKA_SYSADMIN;
GRANT ROLE SF_KAFKA_SYSADMIN TO ROLE SYSADMIN;

-- Service user (key-pair auth only, no password)
CREATE USER IF NOT EXISTS KAF_SVC_ACC
  TYPE = SERVICE
  DEFAULT_ROLE = SF_KAFKA_SYSADMIN
  DEFAULT_WAREHOUSE = SF_KAFKA_WH;
GRANT ROLE SF_KAFKA_SYSADMIN TO USER KAF_SVC_ACC;
GRANT ROLE SF_KAFKA_SYSADMIN TO USER <YOUR_SNOWFLAKE_USERNAME>;  -- so you can query the data

-- Register the PUBLIC key (body only, no BEGIN/END lines)
ALTER USER KAF_SVC_ACC SET RSA_PUBLIC_KEY = '<PUBLIC_KEY_BODY>';

-- Privileges
GRANT OWNERSHIP ON DATABASE SF_KAFKA_DB TO ROLE SF_KAFKA_SYSADMIN COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA SF_KAFKA_DB.STREAMING TO ROLE SF_KAFKA_SYSADMIN COPY CURRENT GRANTS;
GRANT USAGE ON WAREHOUSE SF_KAFKA_WH TO ROLE SF_KAFKA_SYSADMIN;

-- Target table (columns match the JSON fields we send)
CREATE TABLE IF NOT EXISTS SF_KAFKA_DB.STREAMING.ORDERS_RAW (
  ORDER_ID   STRING,
  CUSTOMER   STRING,
  AMOUNT     NUMBER(10,2),
  STATUS     STRING,
  CREATED_AT TIMESTAMP_NTZ
);

-- Account identifier for the connector URL
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS account_identifier;
```

Verify:

```sql
DESC USER KAF_SVC_ACC;                                  -- RSA_PUBLIC_KEY_FP should have a value
SHOW GRANTS ON TABLE SF_KAFKA_DB.STREAMING.ORDERS_RAW;  -- connector role must own it or have INSERT/SELECT
```

If the table is owned by a different role (for example `ACCOUNTADMIN`, because it was created after the ownership grants), fix it:

```sql
GRANT OWNERSHIP ON TABLE SF_KAFKA_DB.STREAMING.ORDERS_RAW
  TO ROLE SF_KAFKA_SYSADMIN COPY CURRENT GRANTS;
```

## Step 4: Install the connector

Cloning the GitHub repository does **not** work. That gives you source code, and Kafka Connect needs compiled JARs. Download these three JARs from Maven Central into `plugins/snowflake-kafka-connector/`:

```bash
mkdir -p plugins/snowflake-kafka-connector
cd plugins/snowflake-kafka-connector

curl -fLO https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/4.2.0/snowflake-kafka-connector-4.2.0.jar
curl -fLO https://repo1.maven.org/maven2/org/bouncycastle/bc-fips/2.1.0/bc-fips-2.1.0.jar
curl -fLO https://repo1.maven.org/maven2/org/bouncycastle/bcpkix-fips/2.1.8/bcpkix-fips-2.1.8.jar

cd ../..
ls -lh plugins/snowflake-kafka-connector
```

Expected sizes: the connector JAR is about 169 MB, bc-fips about 4 MB, bcpkix-fips about 1 MB. A file of a few KB is an error page, so re-download it.

Restart Connect and confirm the plugin loaded:

```bash
docker compose restart connect
# wait 30-60 seconds
curl -s http://localhost:8083/connector-plugins | grep -i snowflake
```

You should see `SnowflakeStreamingSinkConnector`.

## Step 5: Register the connector

Create `connector.json` (keep `connector.example.json` in git with the placeholder key):

```json
{
  "name": "snowflake-orders-sink",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector",
    "tasks.max": "1",
    "topics": "orders",
    "snowflake.topic2table.map": "orders:ORDERS_RAW",
    "snowflake.url.name": "<ORG-ACCOUNT>.snowflakecomputing.com",
    "snowflake.user.name": "KAF_SVC_ACC",
    "snowflake.private.key": "<PRIVATE_KEY_BODY_ONE_LINE>",
    "snowflake.role.name": "SF_KAFKA_SYSADMIN",
    "snowflake.warehouse.name": "SF_KAFKA_WH",
    "snowflake.database.name": "SF_KAFKA_DB",
    "snowflake.schema.name": "STREAMING",
    "snowflake.streaming.validate.compatibility.with.classic": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

Insert your private key as a single line with no header or footer:

```bash
KEY=$(grep -v "KEY" keys/rsa_key.p8 | tr -d '\r\n')
sed -i "s|<PRIVATE_KEY_BODY_ONE_LINE>|$KEY|" connector.json
```

Register it, and check the status:

```bash
curl -X POST -H "Content-Type: application/json" --data @connector.json http://localhost:8083/connectors > /dev/null
curl -s http://localhost:8083/connectors/snowflake-orders-sink/status
```

Both the connector and its task should report `RUNNING`.

> **Security note:** the Kafka Connect REST API echoes the full connector config, **including the private key**, in its response. That is why the register command above discards the output (`> /dev/null`). Never paste that response anywhere. A key like this should be rotated if it was exposed. In this lab the key only authenticates a throwaway service user.

## Step 6: Send a test record

```bash
docker exec -it kafka kafka-console-producer --bootstrap-server kafka:29092 --topic orders
```

Paste one line, then press Ctrl+C:

```json
{"order_id":"A1001","customer":"alice","amount":42.50,"status":"NEW","created_at":"2026-10-02 10:00:00"}
```

Query the table:

```sql
SELECT * FROM SF_KAFKA_DB.STREAMING.ORDERS_RAW;
```

**Result:** the row arrived almost instantly.

## Step 7: Stream live data with the Python producer

```bash
pip install confluent-kafka
python producer.py --rate 2        # use `py` if `python` isn't found in Git Bash
```

Options: `--rate` (orders per second), `--count` (stop after N), `--topic`, `--bootstrap`. While it runs, watch the table grow:

```sql
SELECT COUNT(*), MAX(CREATED_AT) FROM SF_KAFKA_DB.STREAMING.ORDERS_RAW;
```

## Step 8: Experiments

Record your own results in the blanks.

### A. Pause and resume the connector

Shows that Kafka stores messages and the connector tracks its own position (offset).

```bash
curl -X PUT http://localhost:8083/connectors/snowflake-orders-sink/pause
python producer.py --count 50 --rate 20
# Snowflake count should NOT grow
curl -X PUT http://localhost:8083/connectors/snowflake-orders-sink/resume
# Snowflake count should now grow by 50
```

Observed: _______________________

### B. Extra field in a message

Send `{"order_id":"X1","customer":"zed","amount":10,"status":"NEW","created_at":"2026-10-02 12:00:00","coupon":"SAVE10"}` and check whether the row lands and what happens to `coupon`.

Observed: _______________________

### C. Malformed JSON

Send `this is not json` and check the connector status.

Observed: _______________________

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `no configuration file provided: not found` | Not in the project folder, or the compose file is misnamed (for example `docker_compose.yml` with an underscore) | `cd` into the project folder and rename to `docker-compose.yml`, or use `docker compose -f <file>` |
| `unrecognized token cat` | Heredoc (`cat > file << EOF`) run in a non-bash shell | Use Git Bash, or create the file in an editor |
| `failed to copy ... EOF` while pulling images | Network dropped mid-download | Re-run `docker compose up -d`; completed layers are cached. Pull images individually if needed |
| Connector plugin missing after restart | Cloned source repo instead of JARs, or JARs in the wrong folder | Use the three JARs from Step 4 and keep only JARs in `plugins/snowflake-kafka-connector/` |
| Connector `FAILED`: `snowflake.streaming.validate.compatibility.with.classic is enabled but ...` | v4 runs a migration-compatibility check meant for v3 upgraders | Add `"snowflake.streaming.validate.compatibility.with.classic": "false"` for a fresh install |
| Kafka UI not reachable on 8080 | Port already used by another stack (here, an Airflow lab) | Remap to `"8081:8080"` and use http://localhost:8081 |
| Port still taken after `docker pause` | Pausing freezes a container but keeps its port binding | Use `docker compose stop` (or `down`) to release ports |
| No rows in Snowflake | Table owner or grants wrong; key mismatch | Check `SHOW GRANTS ON TABLE ...`, confirm `RSA_PUBLIC_KEY_FP` is set, read `docker logs connect --tail 50` |

## Useful commands

```bash
docker compose ps                                  # container status
docker logs connect --tail 50                      # Connect logs
curl -s http://localhost:8083/connectors           # list connectors
curl -s http://localhost:8083/connectors/snowflake-orders-sink/status
curl -X DELETE http://localhost:8083/connectors/snowflake-orders-sink
docker compose stop                                # stop, keep data, free ports
docker compose down                                # remove containers and network
```

## Cleanup

```bash
docker compose down
```

```sql
DROP DATABASE IF EXISTS SF_KAFKA_DB;
DROP WAREHOUSE IF EXISTS SF_KAFKA_WH;
DROP USER IF EXISTS KAF_SVC_ACC;
DROP ROLE IF EXISTS SF_KAFKA_SYSADMIN;
```

## References

- Snowflake Connector for Kafka documentation (Snowflake docs)
- Connector releases: `snowflakedb/snowflake-kafka-connector` on GitHub
- Maven Central: `com.snowflake:snowflake-kafka-connector`
