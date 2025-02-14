# Python DAS gRPC Server

This is a DAS gRPC Python service that implements a mock DAS server.

## Quickstart

1. **Install requirements**:

```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

2. **Fetch `.proto` files**:

```bash
make fetch
```

3. **Generate gRPC stubs**:

```bash
make build
```

4. **Run the service**:

```bash
python -m das_mock.main
```

To use it from DAS PostgreSQL client (available at [https://github.com/raw-labs/das-client-postgresql](https://github.com/raw-labs/das-client-postgresql)), you can setup this DAS by running, e.g.:

```bash
% psql -h 127.0.0.1 -p 65432 -U postgres
```

And then issuing the following SQL commands to setup the integration:

```sql
DROP SERVER IF EXISTS das_python_mock CASCADE;
DROP SCHEMA IF EXISTS test CASCADE;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS multicorn;
CREATE SERVER das_python_mock FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'host.docker.internal:50051', -- So that Docker is able to reach outside in your host, to this DAS gRPC server running at 50051
  das_type 'mock',
  my_option_a 'my_value_a', -- Appears as key 'my_option_a' in the options dictionary in DASMock.__init__
  my_option_b 'my_value_b'  -- Appears as key 'my_option_b' in the options dictionary in DASMock.__init__
);
CREATE SCHEMA test;
IMPORT FOREIGN SCHEMA test FROM SERVER das_python_mock INTO test;
```

... and then querying it as, e.g.:

```sql
postgres=# SELECT * FROM test.small_table;

 id |     name     
----+--------------
  1 | Mock row #1
  2 | Mock row #2
  3 | Mock row #3
  4 | Mock row #4
  5 | Mock row #5
  6 | Mock row #6
  7 | Mock row #7
  8 | Mock row #8
  9 | Mock row #9
 10 | Mock row #10
(10 rows)
```

5. **Test**:

```bash
pytest tests
```
