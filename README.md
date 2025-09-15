# flight-sql-client

A TypeScript/JavaScript client for Apache Arrow Flight and Flight SQL protocols.

## Installation

```bash
npm install @firetiger-oss/flight-sql-client
```

## Quick Start

```typescript
import { FlightSQLClient } from "@firetiger-oss/flight-sql-client";

const client = new FlightSQLClient({
  host: "localhost",
  port: 4317,
  plaintext: true,
  username: "your-username",
  password: "your-password",
});

// This 'table' has type Arrow.dom.Table
// https://arrow.apache.org/js/current/classes/Arrow.dom.Table.html
const table = await client.executeQuery("SELECT count(*) AS n FROM logs");
// Use the 'toArray()' method to convert the table to an array of row objects, for example:
// [ {"n": 3} ]
console.log(table.toArray());

await client.close();
```

## Dependencies

- `@grpc/grpc-js` - gRPC implementation
- `apache-arrow` - Arrow data format support
