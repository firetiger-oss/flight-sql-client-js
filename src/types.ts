export interface FlightClientConfig {
  host: string;
  port: number;
  plaintext?: boolean;
  username?: string;
  password?: string;
  token?: string;
}

export type FlightSQLClientConfig = FlightClientConfig;

export interface PreparedStatement {
  handle: Uint8Array;
  parameterSchema?: any;
  resultSchema?: any;
}

export interface FlightInfo {
  endpoint: string;
  ticket: Uint8Array;
  totalRecords: number;
  totalBytes: number;
}

export interface DatabaseMetadata {
  catalogs: string[];
  schemas: Array<{ catalog: string; schema: string }>;
  tables: Array<{
    catalog: string;
    schema: string;
    tableName: string;
    tableType: string;
  }>;
}

export interface TableMetadata {
  primaryKeys: Array<{
    catalogName: string;
    schemaName: string;
    tableName: string;
    columnName: string;
    keySequence: number;
  }>;
  foreignKeys: Array<{
    pkCatalogName: string;
    pkSchemaName: string;
    pkTableName: string;
    pkColumnName: string;
    fkCatalogName: string;
    fkSchemaName: string;
    fkTableName: string;
    fkColumnName: string;
  }>;
}