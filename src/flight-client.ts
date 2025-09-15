import * as grpc from '@grpc/grpc-js';
import {Schema, Table, tableFromIPC} from 'apache-arrow';
import { FlightServiceClient } from './generated/proto/Flight_grpc_pb';
import {
  FlightDescriptor,
  FlightInfo,
  Ticket,
  FlightData,
  HandshakeRequest,
  Action,
  ActionType,
  Result,
  Criteria,
  Empty
} from './generated/proto/Flight_pb';
import { FlightClientConfig } from './types';
import { ConnectionError, AuthenticationError, FlightError } from './errors';
import { validateConfig, parseErrorFromGrpc } from './utils';

export class FlightClient {
  private client: FlightServiceClient | null = null;
  private config: FlightClientConfig;
  private readonly credentials: grpc.ChannelCredentials;
  private readonly metadata: grpc.Metadata;

  constructor(config: FlightClientConfig) {
    validateConfig(config);
    this.config = { plaintext: false, ...config };
    this.credentials = this.createCredentials();
    this.metadata = this.createMetadata();
  }

  private createCredentials(): grpc.ChannelCredentials {
    return this.config.plaintext
      ? grpc.credentials.createInsecure()
      : grpc.credentials.createSsl();
  }

  private createMetadata(): grpc.Metadata {
    const metadata = new grpc.Metadata();
    if (this.config.token) {
      metadata.add("authorization", `Bearer ${this.config.token}`)
    } else if (this.config.username && this.config.password) {
      const credential = Buffer.from(`${this.config.username}:${this.config.password}`).toString('base64');
      metadata.add("authorization", `Basic ${credential}`);
    }

    return metadata;
  }

  async connect(): Promise<void> {
    try {
      const address = `${this.config.host}:${this.config.port}`;

      // Configure gRPC options for large message handling
      const options = {
        'grpc.max_receive_message_length': 100 * 1024 * 1024, // 100MB
        'grpc.max_send_message_length': 100 * 1024 * 1024,    // 100MB
        'grpc.keepalive_time_ms': 30000,
        'grpc.keepalive_timeout_ms': 5000,
        'grpc.keepalive_permit_without_calls': 1,
        'grpc.http2.max_pings_without_data': 0,
        'grpc.http2.min_ping_interval_without_data_ms': 300000,
      };

      this.client = new FlightServiceClient(address, this.credentials, options);

      if (this.config.token || (this.config.username && this.config.password)) {
        await this.authenticate();
      }
    } catch (error) {
      throw new ConnectionError(`Failed to connect to ${this.config.host}:${this.config.port}`, error as Error);
    }
  }

  private async authenticate(): Promise<void> {
    if (!this.client) {
      throw new ConnectionError('Client not connected');
    }

    return new Promise((resolve, reject) => {
      const call = this.client!.handshake(this.metadata);

      const handshakeRequest = new HandshakeRequest();
      call.write(handshakeRequest);

      call.on('data', () => {
        resolve();
      });

      call.on('error', (error: any) => {
        reject(new AuthenticationError(parseErrorFromGrpc(error).message));
      });

      call.end();
    });
  }

  async getFlightInfo(descriptor: FlightDescriptor): Promise<FlightInfo> {
    if (!this.client) {
      await this.connect();
    }

    return new Promise((resolve, reject) => {
      this.client!.getFlightInfo(descriptor, this.metadata, (error: any, response: FlightInfo) => {
        if (error) {
          reject(parseErrorFromGrpc(error));
        } else {
          resolve(response);
        }
      });
    });
  }

  async doGet(ticket: Ticket): Promise<Table> {
    if (!this.client) {
      await this.connect();
    }

    return new Promise((resolve, reject) => {
      const STREAM_TIMEOUT_MS = 60000; // 60 second deadline

      const options = {
        deadline: Date.now() + STREAM_TIMEOUT_MS,
      };

      const call = this.client!.doGet(ticket, this.metadata, options);

      // Set up flow control for streaming
      call.pause();
      call.resume();

      const flightDataMessages: Array<{header?: Uint8Array, body?: Uint8Array}> = [];

      call.on('data', (flightData: FlightData) => {
        const dataHeader = flightData.getDataHeader_asU8();
        const dataBody = flightData.getDataBody_asU8();

        // Store each FlightData message as a complete unit for Arrow IPC reconstruction
        flightDataMessages.push({
          header: dataHeader,
          body: dataBody
        });
      });

      call.on('error', (error: any) => {
        reject(parseErrorFromGrpc(error));
      });

      call.on('end', () => {
        try {
          const result = this.processFlightDataMessages(flightDataMessages);
          resolve(result);
        } catch (error) {
          reject(new FlightError(`Failed to parse Arrow data: ${error}`));
        }
      });
    });
  }

  /**
   * Processes FlightData messages and reconstructs them into Arrow IPC format.
   * According to Apache Arrow Flight spec, FlightData contains FlatBuffer messages
   * that need to be converted to proper Arrow IPC format for parsing.
   */
  private processFlightDataMessages(messages: Array<{header?: Uint8Array, body?: Uint8Array}>): Table {
    if (messages.length === 0) {
      return new Table();
    }

    const ipcParts = this.convertFlightDataToIPC(messages);

    return tableFromIPC(ipcParts);
  }

  /**
   * Converts FlightData messages to Arrow IPC format.
   * Each FlightData message contains a header (FlatBuffer) and optional body data.
   */
  private convertFlightDataToIPC(messages: Array<{header?: Uint8Array, body?: Uint8Array}>): Uint8Array[] {
    const ipcParts: Uint8Array[] = [];

    for (const message of messages) {
      const { header, body } = message;

      if (!header || header.length === 0) {
        continue;
      }

      const ipcMessage = this.createIPCMessage(header, body);
      ipcParts.push(ipcMessage);
    }

    return ipcParts;
  }

  /**
   * Creates a single Arrow IPC message from FlightData header and body.
   * Format: [4 bytes continuation] [4 bytes message length] [message] [padding] [body]
   */
  private createIPCMessage(header: Uint8Array, body?: Uint8Array): Uint8Array {
    const messageLength = header.length;
    const bodyLength = body?.length || 0;
    const paddingLength = this.calculatePadding(messageLength);
    const totalLength = 4 + 4 + messageLength + paddingLength + bodyLength;

    const ipcMessage = new Uint8Array(totalLength);
    let offset = 0;

    // Write continuation indicator (0xFFFFFFFF for valid message)
    offset = this.writeUint32(ipcMessage, offset, 0xFFFFFFFF);

    // Write message length
    offset = this.writeUint32(ipcMessage, offset, messageLength);

    // Write FlatBuffer message (header)
    ipcMessage.set(header, offset);
    offset += messageLength;

    // Skip padding (already zero-filled)
    offset += paddingLength;

    // Write body data if present
    if (body && body.length > 0) {
      ipcMessage.set(body, offset);
    }

    return ipcMessage;
  }

  /**
   * Calculates padding needed to align to 8-byte boundary.
   */
  private calculatePadding(messageLength: number): number {
    return (8 - (messageLength % 8)) % 8;
  }

  /**
   * Writes a 32-bit unsigned integer in little-endian format.
   */
  private writeUint32(buffer: Uint8Array, offset: number, value: number): number {
    const view = new DataView(buffer.buffer, offset, 4);
    view.setUint32(0, value, true); // little endian
    return offset + 4;
  }

  async doPut(stream: AsyncIterable<FlightData>): Promise<void> {
    if (!this.client) {
      await this.connect();
    }

    return new Promise((resolve, reject) => {
      const call = this.client!.doPut(this.metadata);

      call.on('error', (error: any) => {
        reject(parseErrorFromGrpc(error));
      });

      call.on('end', () => {
        resolve();
      });

      (async () => {
        try {
          for await (const data of stream) {
            call.write(data);
          }
          call.end();
        } catch (error) {
          reject(new FlightError(`Failed to write data: ${error}`));
        }
      })();
    });
  }

  async doAction(action: Action): Promise<Result[]> {
    if (!this.client) {
      await this.connect();
    }

    return new Promise((resolve, reject) => {
      const call = this.client!.doAction(action, this.metadata);
      const results: Result[] = [];

      call.on('data', (result: Result) => {
        results.push(result);
      });

      call.on('error', (error: any) => {
        reject(parseErrorFromGrpc(error));
      });

      call.on('end', () => {
        resolve(results);
      });
    });
  }

  async listFlights(criteria?: Criteria): Promise<FlightInfo[]> {
    if (!this.client) {
      await this.connect();
    }

    const criteriaToUse = criteria || new Criteria();

    return new Promise((resolve, reject) => {
      const call = this.client!.listFlights(criteriaToUse, this.metadata);
      const flights: FlightInfo[] = [];

      call.on('data', (flightInfo: FlightInfo) => {
        flights.push(flightInfo);
      });

      call.on('error', (error: any) => {
        reject(parseErrorFromGrpc(error));
      });

      call.on('end', () => {
        resolve(flights);
      });
    });
  }

  async getSchema(descriptor: FlightDescriptor): Promise<Schema> {
    if (!this.client) {
      await this.connect();
    }

    return new Promise((resolve, reject) => {
      this.client!.getSchema(descriptor, this.metadata, (error: any, response: any) => {
        if (error) {
          reject(parseErrorFromGrpc(error));
        } else {
          try {
            const schemaBytes = response.getSchema_asU8();
            const schema = Schema.decode(schemaBytes);
            resolve(schema);
          } catch (parseError) {
            reject(new FlightError(`Failed to parse schema: ${parseError}`));
          }
        }
      });
    });
  }

  async listActions(): Promise<ActionType[]> {
    if (!this.client) {
      await this.connect();
    }

    const empty = new Empty();

    return new Promise((resolve, reject) => {
      const call = this.client!.listActions(empty, this.metadata);
      const actions: ActionType[] = [];

      call.on('data', (actionType: ActionType) => {
        actions.push(actionType);
      });

      call.on('error', (error: any) => {
        reject(parseErrorFromGrpc(error));
      });

      call.on('end', () => {
        resolve(actions);
      });
    });
  }

  async close(): Promise<void> {
    if (this.client) {
      this.client.close();
      this.client = null;
    }
  }
}