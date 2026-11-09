// Copyright Turing Biosystems Ltd
// SPDX-License-Identifier: Apache-2.0

export declare const ColumnType: Readonly<{
    UINT64: 0;
    INT64: 1;
    DOUBLE: 2;
    STRING: 3;
    BOOL: 4;
    EMBEDDING: 5;
    LIST_VIEW: 6;
    VALUE_TYPE: 7;
    CELL_VIEW: 8;
    NODE_ID: 9;
    EDGE_ID: 10;
    EDGE_TYPE_ID: 11;
    PROPERTY_TYPE_ID: 12;
    LABEL_ID: 13;
    LABEL_SET_ID: 14;
    COMMIT_HASH: 15;
    CHANGE_ID: 16;
    PROPERTY_NULL: 17;
    MAP_VIEW: 18;
    DATE_TIME: 19;
    DURATION: 20;
    GRAPH_PATH: 21;
    POINT: 22;
}>;

export type ColumnTypeName = keyof typeof ColumnType;
export type ColumnTypeCode = (typeof ColumnType)[ColumnTypeName];

export declare const ColumnEncoding: Readonly<{
    VECTOR: 0;
    OPTIONAL_VECTOR: 1;
    CONSTANT: 2;
    OPTIONAL_CONSTANT: 3;
}>;

export type ColumnEncodingCode = (typeof ColumnEncoding)[keyof typeof ColumnEncoding];

/** A point a query returns. z is undefined for a 2D point. */
export declare class Point {
    private constructor();
    readonly srid: 7203 | 9157 | 4326 | 4979;
    readonly x: number;
    readonly y: number;
    readonly z: number | undefined;
    readonly crs: "cartesian" | "cartesian-3d" | "wgs-84" | "wgs-84-3d";
}

/** A row value as Column.get and Column.toArray return it. A path is its node IDs. */
export type Value =
    | bigint
    | number
    | boolean
    | string
    | null
    | Float32Array
    | Point
    | Value[]
    | { [key: string]: Value };

/** A row value as Column.toNumberArray and TuringClient.queryData return it. */
export type NumberValue =
    | number
    | boolean
    | string
    | null
    | Float32Array
    | Point
    | NumberValue[]
    | { [key: string]: NumberValue };

/** A number sent as a Double whatever its value. */
export declare class DoubleValue {
    constructor(value: number);
    readonly value: number;
}

export declare function double(value: number): DoubleValue;

/**
 * A query parameter. A number with no fraction goes as an Int64 and any other number as a
 * Double. A bigint goes as an Int64, double(x) as a Double, and a Float32Array as an
 * Embedding.
 */
export type ParameterValue =
    | bigint
    | number
    | boolean
    | string
    | null
    | DoubleValue
    | Float32Array
    | ParameterValue[]
    | { [key: string]: ParameterValue };

export type Parameters = { [name: string]: ParameterValue };

export type QueryStatus =
    | "GRAPH_NOT_FOUND"
    | "PARSE_ERROR"
    | "ANALYZE_ERROR"
    | "PLAN_ERROR"
    | "EXEC_ERROR"
    | "COMMIT_NOT_FOUND"
    | "COMMIT_NOT_LOADED"
    | "CHANGE_NOT_FOUND"
    | "HTTP_ERROR"
    | "PROTOCOL_ERROR"
    | "DECODE_ERROR"
    | "PARAMETER_ERROR";

export declare class TuringQueryError extends Error {
    constructor(status: string, message: string);
    readonly name: "TuringQueryError";
    /** A QueryStatus name, or `STATUS_<n>` for a status code this client does not know. */
    status: QueryStatus | (string & {});
}

export declare class Column {
    private constructor();

    readonly name: string;
    readonly typeCode: ColumnTypeCode;
    readonly encoding: ColumnEncodingCode;
    readonly length: number;
    readonly isConstant: boolean;
    readonly isOptional: boolean;
    /**
     * Fixed-width vector columns as their typed array, one element per row. Null for
     * constants, variable-width kinds and points. A missing optional row holds 0: read validity.
     */
    readonly values:
        | BigUint64Array
        | BigInt64Array
        | Float64Array
        | Uint32Array
        | Uint16Array
        | Uint8Array
        | null;
    /** One bit per row, LSB first, set where the row has a value. Null when every row has one. */
    readonly validity: Uint8Array | null;

    /** Undefined for a row past the end of the column. */
    get(row: number): Value | undefined;
    toArray(): Value[];
    /** 64-bit integers and IDs as Number: past 2^53 the precision is lost. */
    toNumberArray(): NumberValue[];
}

export declare class NestedReader {
    private constructor();

    readListAt(offset: number): Value[];
    readElementAt(offset: number): Value;
    readMapAt(offset: number): { [key: string]: Value };
}

export interface Dataframe {
    names: string[];
    columns: Column[];
}

export interface QueryResult {
    chunks: Dataframe[];
    /** Null when the server sent no execution time. */
    execTimeMs: number | null;
}

/** "main" and "head" select the tip. A change ID or a commit hash selects that version. */
export type VersionRef = "main" | "head" | number | bigint | string;

export interface QueryOptions {
    /** Binds `$name` to `parameters.name`. */
    parameters?: Parameters;
    graph?: string;
    change?: VersionRef;
    commit?: VersionRef;
    signal?: AbortSignal;
}

export interface TuringClientOptions {
    /** The query endpoint. Default `/query`. */
    url?: string;
    /** Default `default`. */
    graph?: string;
    /** Sent as `Authorization: Bearer <authToken>`. */
    authToken?: string;
    /** The decoder buffer's starting size in bytes. It grows to fit a larger packet. Default 1 MiB. */
    bufferCapacity?: number;
    fetch?: typeof fetch;
    /** Where to fetch `turing_wasm_decoder.wasm` from, in place of the copy next to the client. */
    wasmUrl?: string;
}

export declare class TuringClient {
    constructor(options?: TuringClientOptions);

    setGraph(name: string): void;
    getGraph(): string;
    setChange(change: VersionRef): void;
    setCommit(commit: VersionRef): void;
    setAuthToken(token: string | null): void;

    /** Rejects with TuringQueryError when the query fails. */
    query(cypher: string, options?: QueryOptions): Promise<QueryResult>;
    /**
     * Chunks by columns by rows, with 64-bit integers as Number. A result with no rows is
     * one chunk of empty columns.
     */
    queryData(cypher: string, options?: QueryOptions): Promise<NumberValue[][][]>;
}
