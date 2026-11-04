import { isDeepStrictEqual } from "node:util";

import { ColumnType, TuringClient, TuringQueryError } from "turingdb";
import type { ColumnTypeName, QueryOptions, Value } from "turingdb";

export type Row = Value[];

export interface QueryRows {
    names: string[];
    types: ColumnTypeName[];
    rows: Row[];
    chunkCount: number;
}

export interface ExpectedError {
    status: string;
    message: string;
}

// A query and what it must answer. Rows compare as a multiset unless ordered is set.
export interface Case {
    name: string;
    query: string;
    options?: QueryOptions;
    names?: string[];
    types?: ColumnTypeName[];
    rows?: Row[];
    ordered?: boolean;
    error?: ExpectedError;
    check?: (result: QueryRows) => void;
}

// A test of the client itself, driving it directly
export interface Check {
    name: string;
    run: (client: TuringClient) => Promise<void>;
}

const TYPE_NAMES = new Map<number, ColumnTypeName>(Object.entries(ColumnType).map(([name, code]) => [code, name as ColumnTypeName]));

function typeName(code: number): ColumnTypeName {
    const name = TYPE_NAMES.get(code);
    if (name === undefined) {
        throw new Error(`unknown column type ${code}`);
    }

    return name;
}

export function at(iso: string): bigint {
    return BigInt(Date.parse(iso)) * 1000n;
}

export function minutes(count: number): bigint {
    return BigInt(count) * 60_000_000n;
}

export function embedding(...values: number[]): Float32Array {
    return new Float32Array(values);
}

// Renders a decoded value so an Int64 (480) reads apart from a Double (480.0)
function describeValue(value: Value | undefined): string {
    if (value === undefined) {
        return "undefined";
    } else if (value === null) {
        return "null";
    } else if (typeof value === "bigint") {
        return value.toString();
    } else if (typeof value === "number") {
        return Number.isInteger(value) ? value.toFixed(1) : value.toString();
    } else if (typeof value === "string") {
        return JSON.stringify(value);
    } else if (typeof value === "boolean") {
        return value.toString();
    } else if (value instanceof Float32Array) {
        return `embedding(${Array.from(value).join(", ")})`;
    } else if (Array.isArray(value)) {
        return `[${value.map(describeValue).join(", ")}]`;
    }

    const entries = Object.entries(value).sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
    return `{${entries.map(([key, entry]) => `${key}: ${describeValue(entry)}`).join(", ")}}`;
}

function describeRows(rows: Row[]): string {
    if (rows.length === 0) {
        return "    (no row)";
    }

    return rows.map((row) => `    ${row.map(describeValue).join(" | ")}`).join("\n");
}

function sortRows(rows: Row[]): Row[] {
    const keyed = rows.map((row) => ({ key: describeValue(row), row }));
    keyed.sort((left, right) => (left.key < right.key ? -1 : left.key > right.key ? 1 : 0));
    return keyed.map(({ row }) => row);
}

function indent(text: string): string {
    return text.split("\n").map((line) => `      ${line}`).join("\n");
}

function describeError(raised: unknown): string {
    if (raised instanceof TuringQueryError) {
        return `${raised.status}: ${raised.message}`;
    } else if (raised instanceof Error) {
        return raised.message;
    }

    return String(raised);
}

export class CaseRunner {
    private readonly _client: TuringClient;
    private _passedCount = 0;
    private readonly _failures: string[] = [];

    constructor(client: TuringClient) {
        this._client = client;
    }

    async query(query: string, options: QueryOptions = {}): Promise<QueryRows> {
        const { chunks } = await this._client.query(query, options);

        const firstChunk = chunks[0];
        const names = firstChunk ? firstChunk.names : [];
        const types = firstChunk ? firstChunk.columns.map((column) => typeName(column.typeCode)) : [];

        const rows: Row[] = [];
        for (const { columns } of chunks) {
            const columnValues = columns.map((column) => column.toArray());
            const rowCount = columns.length > 0 ? columns[0].length : 0;

            for (let row = 0; row < rowCount; row++) {
                rows.push(columnValues.map((values) => values[row]));
            }
        }

        return { names, types, rows, chunkCount: chunks.length };
    }

    // Runs a setup statement, which must succeed for the rest of the test to mean anything
    async execute(query: string, options: QueryOptions = {}): Promise<QueryRows> {
        try {
            return await this.query(query, options);
        } catch (raised) {
            throw new Error(`setup query failed: ${query}\n${describeError(raised)}`);
        }
    }

    async run(section: string, cases: Case[]): Promise<void> {
        await this.runChecks(section, cases.map((testCase) => ({
            name: testCase.name,
            run: () => this._runCase(testCase),
        })));
    }

    async runChecks(section: string, checks: Check[]): Promise<void> {
        console.log(`== ${section}`);

        for (const check of checks) {
            try {
                await check.run(this._client);
                this._passedCount++;
                console.log(`  ok   ${check.name}`);
            } catch (raised) {
                const message = describeError(raised);
                this._failures.push(`${section} / ${check.name}\n${indent(message)}`);
                console.log(`  FAIL ${check.name}\n${indent(message)}`);
            }
        }
    }

    // Prints the summary and tells whether every case passed
    report(): boolean {
        console.log("");
        console.log(`passed: ${this._passedCount}, failed: ${this._failures.length}`);

        for (const failure of this._failures) {
            console.log(`FAILED ${failure}`);
        }

        return this._failures.length === 0;
    }

    private async _runCase(testCase: Case): Promise<void> {
        if (testCase.error !== undefined) {
            await this._expectError(testCase.query, testCase.options, testCase.error);
            return;
        }

        const result = await this.query(testCase.query, testCase.options);

        if (testCase.names !== undefined && !isDeepStrictEqual(result.names, testCase.names)) {
            throw new Error(`query: ${testCase.query}\nexpected columns: ${testCase.names.join(", ")}\ngot columns:      ${result.names.join(", ")}`);
        }

        if (testCase.types !== undefined && !isDeepStrictEqual(result.types, testCase.types)) {
            throw new Error(`query: ${testCase.query}\nexpected types: ${testCase.types.join(", ")}\ngot types:      ${result.types.join(", ")}`);
        }

        if (testCase.rows !== undefined) {
            const actual = testCase.ordered ? result.rows : sortRows(result.rows);
            const expected = testCase.ordered ? testCase.rows : sortRows(testCase.rows);

            if (!isDeepStrictEqual(actual, expected)) {
                throw new Error(`query: ${testCase.query}\nexpected:\n${describeRows(expected)}\ngot:\n${describeRows(actual)}`);
            }
        }

        if (testCase.check !== undefined) {
            testCase.check(result);
        }
    }

    private async _expectError(query: string, options: QueryOptions | undefined, expected: ExpectedError): Promise<void> {
        try {
            await this._client.query(query, options);
        } catch (raised) {
            if (!(raised instanceof TuringQueryError)) {
                throw raised;
            }

            const matches = raised.status === expected.status && raised.message.includes(expected.message);
            if (!matches) {
                throw new Error(`query: ${query}\nexpected ${expected.status} with "${expected.message}"\ngot ${raised.status}: ${raised.message}`);
            }

            return;
        }

        throw new Error(`query: ${query}\nexpected ${expected.status} with "${expected.message}", but the query succeeded`);
    }
}
