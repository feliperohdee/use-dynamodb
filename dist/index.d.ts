import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { CreateTableCommandOutput, DescribeTableCommandOutput } from '@aws-sdk/client-dynamodb';
type Dict<T = any> = {
    [key: string]: T;
};
type TableSchema = {
    partition: string;
    sort: string;
};
type TableIndex = {
    name: string;
    partition: string;
    sort?: string;
    type: 'S' | 'N';
};
type SharedOptions = {
    attributeNames?: Dict<string>;
    attributeValues?: Dict<string | number>;
    filterExpression?: string;
    index?: string;
    prefix?: boolean;
    select?: string[];
};
declare const SCAN_OPTS: {
    all?: boolean;
    attributeNames?: Dict<string>;
    attributeValues?: Dict<string | number>;
    consistentRead?: boolean;
    filterExpression?: string;
    index?: string;
    limit?: number;
    onChunk?: ({ count, items }: {
        count: number;
        items: Dict[];
    }) => Promise<void> | void;
    select?: string[];
    startKey?: Dict<string> | null;
};
declare const concatConditionExpression: (exp1: string, exp2: string) => string;
declare const concatUpdateExpression: (exp1: string, exp2: string) => string;
declare class Dynamodb {
    client: DynamoDBDocumentClient;
    private indexes;
    private schema;
    private table;
    private timestamps;
    constructor(opts?: {
        accessKeyId: string;
        indexes?: TableIndex[];
        region: string;
        schema: TableSchema;
        secretAccessKey: string;
        table: string;
        timestamps?: {
            createdAtField?: string;
            updatedAtField?: string;
        };
    });
    batchWrite<T = Dict>(items: Dict[]): Promise<T[]>;
    batchDelete<T = Dict>(item: Dict, opts?: SharedOptions & {
        expression?: string;
    }): Promise<T[]>;
    delete<T = Dict>(item: Dict, opts?: SharedOptions & {
        conditionExpression?: string;
    }): Promise<T | null>;
    fetch<T = Dict>(item: Dict, opts?: SharedOptions & {
        all?: boolean;
        expression?: string;
        consistentRead?: boolean;
        onChunk?: ({ count, items }: {
            count: number;
            items: Dict[];
        }) => Promise<void> | void;
        limit?: number;
        startKey?: Dict<string> | null;
    }): Promise<{
        count: number;
        items: T[];
        lastEvaluatedKey: Dict<string> | null;
    }>;
    scan<T = Dict>(opts?: {
        all?: boolean;
        attributeNames?: Dict<string>;
        attributeValues?: Dict<string | number>;
        consistentRead?: boolean;
        filterExpression?: string;
        index?: string;
        limit?: number;
        onChunk?: ({ count, items }: {
            count: number;
            items: Dict[];
        }) => Promise<void> | void;
        select?: string[];
        startKey?: Dict<string> | null;
    }): Promise<{
        count: number;
        items: T[];
        lastEvaluatedKey: Dict<string> | null;
    }>;
    get<T = Dict>(item: Dict, opts?: SharedOptions & {
        consistentRead?: boolean;
    }): Promise<T | null>;
    put<T = Dict>(item: Dict, opts?: {
        attributeNames?: Dict<string>;
        attributeValues?: Dict<string | number>;
        conditionExpression?: string;
        overwrite: boolean;
    }): Promise<T>;
    optimisticResolveSchema(item: Dict): {
        index: string;
        schema: TableSchema;
    };
    update<T = Dict>(item: Dict, opts?: SharedOptions & {
        allowUpdatePartitionAndSort?: boolean;
        conditionExpression?: string;
        expression?: string;
        updateFn?: (item: Dict) => Dict | null;
        upsert?: boolean;
    }): Promise<T>;
    private updateWithTransaction;
    createTable(): Promise<DescribeTableCommandOutput | CreateTableCommandOutput>;
}
export { concatConditionExpression, concatUpdateExpression, SCAN_OPTS };
export default Dynamodb;
