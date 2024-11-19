import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { CreateTableCommandOutput, DescribeTableCommandOutput } from '@aws-sdk/client-dynamodb';
type Dict<T = any> = {
    [key: string]: T;
};
type TableSchema = {
    partition: string;
    sort: string;
};
type TableIndex = TableSchema & {
    name: string;
    type: 'S' | 'N';
};
type SharedOptions = {
    attributeNames?: Dict<string>;
    attributeValues?: Dict<string | number>;
    filterExpression?: string;
    index?: string;
    prefix?: boolean;
};
declare const concatConditionExpression: (exp1: string, exp2: string) => string;
declare const concatUpdateExpression: (exp1: string, exp2: string) => string;
declare class Dynamodb {
    client: DynamoDBDocumentClient;
    indexes: TableIndex[];
    schema: TableSchema;
    table: string;
    constructor(opts?: {
        accessKeyId: string;
        indexes?: TableIndex[];
        region: string;
        schema: TableSchema;
        secretAccessKey: string;
        table: string;
    });
    batchWrite(items: Dict[]): Promise<Dict[]>;
    batchDelete(item: Dict, opts?: SharedOptions & {
        expression?: string;
    }): Promise<Dict[]>;
    delete(item: Dict, opts?: SharedOptions & {
        conditionExpression?: string;
    }): Promise<Dict | null>;
    fetch(item: Dict, opts?: SharedOptions & {
        all?: boolean;
        expression?: string;
        onChunk?: ({ count, items }: {
            count: number;
            items: Dict[];
        }) => Promise<void> | void;
        limit?: number;
        startKey?: Dict<string> | null;
    }): Promise<{
        count: number;
        items: Dict[];
        lastEvaluatedKey: Dict<string> | null;
    }>;
    get(item: Dict, opts?: SharedOptions): Promise<Dict | null>;
    put(item: Dict, opts?: {
        attributeNames?: Dict<string>;
        attributeValues?: Dict<string | number>;
        conditionExpression?: string;
        overwrite: boolean;
    }): Promise<Dict>;
    optimisticResolveSchema(item: Dict): {
        index: string;
        schema: TableSchema;
    };
    update(item: Dict, opts?: SharedOptions & {
        allowUpdatePartitionAndSort?: boolean;
        conditionExpression?: string;
        expression?: string;
        updateFn?: (item: Dict) => Dict | null;
        upsert?: boolean;
    }): Promise<Dict>;
    private updateWithTransaction;
    createTable(): Promise<DescribeTableCommandOutput | CreateTableCommandOutput>;
}
export { concatConditionExpression, concatUpdateExpression };
export default Dynamodb;
