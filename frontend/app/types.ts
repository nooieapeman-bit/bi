export interface Column {
    name: string;
    type: string;
    primary_key?: boolean;
    foreign_key?: string;
}

export interface Table {
    name: string;
    columns: Column[];
}

export interface Schema {
    dimensions: Table[];
    facts: Table[];
}
