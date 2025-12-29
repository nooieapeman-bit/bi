export interface Column {
    name: string;
    type: string;
    primary_key?: boolean;
    foreign_key?: string;
    description?: string;
}

export interface Table {
    name: string;
    columns: Column[];
    description?: string;
}

export interface Schema {
    dimensions: Table[];
    facts: Table[];
}
