export interface Column {
    name: string;
    type: string;
    primary_key?: boolean;
    foreign_key?: string | null;
    description?: string | null;
}

export interface Table {
    name: string;
    columns: Column[];
    description?: string | null;
}

export interface Schema {
    dimensions: Table[];
    facts: Table[];
}

export interface ReportFilter {
    column: string;
    label: string;
    name?: string;
    dimension_table?: string;
    dimension_label_col?: string;
}

export interface Report {
    id: string;
    title: string;
    description: string;
    chart_type: string;
    source_table: string;
    measures: { column: string, aggregation: string, label: string }[];
    x_axis: { column: string, label: string, type: string, granularity_options: string[] };
    filters: ReportFilter[];
}
