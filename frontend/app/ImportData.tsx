'use client';

import React, { useState, useEffect } from 'react';
import { ArrowRight, Play, Database, Table as TableIcon, RefreshCw } from 'lucide-react';

interface Schema {
    dimensions: any[];
    facts: any[];
}

export default function ImportData() {
    const [osaioTables, setOsaioTables] = useState<string[]>([]);
    const [biTables, setBiTables] = useState<string[]>([]);
    const [schema, setSchema] = useState<Schema | null>(null);

    const [selectedSourceTable, setSelectedSourceTable] = useState('');
    const [selectedTargetTable, setSelectedTargetTable] = useState('');

    const [sourceColumns, setSourceColumns] = useState<string[]>([]);
    const [targetColumns, setTargetColumns] = useState<any[]>([]);

    const [mappings, setMappings] = useState<{ [key: string]: string }>({});
    const [loading, setLoading] = useState(false);
    const [message, setMessage] = useState('');
    const [apiBase, setApiBase] = useState('http://localhost:8000/api');

    useEffect(() => {
        setApiBase(`http://${window.location.hostname}:8000/api`);
    }, []);

    useEffect(() => {
        fetchOsaioTables();
        fetchBiSchema();
    }, [apiBase]);

    useEffect(() => {
        if (selectedSourceTable) {
            fetchSourceColumns(selectedSourceTable);
        }
    }, [selectedSourceTable, apiBase]);

    useEffect(() => {
        if (selectedTargetTable && schema) {
            const table = [...schema.dimensions, ...schema.facts].find(t => t.name === selectedTargetTable);
            if (table) {
                setTargetColumns(table.columns);
                // Auto-map if names match
                const newMappings: { [key: string]: string } = {};
                table.columns.forEach((col: any) => {
                    // Simple heuristic: if source has exactly same name
                    if (sourceColumns.includes(col.name)) {
                        newMappings[col.name] = col.name;
                    } else {
                        newMappings[col.name] = '';
                    }
                });
                setMappings(newMappings);
            }
        }
    }, [selectedTargetTable, schema, sourceColumns]);

    const fetchOsaioTables = async () => {
        try {
            const res = await fetch(`${apiBase}/osaio/tables`);
            const data = await res.json();
            if (data.tables) setOsaioTables(data.tables);
        } catch (e) {
            console.error("Failed to load source tables", e);
        }
    };

    const fetchBiSchema = async () => {
        try {
            const res = await fetch(`${apiBase}/schema`);
            const data = await res.json();
            if (data.dimensions) {
                setSchema(data);
                const tables = [
                    ...data.dimensions.map((t: any) => t.name),
                    ...data.facts.map((t: any) => t.name)
                ];
                setBiTables(tables);
            }
        } catch (e) {
            console.error("Failed to load BI schema", e);
        }
    };

    const fetchSourceColumns = async (table: string) => {
        try {
            const res = await fetch(`${apiBase}/osaio/columns/${table}`);
            const data = await res.json();
            if (data.columns) setSourceColumns(data.columns);
        } catch (e) {
            console.error("Failed to load source cols", e);
        }
    };

    const handleMappingChange = (targetCol: string, val: string) => {
        setMappings(prev => ({ ...prev, [targetCol]: val }));
    };

    const handleExecute = async () => {
        setLoading(true);
        setMessage('');

        // Filter out empty mappings
        const activeMappings = Object.entries(mappings)
            .filter(([_, sourceExpr]) => sourceExpr && sourceExpr.trim() !== '')
            .map(([targetCol, sourceExpr]) => ({
                target_column: targetCol,
                source_expression: sourceExpr
            }));

        if (activeMappings.length === 0) {
            setMessage('Error: No columns mapped');
            setLoading(false);
            return;
        }

        try {
            const payload = {
                source_table: selectedSourceTable,
                target_table: selectedTargetTable,
                mappings: activeMappings,
                truncate_target: false // Optional: could be a checkbox
            };

            const res = await fetch(`${apiBase}/etl/execute`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });

            const result = await res.json();
            if (res.ok) {
                setMessage('Success: ' + result.message);
            } else {
                setMessage('Error: ' + result.detail);
            }
        } catch (e) {
            setMessage('Error: Execution failed');
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="p-6 max-w-6xl mx-auto space-y-8">
            <div className="flex items-center space-x-4 mb-8">
                <div className="p-3 bg-blue-100 rounded-lg">
                    <Database className="w-8 h-8 text-blue-600" />
                </div>
                <div>
                    <h1 className="text-2xl font-bold text-gray-900">Data Import (ETL)</h1>
                    <p className="text-gray-500">Migrate data from OSAIO database to BI Warehouse</p>
                </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
                {/* Source Selection */}
                <div className="p-6 bg-white border rounded-xl shadow-sm space-y-4">
                    <h2 className="font-semibold flex items-center gap-2">
                        <div className="w-6 h-6 rounded bg-orange-100 flex items-center justify-center text-orange-600 text-xs">S</div>
                        Source (OSAIO)
                    </h2>
                    <select
                        className="w-full p-2 border rounded-md"
                        value={selectedSourceTable}
                        onChange={e => setSelectedSourceTable(e.target.value)}
                    >
                        <option value="">Select Table...</option>
                        {osaioTables.map(t => (
                            <option key={t} value={t}>{t}</option>
                        ))}
                    </select>
                    {selectedSourceTable && (
                        <div className="text-xs text-gray-500">
                            {sourceColumns.length} columns available
                        </div>
                    )}
                </div>

                {/* Arrow */}
                <div className="flex items-center justify-center md:pt-12">
                    <ArrowRight className="w-8 h-8 text-gray-300" />
                </div>

                {/* Target Selection */}
                <div className="p-6 bg-white border rounded-xl shadow-sm space-y-4">
                    <h2 className="font-semibold flex items-center gap-2">
                        <div className="w-6 h-6 rounded bg-blue-100 flex items-center justify-center text-blue-600 text-xs">T</div>
                        Target (BI Data)
                    </h2>
                    <select
                        className="w-full p-2 border rounded-md"
                        value={selectedTargetTable}
                        onChange={e => setSelectedTargetTable(e.target.value)}
                    >
                        <option value="">Select Table...</option>
                        {biTables.map(t => (
                            <option key={t} value={t}>{t}</option>
                        ))}
                    </select>
                </div>
            </div>

            {/* Mapping Section */}
            {selectedSourceTable && selectedTargetTable && (
                <div className="bg-white border rounded-xl shadow-sm overflow-hidden">
                    <div className="p-4 border-b bg-gray-50 flex justify-between items-center">
                        <h3 className="font-medium text-gray-700">Column Mapping</h3>
                        <div className="text-sm text-gray-500">
                            Map destination columns to source expressions (SQL supported)
                        </div>
                    </div>

                    <div className="overflow-x-auto">
                        <table className="w-full text-sm text-left">
                            <thead className="text-xs text-gray-700 uppercase bg-gray-50">
                                <tr>
                                    <th className="px-6 py-3">Target Column</th>
                                    <th className="px-6 py-3">Type</th>
                                    <th className="px-6 py-3 w-1/2">Source Expression</th>
                                </tr>
                            </thead>
                            <tbody>
                                {targetColumns.map(col => (
                                    <tr key={col.name} className="bg-white border-b hover:bg-gray-50">
                                        <td className="px-6 py-4 font-medium text-gray-900">
                                            {col.name}
                                            {col.primary_key && <span className="ml-2 text-xs text-yellow-600 bg-yellow-100 px-1 rounded">PK</span>}
                                        </td>
                                        <td className="px-6 py-4 text-gray-500 font-mono text-xs">{col.type}</td>
                                        <td className="px-6 py-4">
                                            <div className="relative">
                                                <input
                                                    type="text"
                                                    list={`source-cols-${col.name}`}
                                                    className="w-full p-2 border rounded focus:ring-2 focus:ring-blue-500 outline-none font-mono text-sm"
                                                    placeholder="Col, SQL, 'Fixed', or UUID()..."
                                                    value={mappings[col.name] || ''}
                                                    onChange={e => handleMappingChange(col.name, e.target.value)}
                                                />
                                                <datalist id={`source-cols-${col.name}`}>
                                                    <option value="UUID()" label="Generate Unique ID" />
                                                    <option value="NOW()" label="Current Time" />
                                                    <option value="NULL" label="Empty / Null" />
                                                    {sourceColumns.map(sc => (
                                                        <option key={sc} value={sc} />
                                                    ))}
                                                </datalist>
                                            </div>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>

                    <div className="p-4 bg-gray-50 border-t flex justify-between items-center">
                        <div className={`text-sm ${message.startsWith('Error') ? 'text-red-600' : 'text-green-600'}`}>
                            {message}
                        </div>
                        <button
                            onClick={handleExecute}
                            disabled={loading}
                            className={`flex items-center gap-2 px-6 py-2 rounded-lg text-white font-medium 
                        ${loading ? 'bg-gray-400 cursor-not-allowed' : 'bg-blue-600 hover:bg-blue-700 shadow-sm'}`}
                        >
                            {loading ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                            Execute Import
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}
