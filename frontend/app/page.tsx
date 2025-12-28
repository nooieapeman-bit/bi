"use client";

import React, { useState, useEffect } from "react";
import { Schema, Table, Column } from "./types";
import { Save, Database, Plus, Trash2, ArrowRight, LayoutDashboard, Table as TableIcon } from "lucide-react";

const API_BASE = "http://localhost:8000/api";

export default function Home() {
  const [schema, setSchema] = useState<Schema>({ dimensions: [], facts: [] });
  const [selectedTable, setSelectedTable] = useState<Table | null>(null);
  const [selectedTableType, setSelectedTableType] = useState<"dimension" | "fact" | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [notification, setNotification] = useState<{ msg: string, type: 'success' | 'error' } | null>(null);

  useEffect(() => {
    fetchSchema();
  }, []);

  const fetchSchema = async () => {
    try {
      const res = await fetch(`${API_BASE}/schema`);
      const data = await res.json();
      setSchema(data);
    } catch (err) {
      console.error(err);
      showNotification("Failed to load schema", "error");
    } finally {
      setLoading(false);
    }
  };

  const saveSchema = async () => {
    setSaving(true);
    try {
      await fetch(`${API_BASE}/schema`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(schema),
      });
      showNotification("Schema saved!", "success");
    } catch (err) {
      showNotification("Failed to save schema", "error");
    } finally {
      setSaving(false);
    }
  };

  const applySchema = async () => {
    try {
      const res = await fetch(`${API_BASE}/apply-schema`, { method: "POST" });
      const data = await res.json();
      if (data.status === 'success') {
        showNotification("Changes applied to Database!", "success");
      } else {
        showNotification("Failed to apply changes", "error");
      }
    } catch (err) {
      showNotification("Failed to apply changes", "error");
    }
  };

  const showNotification = (msg: string, type: 'success' | 'error') => {
    setNotification({ msg, type });
    setTimeout(() => setNotification(null), 3000);
  };

  // -- Event Handlers --

  const handleTableClick = (table: Table, type: "dimension" | "fact") => {
    setSelectedTable(table);
    setSelectedTableType(type);
  };

  const updateSelectedTable = (updated: Table) => {
    if (!selectedTableType) return;

    const listKey = selectedTableType === "dimension" ? "dimensions" : "facts";
    const newList = schema[listKey].map(t => t.name === selectedTable?.name ? updated : t);

    setSchema({ ...schema, [listKey]: newList });
    setSelectedTable(updated);
  };

  const addColumn = () => {
    if (!selectedTable) return;
    const newCol: Column = { name: "new_column", type: "TEXT" };
    updateSelectedTable({ ...selectedTable, columns: [...selectedTable.columns, newCol] });
  };

  const deleteColumn = (idx: number) => {
    if (!selectedTable) return;
    const newCols = [...selectedTable.columns];
    newCols.splice(idx, 1);
    updateSelectedTable({ ...selectedTable, columns: newCols });
  };

  return (
    <div className="flex h-screen bg-gray-50 text-gray-900 font-sans">
      {/* Sidebar */}
      <div className="w-64 bg-white border-r border-gray-200 flex flex-col">
        <div className="p-4 border-b border-gray-100">
          <h1 className="text-xl font-bold bg-gradient-to-r from-blue-600 to-indigo-600 bg-clip-text text-transparent">BI Designer</h1>
        </div>

        <div className="flex-1 overflow-y-auto p-4 space-y-6">
          {/* Dimensions */}
          <div>
            <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Dimensions</h2>
            <div className="space-y-1">
              {schema.dimensions.map(t => (
                <button
                  key={t.name}
                  onClick={() => handleTableClick(t, "dimension")}
                  className={`w-full text-left px-3 py-2 rounded-md text-sm transition-colors flex items-center space-x-2 
                    ${selectedTable?.name === t.name ? 'bg-blue-50 text-blue-700 font-medium' : 'hover:bg-gray-100 text-gray-600'}
                  `}
                >
                  <LayoutDashboard size={14} />
                  <span>{t.name}</span>
                </button>
              ))}
            </div>
          </div>

          {/* Facts */}
          <div>
            <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">Fact Tables</h2>
            <div className="space-y-1">
              {schema.facts.map(t => (
                <button
                  key={t.name}
                  onClick={() => handleTableClick(t, "fact")}
                  className={`w-full text-left px-3 py-2 rounded-md text-sm transition-colors flex items-center space-x-2 
                    ${selectedTable?.name === t.name ? 'bg-indigo-50 text-indigo-700 font-medium' : 'hover:bg-gray-100 text-gray-600'}
                  `}
                >
                  <TableIcon size={14} />
                  <span>{t.name}</span>
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col h-full overflow-hidden">
        {/* Header */}
        <header className="h-16 bg-white border-b border-gray-200 flex items-center justify-between px-8">
          <h2 className="text-lg font-medium text-gray-800">
            {selectedTable ? `Editing: ${selectedTable.name}` : "Select a table to edit"}
          </h2>
          <div className="flex items-center space-x-4">
            <button
              onClick={saveSchema}
              disabled={saving}
              className="px-4 py-2 bg-white border border-gray-300 text-gray-700 rounded-lg text-sm font-medium hover:bg-gray-50 flex items-center shadow-sm"
            >
              <Save size={16} className="mr-2" />
              {saving ? "Saving..." : "Save Draft"}
            </button>
            <button
              onClick={applySchema}
              className="px-4 py-2 bg-indigo-600 text-white rounded-lg text-sm font-medium hover:bg-indigo-700 flex items-center shadow-md transition-all active:scale-95"
            >
              <Database size={16} className="mr-2" />
              Apply to DB
            </button>
          </div>
        </header>

        {/* Editor Area */}
        <main className="flex-1 overflow-y-auto p-8">
          {selectedTable ? (
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 max-w-4xl mx-auto">
              <div className="flex items-center justify-between mb-6">
                <div>
                  <label className="block text-sm font-medium text-gray-500 mb-1">Table Name</label>
                  <input
                    value={selectedTable.name}
                    onChange={e => {
                      const newName = e.target.value;
                      updateSelectedTable({ ...selectedTable, name: newName });
                    }}
                    className="text-2xl font-bold bg-transparent border-b-2 border-transparent focus:border-indigo-500 focus:outline-none placeholder-gray-300 w-full"
                  />
                </div>
                <div className="text-xs px-2 py-1 bg-gray-100 rounded text-gray-500">
                  {selectedTableType === 'dimension' ? 'Dimension' : 'Fact Table'}
                </div>
              </div>

              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-semibold text-gray-900">Columns</h3>
                  <button onClick={addColumn} className="text-indigo-600 text-sm font-medium hover:text-indigo-800 flex items-center">
                    <Plus size={14} className="mr-1" /> Add Column
                  </button>
                </div>

                <div className="border border-gray-200 rounded-lg overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50 text-gray-500 font-medium">
                      <tr>
                        <th className="px-4 py-3 text-left w-1/3">Name</th>
                        <th className="px-4 py-3 text-left w-1/4">Type</th>
                        <th className="px-4 py-3 text-center">PK</th>
                        <th className="px-4 py-3 text-left w-1/4">Relation (FK)</th>
                        <th className="px-4 py-3 w-10"></th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {selectedTable.columns.map((col, idx) => (
                        <tr key={idx} className="group hover:bg-gray-50">
                          <td className="px-4 py-2">
                            <input
                              value={col.name}
                              onChange={e => {
                                const newCols = [...selectedTable.columns];
                                newCols[idx].name = e.target.value;
                                updateSelectedTable({ ...selectedTable, columns: newCols });
                              }}
                              className="w-full bg-transparent focus:outline-none font-medium text-gray-800"
                            />
                          </td>
                          <td className="px-4 py-2">
                            <select
                              value={col.type}
                              onChange={e => {
                                const newCols = [...selectedTable.columns];
                                newCols[idx].type = e.target.value;
                                updateSelectedTable({ ...selectedTable, columns: newCols });
                              }}
                              className="w-full bg-transparent focus:outline-none text-gray-600"
                            >
                              <option value="INTEGER">INTEGER</option>
                              <option value="TEXT">TEXT</option>
                              <option value="REAL">REAL</option>
                              <option value="BOOLEAN">BOOLEAN</option>
                            </select>
                          </td>
                          <td className="px-4 py-2 text-center">
                            <input
                              type="checkbox"
                              checked={col.primary_key || false}
                              onChange={e => {
                                const newCols = [...selectedTable.columns];
                                newCols[idx].primary_key = e.target.checked;
                                updateSelectedTable({ ...selectedTable, columns: newCols });
                              }}
                              className="rounded border-gray-300 text-indigo-600 focus:ring-indigo-500"
                            />
                          </td>
                          <td className="px-4 py-2">
                            <input
                              value={col.foreign_key || ""}
                              placeholder="-"
                              onChange={e => {
                                const newCols = [...selectedTable.columns];
                                newCols[idx].foreign_key = e.target.value;
                                updateSelectedTable({ ...selectedTable, columns: newCols });
                              }}
                              className="w-full bg-transparent focus:outline-none text-gray-500 text-xs"
                            />
                          </td>
                          <td className="px-4 py-2 text-center">
                            <button onClick={() => deleteColumn(idx)} className="text-gray-300 hover:text-red-500 opacity-0 group-hover:opacity-100 transition-opacity">
                              <Trash2 size={16} />
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          ) : (
            <div className="h-full flex flex-col items-center justify-center text-gray-400">
              <LayoutDashboard size={48} className="mb-4 text-gray-200" />
              <p>Select a dimension or fact table to start editing.</p>
            </div>
          )}
        </main>
      </div>

      {notification && (
        <div className={`fixed bottom-4 right-4 px-6 py-3 rounded-lg shadow-lg text-white ${notification.type === 'success' ? 'bg-green-600' : 'bg-red-600'} animate-fade-in-up`}>
          {notification.msg}
        </div>
      )}
    </div>
  );
}
