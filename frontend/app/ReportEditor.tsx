"use client";

import React, { useState, useEffect } from "react";
import { X, Save, Database, Sigma } from "lucide-react";

interface ReportEditorProps {
    report?: any;
    onSave: (report: any) => void;
    onCancel: () => void;
    tables: any[]; // Table schemas
}

export default function ReportEditor({ report, onSave, onCancel, tables }: ReportEditorProps) {
    const [formData, setFormData] = useState({
        id: report?.id || `report_${Date.now()}`,
        category: report?.category || "finance",
        title: report?.title || "",
        description: report?.description || "",
        source_table: report?.source_table || "",
        joins: report?.joins || [],
        group_by: report?.group_by || "",
        measure_formula: report?.measure_formula || "",
        chart_type: report?.chart_type || "line",
        slices: report?.slices || [],
        image: report?.image || ""
    });

    const [columns, setColumns] = useState<any[]>([]);

    const handleAddJoin = () => {
        setFormData({
            ...formData,
            joins: [...formData.joins, { table: "", join_type: "LEFT", on_expression: "" }]
        });
    };

    const handleRemoveJoin = (idx: number) => {
        const newJoins = [...formData.joins];
        newJoins.splice(idx, 1);
        setFormData({ ...formData, joins: newJoins });
    };

    const handleJoinChange = (idx: number, field: string, val: string) => {
        const newJoins = [...formData.joins];
        newJoins[idx] = { ...newJoins[idx], [field]: val };
        setFormData({ ...formData, joins: newJoins });
    };

    const handleSlicesChange = (val: string) => {
        setFormData({ ...formData, slices: val.split(",").map(s => s.trim()) });
    };



    useEffect(() => {
        if (formData.source_table) {
            const table = tables.find(t => t.name === formData.source_table);
            if (table) {
                setColumns(table.columns);
            }
        }
    }, [formData.source_table, tables]);

    const handleSubmit = () => {
        // Construct standard report object
        const newReport = {
            ...formData,
            // Construct necessary internal fields for backend compatibility
            measures: [{ column: "calculated", label: "Value" }], // simplified
            x_axis: { column: formData.group_by, label: formData.group_by, type: "category", granularity_options: [] },
            filters: []
        };
        onSave(newReport);
    };

    return (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
            <div className="bg-white rounded-xl shadow-2xl w-full max-w-2xl overflow-hidden flex flex-col max-h-[90vh]">
                <div className="px-6 py-4 border-b border-gray-100 flex justify-between items-center bg-gray-50">
                    <h3 className="font-bold text-lg text-gray-800">
                        {report ? "编辑图表 (Edit Report)" : "新建图表 (New Report)"}
                    </h3>
                    <button onClick={onCancel} className="text-gray-400 hover:text-gray-600">
                        <X size={20} />
                    </button>
                </div>

                <div className="p-6 space-y-6 overflow-y-auto flex-1">
                    {/* Basic Info */}
                    <div className="grid grid-cols-2 gap-4">
                        <div>
                            <label className="block text-xs font-semibold text-gray-500 mb-1">标题 (Title)</label>
                            <input
                                className="w-full border border-gray-200 rounded p-2 text-sm focus:ring-1 focus:ring-indigo-500 outline-none"
                                value={formData.title}
                                onChange={e => setFormData({ ...formData, title: e.target.value })}
                                placeholder="e.g. Monthly Revenue"
                            />
                        </div>
                        <div>
                            <label className="block text-xs font-semibold text-gray-500 mb-1">分类 (Category)</label>
                            <select
                                className="w-full border border-gray-200 rounded p-2 text-sm outline-none"
                                value={formData.category}
                                onChange={e => setFormData({ ...formData, category: e.target.value })}
                            >
                                <option value="finance">财务 (Finance)</option>
                                <option value="user">用户 (User)</option>
                                <option value="device">设备 (Device)</option>
                            </select>
                        </div>
                    </div>

                    <div>
                        <label className="block text-xs font-semibold text-gray-500 mb-1">描述 (Description)</label>
                        <textarea
                            className="w-full border border-gray-200 rounded p-2 text-sm h-20 outline-none resize-none"
                            value={formData.description}
                            onChange={e => setFormData({ ...formData, description: e.target.value })}
                            placeholder="Describe what this report shows..."
                        />
                    </div>

                    <div className="h-px bg-gray-100 my-2"></div>

                    {/* Data Configuration */}
                    <div className="space-y-4">
                        <h4 className="flex items-center font-bold text-gray-800 text-sm">
                            <Database size={16} className="mr-2 text-indigo-600" />
                            数据配置 (Data Configuration)
                        </h4>

                        <div className="grid grid-cols-2 gap-4">
                            <div>
                                <label className="block text-xs font-semibold text-gray-500 mb-1">主表 (Main Table)</label>
                                <select
                                    className="w-full border border-gray-200 rounded p-2 text-sm outline-none bg-indigo-50/50"
                                    value={formData.source_table}
                                    onChange={e => setFormData({ ...formData, source_table: e.target.value })}
                                >
                                    <option value="">-- Select Table --</option>
                                    {tables.map(t => (
                                        <option key={t.name} value={t.name}>{t.name}</option>
                                    ))}
                                </select>
                            </div>

                            <div>
                                <label className="block text-xs font-semibold text-gray-500 mb-1">分组字段 (Group By)</label>
                                <select
                                    className="w-full border border-gray-200 rounded p-2 text-sm outline-none disabled:bg-gray-100"
                                    value={formData.group_by}
                                    onChange={e => setFormData({ ...formData, group_by: e.target.value })}
                                    disabled={!columns.length}
                                >
                                    <option value="">-- Select Column --</option>
                                    {columns.map(c => (
                                        <option key={c.name} value={c.name}>{c.name}</option>
                                    ))}
                                </select>
                            </div>
                        </div>

                        <div>
                            <label className="block text-xs font-semibold text-gray-500 mb-1 flex items-center">
                                <Sigma size={14} className="mr-1" />
                                计算公式 (Measure Formula)
                            </label>
                            <input
                                className="w-full border border-gray-200 rounded p-2 text-sm font-mono text-indigo-700 bg-gray-50 focus:bg-white transition-colors outline-none"
                                value={formData.measure_formula}
                                onChange={e => setFormData({ ...formData, measure_formula: e.target.value })}
                                placeholder="e.g. SUM(amount) or COUNT(distinct user_id)"
                            />
                            <p className="text-[10px] text-gray-400 mt-1">SQL Aggregation syntax allowed.</p>
                        </div>

                        <div className="h-px bg-gray-100 my-2"></div>

                        {/* Joins Configuration */}
                        <div className="space-y-3">
                            <div className="flex justify-between items-center">
                                <h4 className="font-bold text-gray-800 text-sm">关联表 (Joins)</h4>
                                <button onClick={handleAddJoin} className="text-xs text-indigo-600 hover:text-indigo-800 font-medium">+ Add Join</button>
                            </div>

                            {formData.joins.map((join, idx) => (
                                <div key={idx} className="bg-gray-50 p-3 rounded border border-gray-100 relative group">
                                    <button
                                        onClick={() => handleRemoveJoin(idx)}
                                        className="absolute top-2 right-2 text-gray-400 hover:text-red-500 opacity-0 group-hover:opacity-100"
                                    >
                                        <X size={14} />
                                    </button>
                                    <div className="grid grid-cols-3 gap-2 mb-2">
                                        <select
                                            className="text-xs border rounded p-1"
                                            value={join.join_type}
                                            onChange={e => handleJoinChange(idx, 'join_type', e.target.value)}
                                        >
                                            <option value="LEFT">LEFT JOIN</option>
                                            <option value="INNER">INNER JOIN</option>
                                        </select>
                                        <select
                                            className="text-xs border rounded p-1 col-span-2"
                                            value={join.table}
                                            onChange={e => handleJoinChange(idx, 'table', e.target.value)}
                                        >
                                            <option value="">-- Table --</option>
                                            {tables.map(t => <option key={t.name} value={t.name}>{t.name}</option>)}
                                        </select>
                                    </div>
                                    <input
                                        className="w-full text-xs border rounded p-1 font-mono"
                                        placeholder="ON condition (e.g. T1.id = T2.id)"
                                        value={join.on_expression}
                                        onChange={e => handleJoinChange(idx, 'on_expression', e.target.value)}
                                    />
                                </div>
                            ))}
                            {formData.joins.length === 0 && <p className="text-xs text-gray-400 italic">No joins configured.</p>}
                        </div>

                        <div className="h-px bg-gray-100 my-2"></div>

                        {/* Slices (Filters) */}
                        <div>
                            <h4 className="font-bold text-gray-800 text-sm mb-2">筛选切片 (Slices)</h4>

                            {/* Selected Slices Tags */}
                            <div className="flex flex-wrap gap-2 mb-2">
                                {formData.slices.map((slice, idx) => (
                                    <span key={idx} className="inline-flex items-center px-2 py-1 rounded bg-indigo-50 text-indigo-700 text-xs border border-indigo-100">
                                        {slice}
                                        <button
                                            onClick={() => {
                                                const newSlices = [...formData.slices];
                                                newSlices.splice(idx, 1);
                                                setFormData({ ...formData, slices: newSlices });
                                            }}
                                            className="ml-1 text-indigo-400 hover:text-indigo-600"
                                        >
                                            <X size={12} />
                                        </button>
                                    </span>
                                ))}
                            </div>

                            {/* Add Slice Dropdown */}
                            <div className="flex gap-2">
                                <select
                                    className="flex-1 border border-gray-200 rounded p-2 text-sm outline-none"
                                    onChange={e => {
                                        if (e.target.value && !formData.slices.includes(e.target.value)) {
                                            setFormData({ ...formData, slices: [...formData.slices, e.target.value] });
                                        }
                                        e.target.value = ""; // Reset
                                    }}
                                >
                                    <option value="">+ Add Slice Filter...</option>
                                    {/* Main Table Columns */}
                                    <optgroup label={`Main: ${formData.source_table}`}>
                                        {columns.map(c => (
                                            <option key={c.name} value={c.name}>{c.name}</option>
                                        ))}
                                    </optgroup>
                                    {/* Joined Tables Columns */}
                                    {formData.joins.map((j, jIdx) => {
                                        const jTable = tables.find(t => t.name === j.table);
                                        if (!jTable) return null;
                                        return (
                                            <optgroup key={jIdx} label={`Joined: ${j.table}`}>
                                                {jTable.columns.map((c: any) => (
                                                    <option key={`${j.table}.${c.name}`} value={c.name}>{c.name} ({j.table})</option>
                                                ))}
                                            </optgroup>
                                        );
                                    })}
                                </select>
                            </div>
                            <p className="text-[10px] text-gray-400 mt-1">Select fields from main or joined tables to allow filtering.</p>
                        </div>
                    </div>
                </div>

                <div className="p-4 border-t border-gray-100 bg-gray-50 flex justify-end space-x-3">
                    <button onClick={onCancel} className="px-4 py-2 rounded-lg text-sm text-gray-600 hover:bg-gray-100 font-medium">Cancel</button>
                    <button
                        onClick={handleSubmit}
                        className="px-4 py-2 rounded-lg text-sm bg-indigo-600 text-white shadow-sm hover:bg-indigo-700 font-medium flex items-center"
                    >
                        <Save size={16} className="mr-2" />
                        Save Configuration
                    </button>
                </div>
            </div>
        </div>
    );
}
