"use client";

import React, { useState, useEffect } from "react";
import { TrendingUp, Users, Smartphone, Plus, Edit2, Trash2, LayoutDashboard, Copy } from "lucide-react";
import ReportEditor from "./ReportEditor";

// Icon mapping
const ICONS: any = {
    finance: <TrendingUp className="text-emerald-600" size={24} />,
    user: <Users className="text-blue-600" size={24} />,
    device: <Smartphone className="text-purple-600" size={24} />
};

const CATEGORY_NAMES: any = {
    finance: "财务营收分析 (Finance)",
    user: "用户增长 (User Growth)",
    device: "设备与使用 (Device & Usage)"
};

export default function ReportViewer() {
    const [reports, setReports] = useState<any[]>([]);
    const [tables, setTables] = useState<any[]>([]); // For editor
    const [editingReport, setEditingReport] = useState<any | null>(null);
    const [isEditorOpen, setIsEditorOpen] = useState(false);
    const [targetCategory, setTargetCategory] = useState("finance");
    const [deleteConfirm, setDeleteConfirm] = useState<{ isOpen: boolean, reportId: string | null }>({ isOpen: false, reportId: null });

    const API_BASE = "http://localhost:8000/api";

    useEffect(() => {
        refreshReports();
        fetchSchema();
    }, []);

    const refreshReports = () => {
        fetch(`${API_BASE}/reports`)
            .then(res => res.json())
            .then(data => setReports(data.reports || []));
    };

    const fetchSchema = () => {
        fetch(`${API_BASE}/schema`)
            .then(res => {
                if (!res.ok) throw new Error("Failed to fetch schema");
                return res.json();
            })
            .then(data => {
                const combined = [...(data.dimensions || []), ...(data.facts || [])];
                console.log("Fetched schema tables:", combined.length);
                setTables(combined);
            })
            .catch(err => console.error("Schema fetch error:", err));
    };

    const openNewReport = (category: string) => {
        if (tables.length === 0) fetchSchema(); // Retry fetch
        setEditingReport(null);
        setTargetCategory(category);
        setIsEditorOpen(true);
    };

    const handleSaveReport = async (report: any) => {
        // Optimistic update
        const otherReports = reports.filter(r => r.id !== report.id);
        const newReportsList = [...otherReports, report];
        setReports(newReportsList);
        setIsEditorOpen(false);

        // Persist
        await fetch(`${API_BASE}/reports`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ reports: newReportsList })
        });
    };

    const handleDeleteClick = (id: string) => {
        setDeleteConfirm({ isOpen: true, reportId: id });
    };

    const executeDelete = async () => {
        if (!deleteConfirm.reportId) return;

        await fetch(`${API_BASE}/reports/${deleteConfirm.reportId}`, { method: "DELETE" });
        setDeleteConfirm({ isOpen: false, reportId: null });
        refreshReports();
    };



    const openEditReport = (report: any) => {
        setEditingReport(report);
        setIsEditorOpen(true);
    };

    const handleCopyClick = (report: any) => {
        const newReport = {
            ...report,
            id: `report_${Date.now()}`,
            title: `${report.title} (Copy)`
        };
        setEditingReport(newReport);
        setIsEditorOpen(true);
    };

    // Group reports by category
    const groupedReports: any = { finance: [], user: [], device: [] };
    reports.forEach(r => {
        const cat = r.category || "finance";
        if (!groupedReports[cat]) groupedReports[cat] = [];
        groupedReports[cat].push(r);
    });

    return (
        <div className="h-full flex flex-col bg-gray-50 overflow-y-auto">
            {/* Page Header */}
            <div className="bg-white border-b border-gray-200 px-8 py-6 flex justify-between items-center">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900">BI 数据分析看板 (Analytics)</h1>
                    <p className="text-gray-500 mt-1 text-sm">
                        自定义报表配置中心
                    </p>
                </div>
            </div>

            <div className="p-8 space-y-10">
                {Object.keys(CATEGORY_NAMES).map((catKey) => (
                    <div key={catKey} className="space-y-4">
                        <div className="flex items-center justify-between border-b border-gray-200 pb-2 mb-4">
                            <div className="flex items-center space-x-2">
                                {ICONS[catKey] || <LayoutDashboard size={24} />}
                                <h2 className="text-xl font-bold text-gray-800">{CATEGORY_NAMES[catKey]}</h2>
                            </div>
                            <button
                                onClick={() => openNewReport(catKey)}
                                className="flex items-center text-sm font-medium text-indigo-600 hover:bg-indigo-50 px-3 py-1.5 rounded transition-colors"
                            >
                                <Plus size={16} className="mr-1" /> Add Chart
                            </button>
                        </div>

                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                            {(groupedReports[catKey] || []).map((report: any) => (
                                <div key={report.id} className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden hover:shadow-md transition-all group flex flex-col relative">

                                    {/* Edit Controls */}
                                    <div className="absolute top-2 right-2 z-10 flex space-x-1">
                                        <button
                                            onClick={(e) => { e.stopPropagation(); openEditReport(report); }}
                                            className="p-1.5 bg-white/90 rounded-md shadow-sm text-gray-600 hover:text-indigo-600 border border-gray-200"
                                            title="Edit Report"
                                        >
                                            <Edit2 size={14} />
                                        </button>
                                        <button
                                            onClick={(e) => { e.stopPropagation(); handleCopyClick(report); }}
                                            className="p-1.5 bg-white/90 rounded-md shadow-sm text-gray-600 hover:text-blue-600 border border-gray-200"
                                            title="Copy Report"
                                        >
                                            <Copy size={14} />
                                        </button>
                                        <button
                                            onClick={(e) => { e.stopPropagation(); handleDeleteClick(report.id); }}
                                            className="p-1.5 bg-white/90 rounded-md shadow-sm text-gray-600 hover:text-red-500 border border-gray-200"
                                            title="Delete Report"
                                        >
                                            <Trash2 size={14} />
                                        </button>
                                    </div>

                                    {/* Content Area */}
                                    <div className="p-5 flex-1 flex flex-col">
                                        <div className="mb-4">
                                            <h3 className="font-bold text-gray-900 mb-1">{report.title}</h3>
                                            <p className="text-xs text-gray-500 line-clamp-2 h-8">{report.description}</p>
                                        </div>

                                        {/* Configuration Preview */}
                                        <div className="bg-gray-50 rounded p-3 text-xs space-y-1 border border-gray-100 font-mono text-gray-600 mt-auto">
                                            <div className="flex justify-between">
                                                <span className="text-gray-400">Table:</span>
                                                <span className="font-semibold text-indigo-600 truncate max-w-[120px]">{report.source_table}</span>
                                            </div>
                                            <div className="flex justify-between">
                                                <span className="text-gray-400">GroupBy:</span>
                                                <span>{report.group_by}</span>
                                            </div>
                                            <div className="pt-1 border-t border-gray-100 text-gray-500 truncate" title={report.measure_formula}>
                                                f(x) = {report.measure_formula}
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            ))}

                            {/* Empty State */}
                            {(!groupedReports[catKey] || groupedReports[catKey].length === 0) && (
                                <div className="col-span-full py-8 text-center text-gray-400 text-sm border-2 border-dashed border-gray-100 rounded-xl">
                                    No charts in this category yet.
                                </div>
                            )}
                        </div>
                    </div>
                ))}
            </div>

            {isEditorOpen && (
                <ReportEditor
                    report={editingReport}
                    tables={tables}
                    onSave={handleSaveReport}
                    onCancel={() => setIsEditorOpen(false)}
                />
            )}

            {/* Custom Delete Confirmation Modal */}
            {deleteConfirm.isOpen && (
                <div className="fixed inset-0 bg-black/50 z-[60] flex items-center justify-center p-4">
                    <div className="bg-white rounded-xl shadow-2xl p-6 w-full max-w-sm">
                        <h3 className="text-lg font-bold text-gray-900 mb-2">Delete Report?</h3>
                        <p className="text-gray-500 text-sm mb-6">
                            Are you sure you want to delete this report? This action cannot be undone.
                        </p>
                        <div className="flex justify-end space-x-3">
                            <button
                                onClick={() => setDeleteConfirm({ isOpen: false, reportId: null })}
                                className="px-4 py-2 text-sm text-gray-600 font-medium hover:bg-gray-100 rounded-lg"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={executeDelete}
                                className="px-4 py-2 text-sm bg-red-600 text-white font-medium hover:bg-red-700 rounded-lg shadow-sm"
                            >
                                Delete
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
