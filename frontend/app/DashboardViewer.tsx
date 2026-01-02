"use client";

import React, { useState, useEffect } from "react";
import { TrendingUp, Users, Smartphone, Plus, Edit2, Trash2, LayoutDashboard, Copy } from "lucide-react";
import ReportEditor from "./ReportEditor";
import ChartRenderer from "./ChartRenderer";

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

export default function DashboardViewer() {
    const [reports, setReports] = useState<any[]>([]);
    const [tables, setTables] = useState<any[]>([]); // For editor
    const [editingReport, setEditingReport] = useState<any | null>(null);
    const [isEditorOpen, setIsEditorOpen] = useState(false);
    const [targetCategory, setTargetCategory] = useState("finance");
    const [deleteConfirm, setDeleteConfirm] = useState<{ isOpen: boolean, reportId: string | null }>({ isOpen: false, reportId: null });

    // Filter State
    const [filters, setFilters] = useState<any>({});
    const [filterOptions, setFilterOptions] = useState<any>({});

    // Dynamic API Base for LAN access
    const [apiBase, setApiBase] = useState("http://localhost:8000/api");

    useEffect(() => {
        // Run only on client side
        if (typeof window !== "undefined") {
            setApiBase(`http://${window.location.hostname}:8000/api`);
        }
    }, []);

    const API_BASE = apiBase;

    useEffect(() => {
        refreshReports();
        fetchSchema();
    }, [apiBase]);

    useEffect(() => {
        if (reports.length > 0) {
            fetchFilterOptions();
        }
    }, [reports]);

    const fetchFilterOptions = async () => {
        const report = reports[0]; // Assuming single report mode as requested
        if (!report || !report.slices) return;

        const options: any = {};
        for (const slice of report.slices) {
            try {
                const res = await fetch(`${API_BASE}/filter-values/${report.source_table}/${slice}`);
                const data = await res.json();
                options[slice] = data.values || [];
            } catch (err) {
                console.error(`Error fetching filter values for ${slice}:`, err);
            }
        }
        setFilterOptions(options);
    };

    const handleFilterChange = (slice: string, value: string) => {
        setFilters((prev: any) => ({
            ...prev,
            [slice]: value === "all" ? undefined : value
        }));
    };

    const refreshReports = () => {
        fetch(`${API_BASE}/reports`)
            .then(res => res.json())
            .then(data => {
                const allReports = data.reports || [];
                // Filter for Revenue and New Users reports
                const targetIds = ['monthly_revenue_report', 'monthly_new_users'];
                const target = allReports.filter((r: any) => targetIds.includes(r.id));
                setReports(target);
            });
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
            body: JSON.stringify(report) // Backend now expects single report object in save_report
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
            <div className="bg-white border-b border-gray-200 px-8 py-6 flex justify-between items-center shadow-sm">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900">Dashboard (Visualization)</h1>
                    <p className="text-gray-500 mt-1 text-sm">
                        实时数据可视化看板
                    </p>
                </div>

                {/* Global Filters UI */}
                <div className="flex items-center space-x-4">
                    {reports[0]?.slices?.map((slice: string) => (
                        <div key={slice} className="flex flex-col">
                            <label className="text-[10px] font-bold text-gray-400 uppercase tracking-widest mb-1 ml-1">{slice.replace('_key', '')}</label>
                            <select
                                className="bg-gray-50 border border-gray-200 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 outline-none min-w-[120px]"
                                onChange={(e) => handleFilterChange(slice, e.target.value)}
                                value={filters[slice] || "all"}
                            >
                                <option value="all">All {slice.replace('_key', '')}s</option>
                                {filterOptions[slice]?.map((val: any) => (
                                    <option key={val} value={val}>{val}</option>
                                ))}
                            </select>
                        </div>
                    ))}
                </div>
            </div>


            <div className="p-8 space-y-10">
                {reports.map((report: any) => (
                    <div key={report.id} className="bg-white rounded-2xl shadow-sm border border-gray-100 overflow-hidden hover:shadow-md transition-all flex flex-col relative p-8">
                        <div className="mb-8 flex justify-between items-start">
                            <div>
                                <h3 className="text-2xl font-black text-gray-900 mb-2">{report.title}</h3>
                                <p className="text-sm text-gray-500 max-w-2xl">{report.description}</p>
                            </div>
                            <div className="flex items-center space-x-2 bg-indigo-50 px-3 py-1.5 rounded-full">
                                <TrendingUp size={16} className="text-indigo-600" />
                                <span className="text-xs font-bold text-indigo-700 uppercase tracking-wider">Operational Metric</span>
                            </div>
                        </div>

                        {/* Chart Rendering - Full Width & Height */}
                        <div className="w-full h-[500px]">
                            <ChartRenderer report={report} apiBase={API_BASE} filters={filters} />
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
