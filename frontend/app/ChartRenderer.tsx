"use client";

import React, { useState, useEffect } from "react";
import {
    LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, LabelList
} from 'recharts';
import { Loader2, AlertCircle } from "lucide-react";

interface ChartRendererProps {
    report: any;
    apiBase: string;
    filters: any;
}

export default function ChartRenderer({ report, apiBase, filters }: ChartRendererProps) {
    const [data, setData] = useState<any[] | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        fetchData();
    }, [report.id, report.config, apiBase, JSON.stringify(filters)]);

    const fetchData = async () => {
        setLoading(true);
        setError(null);
        try {
            const res = await fetch(`${apiBase}/query`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    report_id: report.id,
                    filters: filters
                })
            });
            const json = await res.json();
            if (!res.ok) throw new Error(json.detail || "Query failed");

            // Transform for Recharts: { x_axis: [...], series: [{data: [...]}] }
            // Needs array of objects: [{ name: 'Jan', value: 100 }, ...]
            const chartData = json.x_axis.map((xVal: any, idx: number) => {
                const item: any = { name: xVal };
                json.series.forEach((s: any) => {
                    item[s.name || "Value"] = s.data[idx];
                });
                return item;
            });

            setData(chartData);

        } catch (err: any) {
            console.error("Chart fetch error:", err);
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    if (loading) return (
        <div className="h-64 flex items-center justify-center text-gray-400">
            <Loader2 className="animate-spin mr-2" /> Loading data...
        </div>
    );

    if (error) return (
        <div className="h-64 flex items-center justify-center text-red-400 text-sm p-4 text-center border border-red-100 bg-red-50 rounded">
            <AlertCircle size={16} className="mr-2 mb-1" />
            Error: {error}
        </div>
    );

    if (!data || data.length === 0) return (
        <div className="h-64 flex items-center justify-center text-gray-400 text-sm">
            No data available for this period.
        </div>
    );

    return (
        <div className="w-full h-full">
            <ResponsiveContainer width="100%" height="100%">
                {report.chart_type === "bar" ? (
                    <BarChart data={data}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E5E7EB" />
                        <XAxis
                            dataKey="name"
                            tick={{ fontSize: 10, fill: '#6B7280' }}
                            tickLine={false}
                            axisLine={{ stroke: '#E5E7EB' }}
                        />
                        <YAxis
                            tick={{ fontSize: 10, fill: '#6B7280' }}
                            tickLine={false}
                            axisLine={false}
                            tickFormatter={(val) => val >= 1000 ? `${(val / 1000).toFixed(1)}k` : val}
                        />
                        <Tooltip
                            contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                            itemStyle={{ fontSize: '12px' }}
                        />
                        <Bar dataKey="Value" fill="#4F46E5" radius={[4, 4, 0, 0]}>
                            <LabelList
                                dataKey="Value"
                                position="top"
                                offset={10}
                                formatter={(val: any) => val >= 1000000 ? `${(val / 1000000).toFixed(1)}M` : (val >= 1000 ? `${(val / 1000).toFixed(0)}k` : val)}
                                style={{ fontSize: '10px', fill: '#4F46E5', fontWeight: 'bold' }}
                            />
                        </Bar>
                    </BarChart>
                ) : (
                    <LineChart data={data}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E5E7EB" />
                        <XAxis
                            dataKey="name"
                            tick={{ fontSize: 10, fill: '#6B7280' }}
                            tickLine={false}
                            axisLine={{ stroke: '#E5E7EB' }}
                        />
                        <YAxis
                            tick={{ fontSize: 10, fill: '#6B7280' }}
                            tickLine={false}
                            axisLine={false}
                            tickFormatter={(val) => val >= 1000 ? `${(val / 1000).toFixed(1)}k` : val}
                        />
                        <Tooltip
                            contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                            itemStyle={{ fontSize: '12px' }}
                        />
                        <Line
                            type="monotone"
                            dataKey="Value"
                            stroke="#4F46E5"
                            strokeWidth={2}
                            dot={{ r: 3, fill: '#4F46E5', strokeWidth: 0 }}
                            activeDot={{ r: 5 }}
                        />
                    </LineChart>
                )}
            </ResponsiveContainer>
        </div>
    );
}
