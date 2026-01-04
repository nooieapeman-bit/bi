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
                const item: any = { name: xVal, Total: 0 };
                json.series.forEach((s: any) => {
                    const val = s.data[idx] || 0;
                    item[s.name || "Value"] = val;
                    item.Total += val;
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

    const formatLabel = (val: any) => val >= 1000000 ? `${(val / 1000000).toFixed(1)}M` : (val >= 1000 ? `${(val / 1000).toFixed(0)}k` : val);

    if (report.chart_type === "matrix") {
        // Find all value keys (exclude 'name' and 'Total')
        const valueKeys = Object.keys(data[0] || {}).filter(k => k !== 'name' && k !== 'Total');

        return (
            <div className="w-full overflow-x-auto border border-gray-100 rounded-xl bg-white shadow-sm mt-2">
                <table className="w-full text-[11px] text-left border-collapse">
                    <thead>
                        <tr className="bg-gray-50 text-gray-400 font-bold uppercase tracking-wider">
                            <th className="px-2 py-2 border-r border-gray-100 sticky left-0 bg-gray-50 z-10 w-24">Cohort</th>
                            {valueKeys.map(k => (
                                <th key={k} className="px-2 py-2 text-center min-w-[70px] border-b border-gray-100">{k}</th>
                            ))}
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                        {data.map((row, idx) => (
                            <tr key={idx} className="hover:bg-gray-50 transition-colors">
                                <td className="px-2 py-2 font-bold text-gray-700 bg-gray-50 border-r border-gray-100 sticky left-0 z-10 whitespace-nowrap">{row.name}</td>
                                {valueKeys.map(key => {
                                    const val = row[key];
                                    const opacity = val / 100;
                                    const bgColor = `rgba(79, 70, 229, ${opacity * 0.85})`;
                                    const textColor = opacity > 0.5 ? 'white' : 'black';

                                    return (
                                        <td
                                            key={key}
                                            className="px-2 py-2 text-center font-medium border-l border-gray-50"
                                            style={{ backgroundColor: bgColor, color: textColor }}
                                        >
                                            {val.toFixed(1)}%
                                        </td>
                                    );
                                })}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        );
    }

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
                        <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '10px' }} />
                        {Object.keys(data[0] || {}).filter(k => k !== 'name' && k !== 'Total').map((key, idx, arr) => (
                            <Bar
                                key={key}
                                dataKey={key}
                                fill={idx === 0 ? "#4F46E5" : (idx === 1 ? "#10B981" : (idx === 2 ? "#F59E0B" : "#8B5CF6"))}
                                radius={[4, 4, 0, 0]}
                                stackId="a"
                            >
                                {idx === arr.length - 1 && (
                                    <LabelList
                                        dataKey="Total"
                                        position="top"
                                        offset={10}
                                        formatter={formatLabel}
                                        style={{ fontSize: '12px', fill: '#1F2937', fontWeight: '900' }}
                                    />
                                )}
                            </Bar>
                        ))}
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
                        <Legend wrapperStyle={{ fontSize: '12px', paddingTop: '10px' }} />
                        {Object.keys(data[0] || {}).filter(k => k !== 'name').map((key, idx) => (
                            <Line
                                key={key}
                                type="monotone"
                                dataKey={key}
                                stroke={idx === 0 ? "#4F46E5" : (idx === 1 ? "#10B981" : (idx === 2 ? "#F59E0B" : "#8B5CF6"))}
                                strokeWidth={2}
                                dot={{ r: 3, fill: idx === 0 ? "#4F46E5" : (idx === 1 ? "#10B981" : "#F59E0B"), strokeWidth: 0 }}
                                activeDot={{ r: 5 }}
                            />
                        ))}
                    </LineChart>
                )}
            </ResponsiveContainer>
        </div>
    );
}
