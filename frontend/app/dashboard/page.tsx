"use client";

import React, { useState, useEffect } from "react";
import DashboardViewer from "../DashboardViewer";
import { ArrowLeft } from "lucide-react";
import Link from "next/link";

export default function DashboardPage() {
    return (
        <div className="h-screen flex flex-col bg-gray-50">
            {/* Simple Header for navigation back */}
            <div className="bg-white border-b border-gray-200 px-8 py-4 flex items-center justify-between shadow-sm z-10">
                <div className="flex items-center space-x-4">
                    <Link href="/" className="text-gray-500 hover:text-indigo-600 transition-colors p-2 hover:bg-gray-100 rounded-lg">
                        <ArrowLeft size={20} />
                    </Link>
                    <div>
                        <h1 className="text-2xl font-bold text-gray-900 flex items-center">
                            <span className="bg-indigo-600 w-3 h-8 rounded mr-3"></span>
                            BI Analytics Dashboard
                        </h1>
                        <p className="text-xs text-gray-500 mt-1 ml-6">Real-time Visualization & Insights</p>
                    </div>
                </div>
                <div className="flex items-center space-x-4">
                    <div className="text-right mr-4 hidden md:block">
                        <p className="text-sm font-bold text-gray-800">Administrator</p>
                        <p className="text-xs text-gray-500">admin@bi-platform.com</p>
                    </div>
                    <div className="w-10 h-10 bg-indigo-100 rounded-full flex items-center justify-center text-indigo-700 font-bold border-2 border-white shadow-sm">
                        AD
                    </div>
                </div>
            </div>

            {/* Content */}
            <div className="flex-1 overflow-hidden relative">
                <DashboardViewer />
            </div>
        </div>
    );
}
