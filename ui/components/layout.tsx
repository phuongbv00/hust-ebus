import {DragEvent, ReactNode, useRef, useState} from "react";
import {Menu, Trash2, X} from "lucide-react";
import {Button} from "@/components/ui/button";
import {Input} from "@/components/ui/input";
import JobTriggerForm from "@/components/job-trigger-form";
import {Card} from "@/components/ui/card";
import {MapContext, MapProvider, Point} from "@/context/map-context";
import SearchItemComponent from "@/components/search-item-component";

import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";

interface LayoutProps {
    children: ReactNode;
}

const Layout = ({children}: LayoutProps) => {
    const [sidebarOpen, setSidebarOpen] = useState<boolean>(false);
    return (
        <MapProvider>
            <div className="flex min-h-screen">
                {/* Sidebar */}
                <aside
                    className={`bg-gray-50 text-gray-800 transition-all duration-300 relative overflow-y-auto max-h-screen ${
                        sidebarOpen ? "w-80 p-4" : "w-0 p-0 overflow-hidden"
                    }`}
                >
                    {sidebarOpen && (
                        <>
                            {/* Close Button */}
                            <Button
                                variant="ghost"
                                size="icon"
                                onClick={() => setSidebarOpen(false)}
                                className="absolute top-4 right-4 text-gray-800"
                            >
                                <X size={28}/>
                            </Button>

                            <h2 className="text-xl font-bold mb-6">Hệ thống gợi ý<br/>điểm đón trả xe bus</h2>

                            <Tabs defaultValue="search"  className="w-full">
                                <TabsList className="grid w-full grid-cols-2">
                                    <TabsTrigger value="search">Tìm kiếm</TabsTrigger>
                                    <TabsTrigger value="job">Job</TabsTrigger>
                                </TabsList>

                                <div className="flex-1">
                                    <TabsContent value="search">
                                        <SearchItemComponent />
                                    </TabsContent>
                                    <TabsContent value="job">
                                        <JobTriggerForm />
                                    </TabsContent>
                                </div>
                            </Tabs>
                        </>
                    )}
                </aside>

                {/* Main Content */}
                <div className="flex-1 bg-white">
                    {!sidebarOpen && (
                        <Button
                            size="icon"
                            onClick={() => setSidebarOpen(true)}
                            className="absolute top-3 start-3 z-[1000]"
                        >
                            <Menu size={34}/>
                        </Button>
                    )}

                    {children}
                </div>
            </div>
        </MapProvider>
    );
};

export default Layout;
