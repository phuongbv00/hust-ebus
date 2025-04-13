import {DragEvent, ReactNode, useRef, useState} from "react";
import {Menu, Trash2, X} from "lucide-react";
import {Button} from "@/components/ui/button";
import {Input} from "@/components/ui/input";
import JobTriggerForm from "@/components/job-trigger-form";
import {Card} from "@/components/ui/card";

interface LayoutProps {
    children: ReactNode;
}

const Layout = ({children}: LayoutProps) => {
    const [sidebarOpen, setSidebarOpen] = useState<boolean>(false);
    const [uploadedFile, setUploadedFile] = useState<File | null>(null);
    const fileInputRef = useRef<HTMLInputElement>(null)

    const handleFileUpload = (files: FileList | null) => {
        if (!files || files.length === 0) return;
        const file = files[0];
        if (file.type === "text/csv") {
            setUploadedFile(file);
            alert("Tải lên CSV thành công!");
        } else {
            alert("Tệp không hợp lệ. Vui lòng chọn file .csv!");
        }
    };

    const handleDrop = (e: DragEvent<HTMLDivElement>) => {
        e.preventDefault();
        handleFileUpload(e.dataTransfer.files);
    };

    const handleDragOver = (e: DragEvent<HTMLDivElement>) => {
        e.preventDefault();
    };

    const handleRemoveFile = () => {
        setUploadedFile(null);
    };

    const handleUploadCsv = async () => {
        if (!uploadedFile) return;
        const BASE_URL = "http://localhost:8002"
        try {
            const formData = new FormData();
            formData.append("file", uploadedFile);

            const res = await fetch(BASE_URL + "/upload-csv", {
                method: "POST",
                body: formData,
            });

            const data = await res.json();
            alert(data.message);

        } catch (error) {
            console.log(error);
        }
    };
    return (
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

                        <Card className="rounded-md p-3 gap-3">
                            {/* Upload Section */}
                            <div className="space-y-3">
                            <span className="text-gray-600 dark:text-gray-400 font-medium">
                              📦 Tải lên danh sách sinh viên
                            </span>
                            </div>

                            <div
                                onDrop={handleDrop}
                                onDragOver={handleDragOver}
                                className="border-2 border-dashed border-gray-500 p-4 rounded text-center cursor-pointer bg-gray-50 hover:bg-gray-200"
                                onClick={() => fileInputRef.current?.click()}
                            >
                                <p>Kéo & thả file CSV hoặc click để chọn</p>
                                <Input
                                    ref={fileInputRef}
                                    type="file"
                                    accept=".csv"
                                    onChange={(e) => handleFileUpload(e.target.files)}
                                    className="hidden"
                                />
                            </div>

                            {/* Uploaded File List */}
                            {uploadedFile && (
                                <div className="space-y-2">
                                    <div className="flex items-center justify-between bg-gray-100 p-2 rounded border">
                                        <span className="truncate font-medium">{uploadedFile.name}</span>
                                        <Button
                                            variant="ghost"
                                            size="icon"
                                            onClick={handleRemoveFile}
                                            className="text-gray-500 hover:text-gray-600"
                                        >
                                            <Trash2 size={18}/>
                                        </Button>
                                    </div>
                                </div>
                            )}

                            {/* Upload Button */}
                            <div className="flex items-center justify-between">
                                <Button
                                    onClick={handleUploadCsv}
                                    disabled={!uploadedFile}
                                    className="w-full"
                                >
                                    Tải lên
                                </Button>
                            </div>
                        </Card>
                        <JobTriggerForm className="mt-4"/>
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
    );
};

export default Layout;
