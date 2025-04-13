import {ReactNode, useState, DragEvent} from "react";
import {Menu, X, Trash2} from "lucide-react";

interface LayoutProps {
    children: ReactNode;
}

const Layout = ({children}: LayoutProps) => {
    const [sidebarOpen, setSidebarOpen] = useState<boolean>(false);
    const [uploadedFile, setUploadedFile] = useState<File| null>(null);

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
                className={`bg-gray-50 text-gray-800 transition-all duration-300 relative ${
                    sidebarOpen ? "w-80 p-4" : "w-0 p-0 overflow-hidden"
                }`}
            >
                {sidebarOpen && (
                    <>
                        {/* Close button */}
                        <button
                            onClick={() => setSidebarOpen(false)}
                            className="absolute top-4 right-4 text-gray-800 cursor-pointer text-2xl"
                        >
                            <X size={28}/>
                        </button>

                        <h2 className="text-xl font-bold mb-6">HUST-BUS-STOP</h2>
                        {/*<nav className="space-y-3">*/}
                        {/*    <a href="#" className="block hover:text-blue-400">🏠 Home</a>*/}
                        {/*    <a href="#" className="block hover:text-blue-400">⚙️ Settings</a>*/}
                        {/*    <a href="#" className="block hover:text-blue-400">📦 Products</a>*/}
                        {/*</nav>*/}
                        <div className="space-y-3">
                            <span
                                className="text-gray-600 dark:text-gray-400 font-medium">📦 Tải lên danh sách học sinh</span>
                        </div>

                        {/* Upload CSV zone */}
                        <div
                            onDrop={handleDrop}
                            onDragOver={handleDragOver}
                            className="border-2 border-dashed border-gray-500 p-4 mt-5 rounded text-center cursor-pointer bg-gray-50 hover:bg-gray-200"
                            onClick={() => document.getElementById("csvInput")?.click()}
                        >
                            <p>Kéo & thả file CSV hoặc click để chọn</p>
                            <input
                                id="csvInput"
                                type="file"
                                accept=".csv"
                                onChange={(e) => handleFileUpload(e.target.files)}
                                className="hidden"
                            />
                        </div>

                        {/* Uploaded file list */}
                        {uploadedFile && (
                            <div className="mt-4 space-y-2">
                                    <div key={uploadedFile.name}
                                         className="flex items-center border-2 justify-between bg-gray-100 p-2 rounded">
                                        <span className="truncate font-medium">{uploadedFile.name}</span>
                                        <button onClick={() => handleRemoveFile()}
                                                className="text-gray-500 cursor-pointer hover:text-gray-600">
                                            <Trash2 size={18}/>
                                        </button>
                                    </div>
                            </div>
                        )}

                        <div className="flex mt-4 items-center justify-between">
                            <button
                                className={`text-gray-50 p-2 font-medium ${uploadedFile === null ? "bg-gray-600" : "bg-blue-500 cursor-pointer"} w-full rounded`}
                                disabled={uploadedFile === null} onClick={handleUploadCsv}>
                                Tải lên
                            </button>
                        </div>
                    </>
                )}
            </aside>

            {/* Main Content */}
            <div className="flex-1 bg-white">
                {!sidebarOpen && (
                    <button
                        onClick={() => setSidebarOpen(true)}
                        className="mb-4 text-gray cursor-pointer absolute top-20 left-2 text-2xl z-9999 bg-white border-2"
                    >
                        <Menu size={34}/>
                    </button>
                )}

                {children}
            </div>
        </div>
    );
};

export default Layout;
