// SearchItemComponent.tsx
import {useContext, useState} from "react";
import {Card, CardContent, CardTitle} from "@/components/ui/card";
import {Input} from "@/components/ui/input";
import {Button} from "@/components/ui/button";
import {MapContext} from "@/context/map-context";
import {Label} from "@/components/ui/label";
import {
    Dialog,
    DialogContent,
    DialogHeader,
    DialogTitle,
    DialogTrigger,
} from "@/components/ui/dialog";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import {
    Pagination,
    PaginationContent,
    PaginationEllipsis,
    PaginationItem,
    PaginationLink,
    PaginationNext,
    PaginationPrevious,
} from "@/components/ui/pagination"


export default function SearchItemComponent() {
    const { mapData, setMapCenter, setHighlightPoint } = useContext(MapContext);

    const [type, setType] = useState<"bus" | "bus stop" | "student">("student");
    const [searchId, setSearchId] = useState("");
    const [result, setResult] = useState<any>(null);
    const [error, setError] = useState<string | null>(null);
    const [openDialog, setOpenDialog] = useState(false);
    const [page, setPage] = useState(1);
    const itemsPerPage = 10;

    const dataset =
        type === "student"
            ? mapData?.assignments || []
            : type === "bus stop"
                ? mapData?.busStops || []
                : mapData?.buses || [];


    const paginated = dataset.slice((page - 1) * itemsPerPage, page * itemsPerPage);
    const totalPages = Math.ceil(dataset.length / itemsPerPage);

    // Tính dãy trang cần hiển thị
    const renderPages = () => {
        if (totalPages <= 5) {
            return Array.from({ length: totalPages }, (_, i) => i + 1);
        }
        // >5: 1,2,3,..., last-2,last-1,last
        return [
            1, 2, 3,
            -1,                       // biểu thị dấu ellipsis
            totalPages - 2,
            totalPages - 1,
            totalPages
        ];
    };

    const pagesToShow = renderPages();
    const handleSearch = () => {
        if (!mapData) return;
        const dataset =
            type === "student" ? mapData?.assignments :
                type === "bus stop" ? mapData?.busStops :
                    mapData?.buses;


        const found = dataset.find((i) => {
            return type === "student" ? i?.student_id.toString() === searchId.trim() :
                type === "bus stop" ? i?.stop_id.toString() === searchId.trim() :
                    i?.bus_id.toString() === searchId.trim()

        });
        if (!found) {
            setError(`Id cho ${type} không tồn tại`);
            setResult(null);
        } else {
            setError(null);
            setResult(found);
        }
    };


    const handleMoveToPoint = (point: { latitude: number; longitude: number }) => {
        if (setMapCenter) {
            setMapCenter({ lat: point.latitude, lng: point.longitude });
        }
        if (setHighlightPoint) {
            setHighlightPoint(point); // <--- thêm dòng này để truyền cho map biết
        }
    };

    const handleReassign = async () => {
        const BASE_URL = "http://localhost:8002"
        try {
            const rs = await fetch(BASE_URL + "/reassign-student-locations", {
                method: "POST",
            }).then(res => res.json());
            console.log(rs)

        } catch (error) {
            console.error("Error loading points data:", error)
        }
    }

    return (
        <Card className="p-4 gap-2">
            <h1 className="font-bold">Tìm kiếm vị trí theo đối tượng</h1>
            <div className="mt-3 flex-col">
                <Label>Chọn đối tượng tìm kiếm: </Label>
                <div className="flex mt-2 flex-col items-start justify-between">
                    {["bus", "bus stop", "student"].map((opt) => (
                        <label key={opt} className="flex items-center space-x-1">
                            <input
                                type="radio"
                                name="type"
                                value={opt}
                                checked={type === opt}
                                onChange={() => {
                                    setType(opt as any);
                                    setResult(null);
                                    setSearchId('');
                                }}
                            />
                            <span>{opt}</span>
                        </label>
                    ))}
                </div>
            </div>
            <div className="mt-3 flex-col gap-2">
                <Label>Nhập ID đối tượng: </Label>
                <Input
                    className="mt-2"
                    placeholder="Nhập ID"
                    value={searchId}
                    onChange={(e) => setSearchId(e.target.value)}
                />
            </div>
            <Button onClick={handleSearch}>Tìm kiếm</Button>
            <Button onClick={handleReassign}>Tái phân bổ</Button>
            <Dialog open={openDialog} onOpenChange={setOpenDialog}>
                <DialogTrigger asChild>
                    <Button variant="outline">Xem danh sách các đối tượng</Button>
                </DialogTrigger>
                <DialogContent className="max-w-3xl z-99999">
                    <DialogHeader><DialogTitle>Danh sách {type}</DialogTitle></DialogHeader>
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead>ID</TableHead>
                                {type === "student" && <TableHead>Tên</TableHead>}
                                <TableHead>Latitude</TableHead>
                                <TableHead>Longitude</TableHead>
                                <TableHead>Hành động</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {paginated.map((item, idx) => (
                                <TableRow key={idx}>
                                    <TableCell>{item.stop_id || item.student_id || item.bus_id}</TableCell>
                                    {type === "student" && <TableCell>{item.name}</TableCell>}
                                    <TableCell>{item.latitude}</TableCell>
                                    <TableCell>{item.longitude}</TableCell>
                                    <TableCell>
                                        <Button size="sm" className="cursor-pointer" onClick={() => {
                                            handleMoveToPoint(item)
                                            setOpenDialog(false)
                                        }}>
                                            Bản đồ
                                        </Button>
                                    </TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                    <div className="flex justify-end pt-4">
                        <Pagination>
                            <PaginationContent>
                                <PaginationItem>
                                    <PaginationPrevious onClick={() => setPage((p) => Math.max(1, p - 1))} />
                                </PaginationItem>
                                {pagesToShow.map((p, idx) => (
                                    p === -1 ? (
                                        <PaginationItem key={`ellipsis-${idx}`}><PaginationEllipsis /></PaginationItem>
                                    ) : (
                                        <PaginationItem key={p}>
                                            <PaginationLink href="#" isActive={p === page} onClick={(e) => { e.preventDefault(); setPage(p); }}>
                                                {p}
                                            </PaginationLink>
                                        </PaginationItem>
                                    )
                                ))}
                                <PaginationItem>
                                    <PaginationNext onClick={() => setPage((p) => Math.min(totalPages, p + 1))} />
                                </PaginationItem>
                            </PaginationContent>
                        </Pagination>
                    </div>
                </DialogContent>
            </Dialog>
            {error && <p className="text-red-500">{error}</p>}

            {result && (
                <Card className="p-4 border bg-gray-50">
                    <CardTitle>{type} id: {result.stop_id || result.student_id || result.bus_id}</CardTitle>
                    {type === 'student' && <span><strong>Tên: </strong>{result.name}</span>}
                    <span><strong>Vĩ độ (Lat):</strong></span><p> {result.latitude}</p>
                    <span><strong>Kinh độ (Lng):</strong></span><p> {result.longitude}</p>
                    <Button
                        className="mt-2 cursor-pointer"
                        onClick={() => handleMoveToPoint(result)}
                    >
                        Di chuyển đến điểm này
                    </Button>
                </Card>
            )}
        </Card>
    );
}
