import { useState } from "react";
import { Card, CardContent, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

type Point = {
    id: string | number;
    latitude: number;
    longitude: number;
};

type Props = {
    onFound: (point: Point) => void;
    busStops: Point[];
    students: Point[];
    buses: Point[]; // nếu có dữ liệu bus
};

export default function SearchItemComponent({ onFound, busStops, students, buses }: Props) {
    const [type, setType] = useState<"bus" | "bus stop" | "student">("student");
    const [searchId, setSearchId] = useState("");
    const [result, setResult] = useState<Point | null>(null);
    const [error, setError] = useState<string | null>(null);

    const handleSearch = () => {
        let dataset: Point[] = [];

        if (type === "student") dataset = students;
        else if (type === "bus stop") dataset = busStops;
        else dataset = buses;

        const found = dataset.find((item) => item.id.toString() === searchId.trim());

        if (!found) {
            setError(`Id cho ${type} không tồn tại`);
            setResult(null);
        } else {
            setError(null);
            setResult(found);
        }
    };

    return (
        <Card className="p-4 space-y-4">
            <div className="flex gap-4">
                {["bus", "bus stop", "student"].map((opt) => (
                    <label key={opt} className="flex items-center space-x-1">
                        <input
                            type="radio"
                            name="type"
                            value={opt}
                            checked={type === opt}
                            onChange={() => setType(opt as any)}
                        />
                        <span>{opt}</span>
                    </label>
                ))}
            </div>

            <Input
                placeholder="Nhập ID"
                value={searchId}
                onChange={(e) => setSearchId(e.target.value)}
            />

            <Button onClick={handleSearch}>Tìm kiếm</Button>

            {error && <p className="text-red-500">{error}</p>}

            {result && (
                <Card className="p-4 border bg-gray-50 space-y-2">
                    <CardTitle>ID: {result.id}</CardTitle>
                    <CardContent>
                        <p>Latitude: {result.latitude}</p>
                        <p>Longitude: {result.longitude}</p>
                        <Button
                            className="mt-2"
                            onClick={() => onFound(result)}
                        >
                            Di chuyển đến điểm này
                        </Button>
                    </CardContent>
                </Card>
            )}
        </Card>
    );
}
