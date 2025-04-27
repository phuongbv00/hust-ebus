"use client"

import {useContext, useEffect, useRef, useState} from "react"
import {CircleMarker, GeoJSON, LayerGroup, MapContainer, Popup, TileLayer, useMap, ZoomControl} from "react-leaflet"
import "leaflet/dist/leaflet.css"
import {Card} from "@/components/ui/card";
import {Checkbox} from "@/components/ui/checkbox"
import {MapContext} from "@/context/map-context";

type Point = {
    latitude: number
    longitude: number
}

type BusStop = Point & {
    stop_id: number
    road_id: number
}

type Assignment = Point & {
    stop_id: number
    student_id: number
    name: string
}

type BusAssignment = Point & {
    stop_id: number
    bus_id: number
    distance: number
    num_students: number
    capacity: number
}

type StudentCluster = Point & {
    cluster_id: number
}

// Map center to specified location
const defaultCenter = {lat: 21.018812412744, lng: 105.83191103813589}

// Component to recenter the map to a specific point
function SetViewOnClick({coords}: { coords: { lat: number; lng: number } }) {
    const map = useMap()
    map.setView(coords, map.getZoom())
    return null
}

export default function Map() {
    const [assignments, setAssignments] = useState<Assignment[]>([])
    const [busAssignments, setBusAssignments] = useState<BusAssignment[]>([])
    const [busStops, setBusStops] = useState<BusStop[]>([])
    const [roadsGeoJSON, setRoadsGeoJSON] = useState<any>([])
    const [studentClusters, setStudentClusters] = useState<StudentCluster[]>([])
    const [activePoint, setActivePoint] = useState<Point | null>(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const mapRef = useRef(null)
    const [showAssignments, setShowAssignments] = useState(true)
    const [showStudentClusters, setShowStudentClusters] = useState(true)
    const [showBusStops, setShowBusStops] = useState(true)
    const [showBuses, setShowBuses] = useState(true)
    const [showHighlightPoint, setShowHighlightPoint] = useState(true)
    const {setMapData, mapCenter, highlightPoint} = useContext(MapContext);
    const pollInterval = 3_000;
    const fetchData = async () => {
        const BASE_URL = "http://localhost:8002"
        try {
            const rs = await Promise.all([
                fetch(BASE_URL + "/assignments").then(res => res.json()),
                fetch(BASE_URL + "/bus-stops").then(res => res.json()),
                fetch(BASE_URL + "/roads/hanoi").then(res => res.json()),
                fetch(BASE_URL + "/student-clusters").then(res => res.json()),
                fetch(BASE_URL + "/bus-assignments").then(res => res.json()),
            ])
            setAssignments(rs[0])
            setBusStops(rs[1])
            setRoadsGeoJSON(rs[2])
            setStudentClusters(rs[3])
            setBusAssignments(rs[4])
            const mapData = {
                assignments: rs[0],
                busStops: rs[1],
                busAssignments: rs[4],
            }
            setMapData(mapData)
            setError(null)
        } catch (error) {
            console.error("Error loading points data:", error)
            setError("Failed to load points data. Please try again later.")
        } finally {
            setLoading(false)
        }
    }

    // Fetch points data from JSON files
    useEffect(() => {
        fetchData()
        const id = setInterval(fetchData, pollInterval);
        return () => clearInterval(id);
    }, [])

    // Function to center the map on a specific point
    const centerOnPoint = (point: Point) => {
        setActivePoint(point)
    }

    // Function to render popup content for a point
    const renderPopupContent = (data: any) => (
        <div>
            {Object.entries(data).map(([k, v]) => (
                <div key={k}>{k}: {v + ''}</div>
            ))}
        </div>
    )

    if (loading) {
        return <div className="w-full h-screen flex items-center justify-center">Loading points data...</div>
    }

    if (error) {
        return <div className="w-full h-screen flex items-center justify-center text-red-500">{error}</div>
    }

    return (
        <div className="w-full h-screen">
            <Card className="fixed top-3 end-3 p-4 z-[1000] rounded-md gap-4">
                <div className="flex items-center space-x-2">
                    <Checkbox id="chk-1"
                              className="data-[state=checked]:bg-blue-600 data-[state=checked]:border-blue-600"
                              checked={showAssignments}
                              onCheckedChange={e => setShowAssignments(!!e)}/>
                    <label
                        htmlFor="chk-1"
                        className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                    >
                        Student ({assignments.length})
                    </label>
                </div>
                <div className="flex items-center space-x-2">
                    <Checkbox id="chk-2"
                              className="data-[state=checked]:bg-green-600 data-[state=checked]:border-green-600"
                              checked={showStudentClusters}
                              onCheckedChange={e => setShowStudentClusters(!!e)}/>
                    <label
                        htmlFor="chk-2"
                        className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                    >
                        Cluster ({studentClusters.length})
                    </label>
                </div>
                <div className="flex items-center space-x-2">
                    <Checkbox id="chk-3"
                              className="data-[state=checked]:bg-destructive data-[state=checked]:border-destructive"
                              checked={showBusStops}
                              onCheckedChange={e => setShowBusStops(!!e)}/>
                    <label
                        htmlFor="chk-3"
                        className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                    >
                        Bus Stop ({busStops.length})
                    </label>
                </div>
                <div className="flex items-center space-x-2">
                    <Checkbox id="chk-4"
                              className="data-[state=checked]:bg-yellow-300 data-[state=checked]:border-yellow-300"
                              checked={showBuses}
                              onCheckedChange={e => setShowBuses(!!e)}/>
                    <label
                        htmlFor="chk-4"
                        className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                    >
                        Bus ({busAssignments.length})
                    </label>
                </div>
                <div className="flex items-center space-x-2">
                    <Checkbox id="chk-5"
                              className="data-[state=checked]:bg-violet-700 data-[state=checked]:border-violet-700"
                              checked={showHighlightPoint}
                              onCheckedChange={e => setShowHighlightPoint(!!e)}/>
                    <label
                        htmlFor="chk-5"
                        className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                    >
                        Current point
                    </label>
                </div>
            </Card>
            <MapContainer center={defaultCenter} zoom={13} style={{height: "100%", width: "100%"}} ref={mapRef}
                          zoomControl={false}>
                <TileLayer
                    attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                    url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                />

                {/* Roads GeoJSON Layer with gray lines */}
                {roadsGeoJSON && (
                    <GeoJSON
                        data={roadsGeoJSON}
                        style={{
                            color: "#555555",
                            weight: 2,
                            opacity: 1,
                        }}
                    />
                )}

                {/* Student addresses layer */}
                {showAssignments ? (
                    <LayerGroup>
                        {assignments.map((point) => (
                            <CircleMarker
                                key={point.student_id}
                                center={[point.latitude, point.longitude]}
                                radius={6}
                                pathOptions={{color: "#1d4ed8", fillColor: "#1d4ed8", fillOpacity: 0.8}}
                                eventHandlers={{
                                    click: () => centerOnPoint(point),
                                }}
                            >
                                <Popup>{renderPopupContent(point)}</Popup>
                            </CircleMarker>
                        ))}
                    </LayerGroup>
                ) : ''}

                {/* Student clusters layer with circles */}
                {showStudentClusters ? (
                    <LayerGroup>
                        {studentClusters.map((point) => (
                            <div key={point.cluster_id}>
                                {/* Small circle marker - rendered on top */}
                                <CircleMarker
                                    center={[point.latitude, point.longitude]}
                                    radius={6}
                                    pathOptions={{color: "#16a34a", fillColor: "#16a34a", fillOpacity: 0.8}}
                                    eventHandlers={{
                                        click: () => centerOnPoint(point),
                                    }}
                                >
                                    <Popup>{renderPopupContent(point)}</Popup>
                                </CircleMarker>
                            </div>
                        ))}
                    </LayerGroup>
                ) : ''}


                {/* Bus stops layer with circles */}
                {showBusStops ? (
                    <LayerGroup>
                        {busStops.map((point) => (
                            <div key={point.stop_id}>
                                {/* Small circle marker - rendered on top */}
                                <CircleMarker
                                    center={[point.latitude, point.longitude]}
                                    radius={6}
                                    pathOptions={{color: "#ef4444", fillColor: "#ef4444", fillOpacity: 0.8}}
                                    eventHandlers={{
                                        click: () => centerOnPoint(point),
                                    }}
                                >
                                    <Popup>{renderPopupContent(point)}</Popup>
                                </CircleMarker>
                            </div>
                        ))}
                    </LayerGroup>
                ) : ''}

                {/* Bus layer with circles */}
                {showBuses ? (
                    <LayerGroup>
                        {busAssignments.map((point) => (
                            <div key={point.bus_id}>
                                {/* Small circle marker - rendered on top */}
                                <CircleMarker
                                    center={[point.latitude, point.longitude]}
                                    radius={6}
                                    pathOptions={{color: "#FFFF00", fillColor: "#FFFF00", fillOpacity: 0.8}}
                                    eventHandlers={{
                                        click: () => centerOnPoint(point),
                                    }}
                                >
                                    <Popup>{renderPopupContent(point)}</Popup>
                                </CircleMarker>
                            </div>
                        ))}
                    </LayerGroup>
                ) : ''}

                {/* Set view to active point if selected */}
                {activePoint && <SetViewOnClick
                    coords={{lat: activePoint.latitude, lng: activePoint.longitude}}/>}
                {mapCenter && <SetViewOnClick coords={mapCenter}/>}
                {showHighlightPoint && highlightPoint && (
                    <CircleMarker
                        center={[highlightPoint.latitude, highlightPoint.longitude]}
                        radius={10}
                        pathOptions={{color: "#7F00FF", fillColor: "#7F00FF", fillOpacity: 1}}
                    >
                        <Popup>Đã chọn: {highlightPoint.name || highlightPoint.stop_id || highlightPoint.bus_id}</Popup>
                    </CircleMarker>
                )}

                <ZoomControl position="bottomright"/>
            </MapContainer>
        </div>
    )
}
