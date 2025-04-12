"use client"

import {useEffect, useRef, useState} from "react"
import {CircleMarker, LayerGroup, MapContainer, Popup, TileLayer, useMap} from "react-leaflet"
import "leaflet/dist/leaflet.css"

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

// Map center to specified location
const defaultCenter = {lat: 21.018812412744, lng: 105.83191103813589}

// Component to recenter the map to a specific point
function SetViewOnClick({coords}: { coords: { lat: number; lng: number } }) {
    const map = useMap()
    map.setView(coords, map.getZoom())
    return null
}

export default function Map() {
    const [studentAddresses, setStudentAddresses] = useState<Assignment[]>([])
    const [busStops, setBusStops] = useState<BusStop[]>([])
    const [activePoint, setActivePoint] = useState<Point | null>(null)
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const mapRef = useRef(null)

    // Fetch points data from JSON files
    useEffect(() => {
        async function fetchPointsData() {
            const BASE_URL = "http://localhost:8002"
            try {
                // Fetch student addresses
                const studentAddressesResponse = await fetch(BASE_URL + "/assignments")
                if (!studentAddressesResponse.ok) {
                    throw new Error("Failed to fetch student addresses")
                }
                const studentAddressesData = await studentAddressesResponse.json()

                // Fetch bus stops
                const busStopsResponse = await fetch(BASE_URL + "/bus-stops")
                if (!busStopsResponse.ok) {
                    throw new Error("Failed to fetch bus stops")
                }
                const busStopsData = await busStopsResponse.json()

                // Update state with fetched data (no mapping needed now)
                setStudentAddresses(studentAddressesData)
                setBusStops(busStopsData)
                setError(null)
            } catch (error) {
                console.error("Error loading points data:", error)
                setError("Failed to load points data. Please try again later.")
            } finally {
                setLoading(false)
            }
        }

        fetchPointsData()
    }, [])

    // Function to center the map on a specific point
    const centerOnPoint = (point: Point) => {
        setActivePoint(point)
    }

    // Function to render popup content for a point
    const renderPopupContent = (data: any) => (
        <div>
            {JSON.stringify(data)}
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
            <MapContainer center={defaultCenter} zoom={13} style={{height: "100%", width: "100%"}} ref={mapRef}>
                <TileLayer
                    attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                    url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                />

                {/* Bus stops layer with circles - Red */}
                <LayerGroup>
                    {busStops.map((point) => (
                        <div key={point.stop_id}>
                            {/* Large circle with radius - rendered first */}
                            {/*<Circle*/}
                            {/*    center={[point.latitude, point.longitude]}*/}
                            {/*    radius={10}*/}
                            {/*    pathOptions={{*/}
                            {/*        color: "red",*/}
                            {/*        fillColor: "red",*/}
                            {/*        fillOpacity: 0.1,*/}
                            {/*        // Make the circle non-interactive for mouse events*/}
                            {/*        className: "pointer-events-none",*/}
                            {/*    }}*/}
                            {/*/>*/}

                            {/* Small circle marker - rendered on top */}
                            <CircleMarker
                                center={[point.latitude, point.longitude]}
                                radius={6}
                                pathOptions={{color: "red", fillColor: "red", fillOpacity: 0.8}}
                                eventHandlers={{
                                    click: () => centerOnPoint(point),
                                }}
                            >
                                <Popup>{renderPopupContent(point)}</Popup>
                            </CircleMarker>
                        </div>
                    ))}
                </LayerGroup>

                {/* Student addresses layer - Blue */}
                <LayerGroup>
                    {studentAddresses.map((point) => (
                        <CircleMarker
                            key={point.student_id}
                            center={[point.latitude, point.longitude]}
                            radius={6}
                            pathOptions={{color: "blue", fillColor: "blue", fillOpacity: 0.8}}
                            eventHandlers={{
                                click: () => centerOnPoint(point),
                            }}
                        >
                            <Popup>{renderPopupContent(point)}</Popup>
                        </CircleMarker>
                    ))}
                </LayerGroup>

                {/* Set view to active point if selected */}
                {activePoint && <SetViewOnClick
                    coords={{lat: activePoint.latitude, lng: activePoint.longitude}}/>}
            </MapContainer>
        </div>
    )
}
