// MapContext.tsx
import React, { createContext, useState } from "react";

export const MapContext = createContext<{
    mapData: {
        assignments: any,
        busStops: any,
        buses: any,
        busAssignments: any,
    },
    setMapData: (mapData: any) => void,
    mapCenter: any
    setMapCenter: (coords: any) => void,
    highlightPoint: any,
    setHighlightPoint: (point: any) => void,
}>({
    mapData: {
        assignments: [],
        busStops: [],
        buses: [],
        busAssignments: [],
    },
    setMapData: (data: any) => {},
    mapCenter: null,
    setMapCenter: (coords: { lat: number; lng: number }) => {},
    highlightPoint: null,
    setHighlightPoint: (point: any) => {},
});

export const MapProvider = ({ children }) => {
    const [mapData, setMapData] = useState(null);
    const [mapCenter, setMapCenter] = useState(null);
    const [highlightPoint, setHighlightPoint] = useState(null);

    return (
        <MapContext.Provider
            value={{ mapData, setMapData, mapCenter, setMapCenter, highlightPoint, setHighlightPoint }}
        >
            {children}
        </MapContext.Provider>
    );
};
