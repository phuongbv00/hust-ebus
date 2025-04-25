// MapContext.tsx
import React, { createContext, useState } from "react";

export const MapContext = createContext({
    mapData: {
        assignments: [],
        busStops: [],
        buses: [],
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
