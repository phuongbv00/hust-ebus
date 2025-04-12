"use client"

import dynamic from "next/dynamic"

export default function Home() {
    // Dynamically import the Map component to avoid SSR issues with Leaflet
    const MapWithNoSSR = dynamic(() => import("@/components/map"), {
        ssr: false,
        loading: () => <div className="w-full h-screen flex items-center justify-center">Loading Map...</div>,
    })

    // Set the circle radius as a prop (in meters)
    const circleRadius = 500

    return (
        <main className="flex min-h-screen flex-col items-center justify-between">
            <MapWithNoSSR circleRadius={circleRadius} />
        </main>
    )
}
