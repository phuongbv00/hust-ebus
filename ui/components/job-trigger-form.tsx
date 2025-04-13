'use client'

import {useEffect, useRef, useState} from 'react'
import {Input} from '@/components/ui/input'
import {Button} from '@/components/ui/button'
import {Label} from '@/components/ui/label'
import {Card} from '@/components/ui/card'
import {cn} from '@/lib/utils'
import {CheckCircle, Loader2, XCircle} from 'lucide-react'

export default function JobFilterForm({className}: { className?: string }) {
    const [studentCount, setStudentCount] = useState(null)
    const [walkMaxDistance, setWalkMaxDistance] = useState(500)
    const [coverageRatio, setCoverageRatio] = useState(null)

    const [loading, setLoading] = useState(false)
    const [response, setResponse] = useState<any>(null)
    const [executions, setExecutions] = useState<any>(null)

    const pollingRef = useRef<NodeJS.Timeout | null>(null)

    const handleSubmit = async () => {
        setLoading(true)
        setResponse(null)
        setExecutions(null)

        const params = new URLSearchParams({
            walk_max_distance: walkMaxDistance.toString(),
        })

        if (studentCount) {
            params.append("student_count", studentCount)
        }

        if (coverageRatio) {
            params.append("coverage_ratio", coverageRatio)
        }

        try {
            const res = await fetch(`http://localhost:8000/job/uc01?${params}`)
            if (!res.ok) throw new Error(`Error: ${res.status}`)
            const data = await res.json()
            setResponse(data)

            // Start polling after the job is triggered
            startPollingExecutions()
        } catch (error: any) {
            console.error('Fetch error:', error)
            setResponse({error: error.message})
        }
    }

    const startPollingExecutions = () => {
        // Clear previous interval if exists
        if (pollingRef.current) {
            clearInterval(pollingRef.current)
        }

        pollingRef.current = setInterval(async () => {
            try {
                const res = await fetch('http://localhost:8000/job/uc01/executions')
                if (!res.ok) throw new Error(`Polling failed with status ${res.status}`)
                const data = await res.json().then(d => d.sort().reverse())

                setExecutions(data)

                // Example: Stop polling when the status is completed or failed
                if (!data.map(i => i.status).some(i => i === 0)) {
                    if (pollingRef.current) clearInterval(pollingRef.current)
                    pollingRef.current = null
                }
                setLoading(false)
            } catch (err) {
                console.error('Polling error:', err)
                if (pollingRef.current) clearInterval(pollingRef.current)
                pollingRef.current = null
                setLoading(false)
            }
        }, 2000) // polling every 2s
    }

    // Cleanup polling on unmount
    useEffect(() => {
        return () => {
            if (pollingRef.current) clearInterval(pollingRef.current)
        }
    }, [])

    return (
        <div className={cn('w-full space-y-4', className)}>
            <Card className="p-4 rounded-md gap-3">
                <div className="space-y-2">
                    <Label>Khoảng cách đi bộ tối đa (mét)</Label>
                    <Input
                        type="number"
                        value={walkMaxDistance}
                        onChange={(e) => setWalkMaxDistance(parseInt(e.target.value))}
                    />
                </div>

                <Button onClick={handleSubmit} disabled={loading}>
                    Thực thi luồng xử lý {loading && <Loader2 className="ml-2 h-4 w-4 animate-spin"/>}
                </Button>

                {executions && (
                    <div className="grid grid-cols-1 gap-3">
                        {executions.map((exec: any) => (
                            <Card
                                key={exec.id}
                                className="gap-3 p-3 rounded-md"
                            >
                                <div className="flex flex-col">
                                    <span className="text-sm font-medium text-gray-800">{exec.id}</span>
                                    <span className="text-sm text-gray-600">
                                        Thời gian xử lý: {exec.execution_time ? exec.execution_time.toFixed(2) + 's' : ''}
                                    </span>
                                </div>
                                <div className="flex items-center gap-2">
                                    {exec.status === 1 ? (
                                        <>
                                            <CheckCircle className="text-green-500 w-5 h-5"/>
                                            <span className="text-green-600 text-sm">Hoàn thành</span>
                                        </>
                                    ) : exec.status === 0 ? (
                                        <>
                                            <Loader2 className="text-blue-500 w-5 h-5 animate-spin"/>
                                            <span className="text-blue-600 text-sm">Đang xử lý</span>
                                        </>
                                    ) : exec.status === -1 ? (
                                        <>
                                            <XCircle className="text-red-500 w-5 h-5"/>
                                            <span className="text-red-600 text-sm">Lỗi xử lý</span>
                                        </>
                                    ) : ''}
                                </div>
                                {exec.error ? (
                                    <pre className="bg-muted p-3 rounded-md max-h-40 overflow-auto">{exec.error}</pre>
                                ) : ''}
                            </Card>
                        ))}
                    </div>
                )}
            </Card>
        </div>
    )
}