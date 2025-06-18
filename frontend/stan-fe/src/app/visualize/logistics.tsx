"use client"

import { useState, useEffect } from "react"
import { useSession } from "next-auth/react"
import { Button } from "@/components/ui/button"
import { Loader2, AlertCircle, LineChart, BarChart, PieChart, Activity } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { useRouter } from "next/navigation"
import {
  LineChart as RechartsLineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  BarChart as RechartsBarChart,
  Bar,
  Cell,
  RadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  Radar,
  Scatter,
  ScatterChart,
  ZAxis,
} from "recharts"
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart"

interface Evaluation {
  user: number
  gain_percentage: number
  loss_percentage: number
  position_length: number
  algos_used: string[]
  intercept_range: number
  clean_range: number
  intercept_needed: number
  results: any[][]
  run_date: string
}

interface ProcessedEvaluation {
  id: number
  score: number
  buyWinRate: number
  sellWinRate: number
  totalWinRate: number
  buyActions: number
  sellActions: number
  totalActions: number
  algorithms: string[]
  positionLength: number
  gainPercentage: number
  lossPercentage: number
  interceptRange: number
  cleanRange: number
  interceptNeeded: number
  assetType: string
}

const CustomLegend = (props: any) => {
  const { payload } = props

  return (
    <div className="flex justify-center items-center gap-6 pt-2">
      {payload.map((entry: any, index: number) => (
        <div key={`item-${index}`} className="flex items-center gap-2">
          <div className="w-3 h-3 rounded-full" style={{ backgroundColor: entry.color }} />
          <span className="text-white text-sm font-medium">{entry.value}</span>
        </div>
      ))}
    </div>
  )
}

export default function Visualize() {
  const [evaluations, setEvaluations] = useState<Evaluation[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [processedData, setProcessedData] = useState<ProcessedEvaluation[]>([])

  const { data: session } = useSession()
  const router = useRouter()

  useEffect(() => {
    const fetchEvaluations = async () => {
      if (!session?.accessToken) {
        setError("No authentication token available")
        setLoading(false)
        return
      }
      try {
        const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/past-evals/`, {
          method: "GET",
          credentials: 'include',
          headers: {
            Authorization: `Bearer ${session.accessToken}`,
            "Content-Type": "application/json",
          },
        })

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`)
        }

        const data = await response.json()
        setEvaluations(data)
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to fetch evaluations")
      } finally {
        setLoading(false)
      }
    }

    fetchEvaluations()
  }, [session])

  useEffect(() => {
    if (evaluations.length > 0) {
      const processed = evaluations.map((evaluation, index) => {
        const mainResult = evaluation.results?.[0]?.[0]
        const [assetCode, assetName, score, buyWins, buyLoses, sellWins, sellLoses, buyActions, sellActions] =
          mainResult || []

        const buyWinRate = buyActions > 0 ? (buyWins / buyActions) * 100 : 0
        const sellWinRate = sellActions > 0 ? (sellWins / sellActions) * 100 : 0
        const totalActions = Number(buyActions) + Number(sellActions)
        const totalWins = Number(buyWins) + Number(sellWins)
        const totalWinRate = totalActions > 0 ? (totalWins / totalActions) * 100 : 0

        return {
          id: index,
          score: Number(score) || 0,
          buyWinRate,
          sellWinRate,
          totalWinRate,
          buyActions: Number(buyActions) || 0,
          sellActions: Number(sellActions) || 0,
          totalActions,
          algorithms: evaluation.algos_used,
          positionLength: evaluation.position_length,
          gainPercentage: evaluation.gain_percentage,
          lossPercentage: evaluation.loss_percentage,
          interceptRange: evaluation.intercept_range,
          cleanRange: evaluation.clean_range,
          interceptNeeded: evaluation.intercept_needed,
          assetType: mapAssetCodeToName(assetCode),
        }
      })

      setProcessedData(processed)
    }
  }, [evaluations])

  const mapAssetCodeToName = (assetCode: string): string => {
    switch (assetCode) {
      case "T":
        return "Quick Composite"
      case "S":
        return "Stock"
      case "C":
        return "Crypto"
      default:
        return assetCode || "Unknown"
    }
  }

  const calculateStats = () => {
    if (processedData.length === 0) return null

    const avgScore = processedData.reduce((sum, item) => sum + item.score, 0) / processedData.length
    const maxScore = Math.max(...processedData.map((item) => item.score))
    const minScore = Math.min(...processedData.map((item) => item.score))
    const avgWinRate = processedData.reduce((sum, item) => sum + item.totalWinRate, 0) / processedData.length
    const totalAlgorithms = [...new Set(processedData.flatMap((item) => item.algorithms))].length

    return {
      avgScore,
      maxScore,
      minScore,
      avgWinRate,
      totalAlgorithms,
      totalEvaluations: processedData.length,
    }
  }

  const stats = calculateStats()

  const prepareScoreData = () => {
    return processedData.map((item, index) => ({
      name: `Eval ${index + 1}`,
      score: item.score,
      winRate: item.totalWinRate,
    }))
  }

  const prepareWinRateData = () => {
    return processedData.map((item, index) => ({
      name: `Eval ${index + 1}`,
      buyWinRate: item.buyWinRate,
      sellWinRate: item.sellWinRate,
      totalWinRate: item.totalWinRate,
    }))
  }

  const prepareActionsData = () => {
    return processedData.map((item, index) => ({
      name: `Eval ${index + 1}`,
      buyActions: item.buyActions,
      sellActions: item.sellActions,
    }))
  }

  const prepareMetricsRadarData = () => {
    if (processedData.length === 0) return []

    const maxValues = {
      positionLength: Math.max(...processedData.map((item) => item.positionLength)),
      gainPercentage: Math.max(...processedData.map((item) => item.gainPercentage)),
      lossPercentage: Math.max(...processedData.map((item) => item.lossPercentage)),
      interceptRange: Math.max(...processedData.map((item) => item.interceptRange)),
      cleanRange: Math.max(...processedData.map((item) => item.cleanRange)),
      interceptNeeded: Math.max(...processedData.map((item) => item.interceptNeeded)),
    }

    return processedData.slice(0, 5).map((item, index) => ({
      evaluation: `Eval ${index + 1}`,
      positionLength: (item.positionLength / maxValues.positionLength) * 100,
      gainPercentage: (item.gainPercentage / maxValues.gainPercentage) * 100,
      lossPercentage: (item.lossPercentage / maxValues.lossPercentage) * 100,
      interceptRange: (item.interceptRange / maxValues.interceptRange) * 100,
      cleanRange: (item.cleanRange / maxValues.cleanRange) * 100,
      interceptNeeded: (item.interceptNeeded / maxValues.interceptNeeded) * 100,
    }))
  }

  const prepareScatterData = () => {
    return processedData.map((item) => ({
      x: item.score,
      y: item.totalWinRate,
      z: item.totalActions,
      name: `Eval ${item.id + 1}`,
    }))
  }

  const prepareAlgorithmDistribution = () => {
    const algoCount: Record<string, number> = {}
    processedData.forEach((item) => {
      item.algorithms.forEach((algo) => {
        algoCount[algo] = (algoCount[algo] || 0) + 1
      })
    })

    return Object.entries(algoCount).map(([name, value]) => ({
      name,
      value,
    }))
  }

  if (loading) {
    return (
      <div
        className="w-full min-h-[calc(100vh-64px)] flex items-center justify-center"
        style={{ backgroundColor: "#1c1e21" }}
      >
        <div className="text-slate-400 text-center flex flex-col items-center gap-3">
          <Loader2 className="h-8 w-8 animate-spin" />
          <p>Loading evaluation data...</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div
        className="w-full min-h-[calc(100vh-64px)] flex items-center justify-center"
        style={{ backgroundColor: "#1c1e21" }}
      >
        <div className="text-red-400 text-center flex flex-col items-center gap-3 max-w-md">
          <AlertCircle className="h-12 w-12" />
          <p className="text-xl font-medium">Error loading evaluation data</p>
          <p className="text-slate-400">{error}</p>
          <Button onClick={() => router.back()} variant="outline" className="mt-4">
            Go Back
          </Button>
        </div>
      </div>
    )
  }

  return (
    <div className="w-full min-h-[calc(100vh-64px)] pt-0" style={{ backgroundColor: "#1c1e21" }}>
      <div className="p-8">
        <div className="relative">
          <div className="relative">
            <div className="flex items-center gap-4 mb-3">
              <div className="w-12 h-12 bg-gradient-to-br from-teal-500 to-cyan-500 rounded-xl flex items-center justify-center shadow-lg shadow-teal-500/25">
                <BarChart className="h-6 w-6 text-white" />
              </div>

              <h1 className="text-5xl font-bold text-white tracking-tight">Evaluation Analytics</h1>
            </div>

            <div className="flex items-center">
              <p className="text-lg text-slate-400 font-medium">
                Visualizing data from your <span className="text-teal-400">{processedData.length}</span> evaluation
                {processedData.length !== 1 ? "s" : ""}
              </p>
            </div>

            <div className="mt-4 h-1 w-32 bg-gradient-to-r from-teal-500 to-cyan-500 rounded-full"></div>
          </div>
        </div>
      </div>

      <div className="relative mb-6 px-8">
        <div className="h-px bg-gradient-to-r from-transparent via-slate-600 to-transparent"></div>
      </div>

      <div className="px-8 pb-8">
        <div className="space-y-8">
          {stats && (
            <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-6 gap-4">
              <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                <CardHeader className="pb-2">
                  <CardTitle className="text-slate-300 text-sm">Evaluations</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-3xl font-bold text-white">{stats.totalEvaluations}</div>
                </CardContent>
              </Card>
              <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                <CardHeader className="pb-2">
                  <CardTitle className="text-slate-300 text-sm">Avg Score</CardTitle>
                </CardHeader>
                <CardContent>
                  <div
                    className={`text-3xl font-bold ${
                      stats.avgScore > 0 ? "text-green-400" : stats.avgScore < 0 ? "text-red-400" : "text-white"
                    }`}
                  >
                    {stats.avgScore.toFixed(2)}
                  </div>
                </CardContent>
              </Card>
              <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                <CardHeader className="pb-2">
                  <CardTitle className="text-slate-300 text-sm">Max Score</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-3xl font-bold text-green-400">{stats.maxScore.toFixed(2)}</div>
                </CardContent>
              </Card>
              <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                <CardHeader className="pb-2">
                  <CardTitle className="text-slate-300 text-sm">Min Score</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-3xl font-bold text-red-400">{stats.minScore.toFixed(2)}</div>
                </CardContent>
              </Card>
              <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                <CardHeader className="pb-2">
                  <CardTitle className="text-slate-300 text-sm">Avg Win Rate</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-3xl font-bold text-cyan-400">{stats.avgWinRate.toFixed(1)}%</div>
                </CardContent>
              </Card>
              <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                <CardHeader className="pb-2">
                  <CardTitle className="text-slate-300 text-sm">Algorithms</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-3xl font-bold text-teal-400">{stats.totalAlgorithms}</div>
                </CardContent>
              </Card>
            </div>
          )}

          {processedData.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-16 px-8">
              <div className="bg-slate-800/50 border border-slate-700 rounded-xl p-12 text-center max-w-2xl">
                <div className="mb-6">
                  <div className="w-24 h-24 bg-slate-700/50 rounded-full flex items-center justify-center mx-auto mb-4">
                    <BarChart className="h-12 w-12 text-slate-400" />
                  </div>
                  <h3 className="text-2xl font-bold text-white mb-2">No Evaluations Yet</h3>
                  <p className="text-slate-400 text-lg mb-6">
                    You haven't run any algorithm evaluations yet. Start by creating and running your first evaluation
                    to see analytics and visualizations here.
                  </p>
                  <div className="flex flex-col sm:flex-row gap-4 justify-center">
                    <Button
                      onClick={() => router.push("/evaluate")}
                      className="bg-gradient-to-r from-cyan-500 to-teal-500 hover:from-cyan-400 hover:to-teal-400 text-white font-medium px-8 py-3 rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02]"
                    >
                      Run Your First Evaluation
                    </Button>
                    <Button
                      onClick={() => router.push("/algorithms")}
                      variant="outline"
                      className="border-slate-600 text-slate-300 hover:bg-slate-700/50 hover:text-white px-8 py-3"
                    >
                      Browse Algorithms
                    </Button>
                  </div>
                </div>
              </div>
            </div>
          ) : (
            <Tabs defaultValue="performance" className="w-full">
              <TabsList className="bg-slate-800 border border-slate-700 p-1 mb-6">
                <TabsTrigger
                  value="performance"
                  className="data-[state=active]:bg-slate-700 data-[state=active]:text-white"
                >
                  <LineChart className="h-4 w-4 mr-2" />
                  Performance
                </TabsTrigger>
                <TabsTrigger
                  value="metrics"
                  className="data-[state=active]:bg-slate-700 data-[state=active]:text-white"
                >
                  <BarChart className="h-4 w-4 mr-2" />
                  Metrics
                </TabsTrigger>
                <TabsTrigger
                  value="comparison"
                  className="data-[state=active]:bg-slate-700 data-[state=active]:text-white"
                >
                  <PieChart className="h-4 w-4 mr-2" />
                  Comparison
                </TabsTrigger>
                <TabsTrigger
                  value="correlation"
                  className="data-[state=active]:bg-slate-700 data-[state=active]:text-white"
                >
                  <Activity className="h-4 w-4 mr-2" />
                  Correlation
                </TabsTrigger>
              </TabsList>

              <TabsContent value="performance" className="space-y-6">
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                    <CardHeader>
                      <CardTitle className="text-white">Score Trend</CardTitle>
                      <CardDescription className="text-slate-400">
                        Performance scores across evaluations
                      </CardDescription>
                    </CardHeader>
                    <CardContent className="pt-4">
                      <div className="h-80">
                        <ChartContainer
                          config={{
                            score: {
                              label: "Score",
                              color: "hsl(var(--chart-1))",
                            },
                            winRate: {
                              label: "Win Rate (%)",
                              color: "hsl(var(--chart-2))",
                            },
                          }}
                          className="h-full"
                        >
                          <ResponsiveContainer width="100%" height="100%">
                            <RechartsLineChart
                              data={prepareScoreData()}
                              margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
                            >
                              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                              <XAxis dataKey="name" stroke="#9CA3AF" />
                              <YAxis stroke="#9CA3AF" />
                              <ChartTooltip content={<ChartTooltipContent />} />
                              <Legend
                                content={CustomLegend}
                                formatter={(value) => {
                                  if (value === "score") return "Score"
                                  if (value === "winRate") return "Win Rate (%)"
                                  return value
                                }}
                              />
                              <Line
                                type="monotone"
                                dataKey="score"
                                stroke="var(--color-score)"
                                activeDot={{ r: 8 }}
                                strokeWidth={2}
                              />
                              <Line type="monotone" dataKey="winRate" stroke="var(--color-winRate)" strokeWidth={2} />
                            </RechartsLineChart>
                          </ResponsiveContainer>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>

                  <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                    <CardHeader>
                      <CardTitle className="text-white">Win Rate Analysis</CardTitle>
                      <CardDescription className="text-slate-400">Buy vs Sell win rates</CardDescription>
                    </CardHeader>
                    <CardContent className="pt-4">
                      <div className="h-80">
                        <ChartContainer
                          config={{
                            buyWinRate: {
                              label: "Buy Win Rate (%)",
                              color: "hsl(var(--chart-1))",
                            },
                            sellWinRate: {
                              label: "Sell Win Rate (%)",
                              color: "hsl(var(--chart-2))",
                            },
                            totalWinRate: {
                              label: "Total Win Rate (%)",
                              color: "hsl(var(--chart-3))",
                            },
                          }}
                          className="h-full"
                        >
                          <ResponsiveContainer width="100%" height="100%">
                            <RechartsBarChart
                              data={prepareWinRateData()}
                              margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
                            >
                              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                              <XAxis dataKey="name" stroke="#9CA3AF" />
                              <YAxis stroke="#9CA3AF" />
                              <ChartTooltip content={<ChartTooltipContent />} />
                              <Legend
                                content={CustomLegend}
                                formatter={(value) => {
                                  if (value === "buyWinRate") return "Buy Win Rate (%)"
                                  if (value === "sellWinRate") return "Sell Win Rate (%)"
                                  if (value === "totalWinRate") return "Total Win Rate (%)"
                                  return value
                                }}
                              />
                              <Bar dataKey="buyWinRate" fill="var(--color-buyWinRate)" />
                              <Bar dataKey="sellWinRate" fill="var(--color-sellWinRate)" />
                              <Bar dataKey="totalWinRate" fill="var(--color-totalWinRate)" />
                            </RechartsBarChart>
                          </ResponsiveContainer>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>
                </div>

                <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                  <CardHeader>
                    <CardTitle className="text-white">Actions Distribution</CardTitle>
                    <CardDescription className="text-slate-400">Buy vs Sell actions per evaluation</CardDescription>
                  </CardHeader>
                  <CardContent className="pt-4">
                    <div className="h-80">
                      <ChartContainer
                        config={{
                          buyActions: {
                            label: "Buy Actions",
                            color: "hsl(var(--chart-1))",
                          },
                          sellActions: {
                            label: "Sell Actions",
                            color: "hsl(var(--chart-2))",
                          },
                        }}
                        className="h-full"
                      >
                        <ResponsiveContainer width="100%" height="100%">
                          <RechartsBarChart
                            data={prepareActionsData()}
                            margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
                          >
                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                            <XAxis dataKey="name" stroke="#9CA3AF" />
                            <YAxis stroke="#9CA3AF" />
                            <ChartTooltip content={<ChartTooltipContent />} />
                            <Legend
                              content={CustomLegend}
                              formatter={(value) => {
                                if (value === "buyActions") return "Buy Actions"
                                if (value === "sellActions") return "Sell Actions"
                                return value
                              }}
                            />
                            <Bar dataKey="buyActions" fill="var(--color-buyActions)" />
                            <Bar dataKey="sellActions" fill="var(--color-sellActions)" />
                          </RechartsBarChart>
                        </ResponsiveContainer>
                      </ChartContainer>
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>

              <TabsContent value="metrics" className="space-y-6">
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                    <CardHeader>
                      <CardTitle className="text-white">Metrics Comparison</CardTitle>
                      <CardDescription className="text-slate-400">
                        Normalized metrics across top evaluations
                      </CardDescription>
                    </CardHeader>
                    <CardContent className="pt-4">
                      <div className="h-96">
                        <ResponsiveContainer width="100%" height="100%">
                          <RadarChart outerRadius={90} width={730} height={250} data={prepareMetricsRadarData()}>
                            <PolarGrid stroke="#374151" />
                            <PolarAngleAxis dataKey="evaluation" stroke="#9CA3AF" />
                            <PolarRadiusAxis angle={30} domain={[0, 100]} stroke="#9CA3AF" />
                            <Radar
                              name="Eval 1"
                              dataKey="positionLength"
                              stroke="#10B981"
                              fill="#10B981"
                              fillOpacity={0.3}
                            />
                            <Radar
                              name="Eval 2"
                              dataKey="gainPercentage"
                              stroke="#3B82F6"
                              fill="#3B82F6"
                              fillOpacity={0.3}
                            />
                            <Radar
                              name="Eval 3"
                              dataKey="lossPercentage"
                              stroke="#EF4444"
                              fill="#EF4444"
                              fillOpacity={0.3}
                            />
                            <Radar
                              name="Eval 4"
                              dataKey="interceptRange"
                              stroke="#F59E0B"
                              fill="#F59E0B"
                              fillOpacity={0.3}
                            />
                            <Radar
                              name="Eval 5"
                              dataKey="cleanRange"
                              stroke="#8B5CF6"
                              fill="#8B5CF6"
                              fillOpacity={0.3}
                            />
                            <Legend />
                          </RadarChart>
                        </ResponsiveContainer>
                      </div>
                    </CardContent>
                  </Card>

                  <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                    <CardHeader>
                      <CardTitle className="text-white">Algorithm Usage</CardTitle>
                      <CardDescription className="text-slate-400">
                        Distribution of algorithms across evaluations
                      </CardDescription>
                    </CardHeader>
                    <CardContent className="pt-4">
                      <div className="h-96">
                        <ChartContainer
                          config={{
                            value: {
                              label: "Count",
                              color: "hsl(var(--chart-1))",
                            },
                          }}
                          className="h-full"
                        >
                          <ResponsiveContainer width="100%" height="100%">
                            <RechartsBarChart
                              data={prepareAlgorithmDistribution()}
                              layout="vertical"
                              margin={{ top: 5, right: 30, left: 100, bottom: 5 }}
                            >
                              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                              <XAxis type="number" stroke="#9CA3AF" />
                              <YAxis
                                dataKey="name"
                                type="category"
                                stroke="#9CA3AF"
                                width={80}
                                tick={{ fontSize: 12 }}
                              />
                              <ChartTooltip content={<ChartTooltipContent />} />
                              <Legend content={CustomLegend} />
                              <Bar dataKey="value" fill="#06B6D4">
                                {prepareAlgorithmDistribution().map((entry, index) => (
                                  <Cell key={`cell-${index}`} fill={`hsl(${(index * 30) % 360}, 70%, 60%)`} />
                                ))}
                              </Bar>
                            </RechartsBarChart>
                          </ResponsiveContainer>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              </TabsContent>

              <TabsContent value="comparison" className="space-y-6">
                <div className="grid grid-cols-1 gap-6">
                  <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                    <CardHeader>
                      <CardTitle className="text-white">Evaluation Metrics Comparison</CardTitle>
                      <CardDescription className="text-slate-400">
                        Detailed comparison of all evaluation metrics
                      </CardDescription>
                    </CardHeader>
                    <CardContent className="pt-4">
                      <div className="overflow-x-auto">
                        <table className="w-full border-collapse">
                          <thead>
                            <tr className="border-b border-slate-700">
                              <th className="px-4 py-3 text-left text-sm font-medium text-slate-300">Evaluation</th>
                              <th className="px-4 py-3 text-left text-sm font-medium text-slate-300">Score</th>
                              <th className="px-4 py-3 text-left text-sm font-medium text-slate-300">Win Rate</th>
                              <th className="px-4 py-3 text-left text-sm font-medium text-slate-300">
                                Position Length
                              </th>
                              <th className="px-4 py-3 text-left text-sm font-medium text-slate-300">
                                Gain Percentage
                              </th>
                              <th className="px-4 py-3 text-left text-sm font-medium text-slate-300">
                                Loss Percentage
                              </th>
                              <th className="px-4 py-3 text-left text-sm font-medium text-slate-300">Asset Type</th>
                            </tr>
                          </thead>
                          <tbody>
                            {processedData.map((item, index) => (
                              <tr
                                key={index}
                                className="border-b border-slate-700 hover:bg-slate-700/30 transition-colors"
                              >
                                <td className="px-4 py-3 text-sm text-slate-300">Eval {index + 1}</td>
                                <td
                                  className={`px-4 py-3 text-sm font-medium ${
                                    item.score > 0 ? "text-green-400" : item.score < 0 ? "text-red-400" : "text-white"
                                  }`}
                                >
                                  {item.score.toFixed(2)}
                                </td>
                                <td className="px-4 py-3 text-sm text-cyan-400">{item.totalWinRate.toFixed(1)}%</td>
                                <td className="px-4 py-3 text-sm text-slate-300">{item.positionLength}</td>
                                <td className="px-4 py-3 text-sm text-green-400">{item.gainPercentage}</td>
                                <td className="px-4 py-3 text-sm text-red-400">{item.lossPercentage}</td>
                                <td className="px-4 py-3 text-sm text-slate-300">{item.assetType}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              </TabsContent>

              <TabsContent value="correlation" className="space-y-6">
                <div className="grid grid-cols-1 gap-6">
                  <Card className="bg-slate-800/50 border-slate-700 shadow-lg">
                    <CardHeader>
                      <CardTitle className="text-white">Score vs Win Rate Correlation</CardTitle>
                      <CardDescription className="text-slate-400">Bubble size represents total actions</CardDescription>
                    </CardHeader>
                    <CardContent className="pt-4">
                      <div className="h-96">
                        <ResponsiveContainer width="100%" height="100%">
                          <ScatterChart
                            margin={{
                              top: 20,
                              right: 20,
                              bottom: 30, // increased bottom margin to prevent label overlap
                              left: 20,
                            }}
                          >
                            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                            <XAxis
                              type="number"
                              dataKey="x"
                              name="Score"
                              stroke="#9CA3AF"
                              label={{
                                value: "Score",
                                position: "insideBottom",
                                offset: -29, 
                                fill: "#9CA3AF",
                              }}
                            />
                            <YAxis
                              type="number"
                              dataKey="y"
                              name="Win Rate"
                              stroke="#9CA3AF"
                              label={{
                                value: "Win Rate (%)",
                                angle: -90,
                                position: "insideLeft",
                                fill: "#9CA3AF",
                              }}
                            />
                            <ZAxis type="number" dataKey="z" range={[50, 400]} name="Total Actions" />
                            <Tooltip
                              cursor={{ strokeDasharray: "3 3" }}
                              content={({ active, payload }) => {
                                if (active && payload && payload.length) {
                                  const data = payload[0].payload
                                  return (
                                    <div className="bg-slate-800 p-3 border border-slate-700 rounded-md shadow-lg">
                                      <p className="text-white font-medium">{data.name}</p>
                                      <p className="text-slate-300">
                                        Score: <span className="text-cyan-400">{data.x.toFixed(2)}</span>
                                      </p>
                                      <p className="text-slate-300">
                                        Win Rate: <span className="text-cyan-400">{data.y.toFixed(1)}%</span>
                                      </p>
                                      <p className="text-slate-300">
                                        Total Actions: <span className="text-cyan-400">{data.z}</span>
                                      </p>
                                    </div>
                                  )
                                }
                                return null
                              }}
                            />
                            <Scatter name="Evaluations" data={prepareScatterData()} fill="#06B6D4" fillOpacity={0.6}>
                              {prepareScatterData().map((entry, index) => (
                                <Cell
                                  key={`cell-${index}`}
                                  fill={
                                    entry.x > 0
                                      ? `rgba(16, 185, 129, ${0.5 + entry.x / 2})`
                                      : `rgba(239, 68, 68, ${0.5 + Math.abs(entry.x) / 2})`
                                  }
                                />
                              ))}
                            </Scatter>
                          </ScatterChart>
                        </ResponsiveContainer>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              </TabsContent>
            </Tabs>
          )}
        </div>
      </div>
    </div>
  )
}
