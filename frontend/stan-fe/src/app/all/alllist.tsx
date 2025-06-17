"use client"

import { useState, useEffect } from "react"
import { useSession } from "next-auth/react"
import { Button } from "@/components/ui/button"
import { BarChart3, Search, X, Loader2, ArrowUpDown } from 'lucide-react'
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Badge } from "@/components/ui/badge"
import { AlertCircle } from 'lucide-react'
import { useRouter } from "next/navigation"

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

interface TestResult {
  assetCode: string
  assetName: string
  score: number
  buyWins: number
  buyLoses: number
  sellWins: number
  sellLoses: number
  buyActions: number
  sellActions: number
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

export default function AllList() {
  const [evaluations, setEvaluations] = useState<Evaluation[]>([])
  const [filteredEvaluations, setFilteredEvaluations] = useState<Evaluation[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [searchTerm, setSearchTerm] = useState("")
  const [sortBy, setSortBy] = useState<string>("score-desc")
  const [selectedEvaluation, setSelectedEvaluation] = useState<Evaluation | null>(null)
  const [detailsOpen, setDetailsOpen] = useState(false)
  const [testResult, setTestResult] = useState<TestResult | null>(null)

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
        setFilteredEvaluations(data)
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to fetch evaluations")
      } finally {
        setLoading(false)
      }
    }

    fetchEvaluations()
  }, [session])

  useEffect(() => {
    let filteredList = [...evaluations]

    if (searchTerm) {
      filteredList = filteredList.filter((evaluation) =>
        evaluation.algos_used.some((algo) => algo.toLowerCase().includes(searchTerm.toLowerCase())),
      )
    }

    filteredList.sort((a, b) => {
      const scoreA = Number(a.results?.[0]?.[0]?.[2] || 0)
      const scoreB = Number(b.results?.[0]?.[0]?.[2] || 0)

      if (sortBy === "score-desc") {
        return scoreB - scoreA
      } else {
        return scoreA - scoreB
      }
    })

    setFilteredEvaluations(filteredList)
  }, [searchTerm, sortBy, evaluations])

  const handleViewDetails = (evaluation: Evaluation) => {
    setSelectedEvaluation(evaluation)

    const mainResult = evaluation.results?.[0]?.[0]

    if (mainResult && Array.isArray(mainResult)) {
      const [assetCode, assetName, score, buyWins, buyLoses, sellWins, sellLoses, buyActions, sellActions] = mainResult

      const result: TestResult = {
        assetCode,
        assetName,
        score: Number(score),
        buyWins: Number(buyWins),
        buyLoses: Number(buyLoses),
        sellWins: Number(sellWins),
        sellLoses: Number(sellLoses),
        buyActions: Number(buyActions),
        sellActions: Number(sellActions),
      }

      setTestResult(result)
    }

    setDetailsOpen(true)
  }

  const closeDetailsModal = () => {
    setDetailsOpen(false)
    setSelectedEvaluation(null)
    setTestResult(null)
  }

  const handleVisualize = () => {
    router.push("/visualize")
  }

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

  if (loading) {
    return (
      <div className="w-full">
        <div
          className="w-full min-h-[calc(100vh-96px)] flex items-center justify-center"
          style={{ backgroundColor: "#1c1e21" }}
        >
          <div className="text-slate-400 text-center flex flex-col items-center gap-3">
            <Loader2 className="h-8 w-8 animate-spin" />
            <p>Loading evaluations...</p>
          </div>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="w-full">
        <div
          className="w-full min-h-[calc(100vh-96px)] flex items-center justify-center"
          style={{ backgroundColor: "#1c1e21" }}
        >
          <div className="text-red-400 text-center flex flex-col items-center gap-3 max-w-md">
            <AlertCircle className="h-12 w-12" />
            <p className="text-xl font-medium">Error loading evaluations</p>
            <p className="text-slate-400">{error}</p>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="w-full">
      <div className="w-full min-h-[calc(100vh-96px)]" style={{ backgroundColor: "#1c1e21" }}>
        <div className="p-8">
          <div className="flex items-center justify-between">
            <div className="relative">
              <div className="flex items-center gap-4 mb-3">
                <div className="w-12 h-12 bg-gradient-to-br from-teal-500 to-cyan-500 rounded-xl flex items-center justify-center shadow-lg shadow-teal-500/25">
                  <BarChart3 className="h-6 w-6 text-white" />
                </div>
                <h1 className="text-5xl font-bold text-white tracking-tight">Past Evaluations</h1>
              </div>
              <div className="flex items-center">
                <p className="text-lg text-white font-medium">View and analyze your previous algorithm evaluations</p>
              </div>
            </div>
            <Button
              onClick={handleVisualize}
              className="bg-gradient-to-br from-slate-800 via-slate-900 to-black hover:from-slate-700 hover:via-slate-800 hover:to-slate-900 text-white font-bold px-12 py-4 h-16 text-lg rounded-2xl shadow-2xl shadow-slate-900/60 border border-slate-400/60 transition-all duration-300 hover:shadow-slate-800/80 hover:scale-[1.05] relative overflow-hidden group backdrop-blur-sm"
            >
              <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/40 to-transparent translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700"></div>
              <div className="absolute inset-0 bg-gradient-to-br from-white/20 via-transparent to-slate-200/30 opacity-70"></div>
              <div className="absolute inset-0 bg-gradient-to-t from-slate-200/20 via-transparent to-white/10 opacity-50"></div>
              <span className="relative z-10 tracking-wide">Visualize</span>
            </Button>
          </div>
        </div>

        <div className="px-8 pb-6">
          <div className="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50 shadow-lg">
            <div className="flex flex-col md:flex-row gap-4">
              <div className="flex-1 relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-slate-400 h-4 w-4" />
                <Input
                  placeholder="Search by algorithm name..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10 bg-slate-800/80 border-slate-700 text-white placeholder:text-slate-400 focus:border-teal-400"
                />
              </div>
              <div className="w-full md:w-64">
                <Select value={sortBy} onValueChange={setSortBy}>
                  <SelectTrigger className="bg-slate-800/80 border-slate-700 text-white">
                    <div className="flex items-center gap-2">
                      <ArrowUpDown className="h-4 w-4 text-slate-400" />
                      <SelectValue placeholder="Sort by" />
                    </div>
                  </SelectTrigger>
                  <SelectContent className="bg-slate-800 border-slate-700">
                    <SelectItem value="score-desc" className="text-white hover:bg-slate-700 focus:bg-slate-700">
                      Score: High to Low
                    </SelectItem>
                    <SelectItem value="score-asc" className="text-white hover:bg-slate-700 focus:bg-slate-700">
                      Score: Low to High
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>
        </div>

        <div className="px-8 pb-8">
          {filteredEvaluations.length === 0 ? (
            <div className="bg-slate-800/50 rounded-xl p-12 border border-slate-700/50 shadow-lg text-center">
              <p className="text-slate-400 text-lg">No evaluations found</p>
              <p className="text-slate-500 mt-2">Try adjusting your search filters</p>
            </div>
          ) : (
            <div className="space-y-4">
              {filteredEvaluations.map((evaluation, index) => {
                const score = evaluation.results?.[0]?.[0]?.[2] || 0
                const scoreValue = Number(score)

                const mainResult = evaluation.results?.[0]?.[0]
                const buyWins = Number(mainResult?.[3] || 0)
                const buyLoses = Number(mainResult?.[4] || 0)
                const sellWins = Number(mainResult?.[5] || 0)
                const sellLoses = Number(mainResult?.[6] || 0)
                const buyActions = Number(mainResult?.[7] || 0)
                const sellActions = Number(mainResult?.[8] || 0)
                const totalActions = buyActions + sellActions
                const totalWins = buyWins + sellWins
                const winRate = totalActions > 0 ? ((totalWins / totalActions) * 100).toFixed(1) : "0"

                return (
                  <div
                    key={index}
                    className="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50 shadow-lg hover:bg-slate-800/70 transition-colors duration-200"
                  >
                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                      <div className="relative h-full">
                        <div>
                          <h3 className="text-3xl font-semibold text-white mb-3">Algorithms Used</h3>
                          <div className="flex flex-wrap gap-2">
                            {evaluation.algos_used.map((algo, i) => (
                              <Badge
                                key={i}
                                className="bg-teal-500/20 hover:bg-teal-500/30 text-teal-300 border border-teal-500/30"
                              >
                                {algo}
                              </Badge>
                            ))}
                          </div>
                        </div>

                        <div className="absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/4 text-center">
                          <div
                            className={`text-7xl font-bold ${
                              scoreValue === 0
                                ? "text-white"
                                : scoreValue > 0
                                  ? scoreValue >= 0.5
                                    ? "text-green-400"
                                    : "text-green-300"
                                  : scoreValue <= -0.5
                                    ? "text-red-400"
                                    : "text-red-300"
                            }`}
                          >
                            {scoreValue.toFixed(2)}
                          </div>
                          <div className="text-slate-400 text-sm mt-1">Score</div>
                        </div>
                      </div>

                      <div className="space-y-4">
                        <h3 className="text-lg font-semibold text-white mb-3 pb-2 border-b border-slate-600/30">
                          Position Metrics
                        </h3>
                        <div className="grid grid-cols-2 gap-3">
                          <div className="bg-slate-700/30 p-3 rounded-lg">
                            <p className="text-slate-400 text-xs">Position Length</p>
                            <p className="text-white font-semibold text-lg">{evaluation.position_length}</p>
                          </div>
                          <div className="bg-slate-700/30 p-3 rounded-lg">
                            <p className="text-slate-400 text-xs">Gain %</p>
                            <p className="text-green-400 font-semibold text-lg">{evaluation.gain_percentage}</p>
                          </div>
                          <div className="bg-slate-700/30 p-3 rounded-lg">
                            <p className="text-slate-400 text-xs">Loss %</p>
                            <p className="text-red-400 font-semibold text-lg">{evaluation.loss_percentage}</p>
                          </div>
                          <div className="bg-slate-700/30 p-3 rounded-lg">
                            <p className="text-slate-400 text-xs">Win Rate</p>
                            <p className="text-cyan-400 font-semibold text-lg">{winRate}%</p>
                          </div>
                        </div>

                        <div className="grid grid-cols-3 gap-2">
                          <div className="bg-slate-700/20 p-2 rounded text-center">
                            <p className="text-slate-400 text-xs leading-tight">Intercept Range</p>
                            <p className="text-white font-medium">{evaluation.intercept_range}</p>
                          </div>
                          <div className="bg-slate-700/20 p-2 rounded text-center">
                            <p className="text-slate-400 text-xs leading-tight">
                              Clean
                              <br />
                              Range
                            </p>
                            <p className="text-white font-medium">{evaluation.clean_range}</p>
                          </div>
                          <div className="bg-slate-700/20 p-2 rounded text-center">
                            <p className="text-slate-400 text-xs leading-tight">Intercept Needed</p>
                            <p className="text-white font-medium">{evaluation.intercept_needed}</p>
                          </div>
                        </div>
                      </div>

                      <div className="space-y-4">
                        <h3 className="text-lg font-semibold text-white mb-3 pb-2 border-b border-slate-600/30">
                          Performance Stats
                        </h3>
                        <div className="grid grid-cols-2 gap-3">
                          <div className="bg-slate-700/30 p-3 rounded-lg text-center">
                            <div className="text-lg font-semibold text-teal-400">{buyWins}</div>
                            <div className="text-xs text-slate-400">Buy Wins</div>
                          </div>
                          <div className="bg-slate-700/30 p-3 rounded-lg text-center">
                            <div className="text-lg font-semibold text-red-400">{buyLoses}</div>
                            <div className="text-xs text-slate-400">Buy Losses</div>
                          </div>
                          <div className="bg-slate-700/30 p-3 rounded-lg text-center">
                            <div className="text-lg font-semibold text-teal-400">{sellWins}</div>
                            <div className="text-xs text-slate-400">Sell Wins</div>
                          </div>
                          <div className="bg-slate-700/30 p-3 rounded-lg text-center">
                            <div className="text-lg font-semibold text-red-400">{sellLoses}</div>
                            <div className="text-xs text-slate-400">Sell Losses</div>
                          </div>
                        </div>

                        <div className="grid grid-cols-2 gap-3">
                          <div className="bg-slate-700/20 p-2 rounded text-center">
                            <p className="text-slate-400 text-xs">Buy Actions</p>
                            <p className="text-white font-medium">{buyActions}</p>
                          </div>
                          <div className="bg-slate-700/20 p-2 rounded text-center">
                            <p className="text-slate-400 text-xs">Sell Actions</p>
                            <p className="text-white font-medium">{sellActions}</p>
                          </div>
                        </div>

                        <div className="pt-2">
                          <Button
                            onClick={() => handleViewDetails(evaluation)}
                            className="w-full bg-gradient-to-r from-cyan-500 to-teal-500 hover:from-cyan-400 hover:to-teal-400 text-white font-medium px-6 py-3 h-12 rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02]"
                          >
                            View Details
                          </Button>
                        </div>
                      </div>
                    </div>
                  </div>
                )
              })}
            </div>
          )}
        </div>

        {detailsOpen && selectedEvaluation && testResult && (
          <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center z-50 p-4">
            <div className="bg-slate-900 border border-slate-700 rounded-xl w-full max-w-3xl max-h-[80vh] flex flex-col shadow-2xl">
              <div className="flex items-center justify-between p-6 border-b border-slate-700">
                <h2 className="text-2xl font-bold text-white">Evaluation Details</h2>
                <button
                  onClick={closeDetailsModal}
                  className="p-2 text-white hover:bg-slate-700/50 rounded-lg transition-colors"
                  title="Close"
                >
                  <X className="h-5 w-5" />
                </button>
              </div>

              <div className="flex-1 overflow-auto p-6">
                <div className="space-y-6">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div className="space-y-3">
                      <h3 className="text-lg font-semibold text-white border-b border-slate-700 pb-2">Algorithms Used</h3>
                      <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-4 max-h-40 overflow-y-auto">
                        {selectedEvaluation.algos_used.length === 0 ? (
                          <p className="text-slate-400 text-sm">No algorithms selected</p>
                        ) : (
                          <ul className="space-y-2">
                            {selectedEvaluation.algos_used.map((algo, index) => (
                              <li key={index} className="text-slate-300 text-sm">
                                {algo}
                              </li>
                            ))}
                          </ul>
                        )}
                      </div>
                    </div>

                    <div className="space-y-3">
                      <h3 className="text-lg font-semibold text-white border-b border-slate-700 pb-2">Metrics</h3>
                      <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-4 max-h-40 overflow-y-auto">
                        <div className="space-y-2">
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Position Length:</span>
                            <span className="text-slate-300 text-sm">{selectedEvaluation.position_length}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Gain Percentage:</span>
                            <span className="text-slate-300 text-sm">{selectedEvaluation.gain_percentage}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Loss Percentage:</span>
                            <span className="text-slate-300 text-sm">{selectedEvaluation.loss_percentage}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Intercept Range:</span>
                            <span className="text-slate-300 text-sm">{selectedEvaluation.intercept_range}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Clean Range:</span>
                            <span className="text-slate-300 text-sm">{selectedEvaluation.clean_range}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Intercept Needed:</span>
                            <span className="text-slate-300 text-sm">{selectedEvaluation.intercept_needed}</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="bg-slate-800/80 border border-slate-700 rounded-xl p-6 space-y-6">
                    <div className="flex items-center justify-between border-b border-slate-700 pb-3">
                      <div className="flex-1"></div>
                      <h3 className="text-xl font-semibold text-white">Results</h3>
                      <div className="flex-1"></div>
                    </div>

                    <div className="flex flex-col items-center justify-center">
                      <div
                        className={`text-6xl font-bold ${
                          testResult.score === 0
                            ? "text-white"
                            : testResult.score > 0
                              ? testResult.score >= 0.5
                                ? "text-green-400"
                                : "text-green-300"
                              : testResult.score <= -0.5
                                ? "text-red-400"
                                : "text-red-300"
                        }`}
                      >
                        {testResult.score.toFixed(2)}
                      </div>
                      <div className="text-slate-400 text-sm mt-1">Score</div>
                    </div>

                    <div className="flex justify-center">
                      <div className="px-4 py-1 bg-slate-700/50 rounded-full">
                        <span className="text-slate-300 text-sm">{mapAssetCodeToName(testResult.assetCode)}</span>
                      </div>
                    </div>

                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      <div className="bg-slate-700/30 p-3 rounded-lg text-center">
                        <div className="text-xl font-semibold text-teal-400">{testResult.buyWins}</div>
                        <div className="text-xs text-slate-400">Buy Wins</div>
                      </div>
                      <div className="bg-slate-700/30 p-3 rounded-lg text-center">
                        <div className="text-xl font-semibold text-red-400">{testResult.buyLoses}</div>
                        <div className="text-xs text-slate-400">Buy Losses</div>
                      </div>
                      <div className="bg-slate-700/30 p-3 rounded-lg text-center">
                        <div className="text-xl font-semibold text-teal-400">{testResult.sellWins}</div>
                        <div className="text-xs text-slate-400">Sell Wins</div>
                      </div>
                      <div className="bg-slate-700/30 p-3 rounded-lg text-center">
                        <div className="text-xl font-semibold text-red-400">{testResult.sellLoses}</div>
                        <div className="text-xs text-slate-400">Sell Losses</div>
                      </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div className="bg-slate-700/30 p-3 rounded-lg">
                        <div className="flex justify-between items-center">
                          <span className="text-slate-300">Total Buy Actions:</span>
                          <span className="text-lg font-semibold text-white">{testResult.buyActions}</span>
                        </div>
                        {testResult.buyActions > 0 && (
                          <div className="mt-2 h-2 bg-slate-600 rounded-full overflow-hidden">
                            <div
                              className="h-full bg-gradient-to-r from-teal-500 to-cyan-500"
                              style={{
                                width: `${(testResult.buyWins / testResult.buyActions) * 100}%`,
                              }}
                            ></div>
                          </div>
                        )}
                        <div className="flex justify-between text-xs mt-1">
                          <span className="text-slate-400">
                            Win Rate:{" "}
                            {testResult.buyActions > 0
                              ? ((testResult.buyWins / testResult.buyActions) * 100).toFixed(1)
                              : 0}
                            %
                          </span>
                          <span className="text-slate-400">
                            {testResult.buyWins} / {testResult.buyActions}
                          </span>
                        </div>
                      </div>
                      <div className="bg-slate-700/30 p-3 rounded-lg">
                        <div className="flex justify-between items-center">
                          <span className="text-slate-300">Total Sell Actions:</span>
                          <span className="text-lg font-semibold text-white">{testResult.sellActions}</span>
                        </div>
                        {testResult.sellActions > 0 && (
                          <div className="mt-2 h-2 bg-slate-600 rounded-full overflow-hidden">
                            <div
                              className="h-full bg-gradient-to-r from-teal-500 to-cyan-500"
                              style={{
                                width: `${(testResult.sellWins / testResult.sellActions) * 100}%`,
                              }}
                            ></div>
                          </div>
                        )}
                        <div className="flex justify-between text-xs mt-1">
                          <span className="text-slate-400">
                            Win Rate:{" "}
                            {testResult.sellActions > 0
                              ? ((testResult.sellWins / testResult.sellActions) * 100).toFixed(1)
                              : 0}
                            %
                          </span>
                          <span className="text-slate-400">
                            {testResult.sellWins} / {testResult.sellActions}
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}