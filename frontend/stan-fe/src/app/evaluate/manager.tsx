"use client"
import { useState, useEffect } from "react"
import { useSession } from "next-auth/react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { BarChart3, Eye, X, Copy, CheckCircle, AlertCircle, Globe, User, TrendingUp, Loader2 } from "lucide-react"
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter"
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism"
import { useRouter } from "next/navigation"

interface PublicAlgorithm {
  algoname: string
  summary: string
}

interface UserAlgorithm {
  algoname: string
  description: string
  date_added: string
}

interface SelectedAlgorithm {
  name: string
  type: "public" | "user"
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

export default function Manager() {
  const [publicAlgorithms, setPublicAlgorithms] = useState<PublicAlgorithm[]>([])
  const [userAlgorithms, setUserAlgorithms] = useState<UserAlgorithm[]>([])
  const [selectedAlgorithms, setSelectedAlgorithms] = useState<SelectedAlgorithm[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [previewOpen, setPreviewOpen] = useState(false)
  const [previewCode, setPreviewCode] = useState("")
  const [previewAlgoName, setPreviewAlgoName] = useState("")
  const [previewDescription, setPreviewDescription] = useState("")
  const [previewLoading, setPreviewLoading] = useState(false)

  const [resultsOpen, setResultsOpen] = useState(false)
  const [runTestLoading, setRunTestLoading] = useState(false)
  const [testResult, setTestResult] = useState<TestResult | null>(null)

  const [positionLength, setPositionLength] = useState("")
  const [gainPercentage, setGainPercentage] = useState("")
  const [lossPercentage, setLossPercentage] = useState("")
  const [interceptRange, setInterceptRange] = useState("")
  const [cleanRange, setCleanRange] = useState("")
  const [interceptNeeded, setInterceptNeeded] = useState("")
  const [selectedAsset, setSelectedAsset] = useState("")

  const [formErrors, setFormErrors] = useState<{ [key: string]: string }>({})
  const [notification, setNotification] = useState<{
    type: "success" | "error"
    message: string
  } | null>(null)

  const { data: session } = useSession()
  const router = useRouter()

  useEffect(() => {
    if (notification) {
      const timer = setTimeout(() => {
        setNotification(null)
      }, 3000)
      return () => clearTimeout(timer)
    }
  }, [notification])

  useEffect(() => {
    const fetchAlgorithms = async () => {
      if (!session?.accessToken) {
        setError("No authentication token available")
        setLoading(false)
        return
      }

      try {
        const publicResponse = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/scripts/public/`, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${session.accessToken}`,
            "Content-Type": "application/json",
          },
        })

        const userResponse = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/scripts/user/`, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${session.accessToken}`,
            "Content-Type": "application/json",
          },
        })

        if (!publicResponse.ok || !userResponse.ok) {
          throw new Error("Failed to fetch algorithms")
        }

        const publicData = await publicResponse.json()
        const userData = await userResponse.json()

        setPublicAlgorithms(publicData.algorithms || [])
        setUserAlgorithms(userData.algorithms || [])
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to fetch algorithms")
      } finally {
        setLoading(false)
      }
    }

    if (session?.accessToken) {
      fetchAlgorithms()
    }
  }, [session])

  const handleAlgorithmSelect = (name: string, type: "public" | "user") => {
    const isSelected = selectedAlgorithms.some((algo) => algo.name === name && algo.type === type)

    if (isSelected) {
      setSelectedAlgorithms((prev) => prev.filter((algo) => !(algo.name === name && algo.type === type)))
    } else {
      setSelectedAlgorithms((prev) => [...prev, { name, type }])
    }
  }

  const isAlgorithmSelected = (name: string, type: "public" | "user") => {
    return selectedAlgorithms.some((algo) => algo.name === name && algo.type === type)
  }

  const handlePreview = async (algoname: string, type: "public" | "user", description?: string) => {
    if (!session?.accessToken) {
      setError("No authentication token available")
      return
    }

    setPreviewLoading(true)
    setPreviewOpen(true)
    setPreviewAlgoName(algoname)
    setPreviewDescription(description || "")

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/scripts/preview/`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${session.accessToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          algo_type: type,
          algoname: algoname,
        }),
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const data = await response.json()
      setPreviewCode(data.code || "")
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch algorithm preview")
      setPreviewOpen(false)
    } finally {
      setPreviewLoading(false)
    }
  }

  const validateForm = () => {
    const errors: { [key: string]: string } = {}

    // Algorithm selection validation
    if (selectedAlgorithms.length === 0) {
      errors.selectedAlgorithms = "Note: Please select at least one algorithm to test."
    }

    // Position length validation
    const posLengthNum = Number.parseInt(positionLength)
    if (!positionLength || isNaN(posLengthNum) || posLengthNum <= 0) {
      errors.positionLength = "Must be a positive integer"
    }

    // Gain percentage validation
    const gainNum = Number.parseFloat(gainPercentage)
    if (!gainPercentage || isNaN(gainNum) || gainNum <= 0) {
      errors.gainPercentage = "Must be a positive decimal"
    }

    // Loss percentage validation
    const lossNum = Number.parseFloat(lossPercentage)
    if (!lossPercentage || isNaN(lossNum) || lossNum <= 0) {
      errors.lossPercentage = "Must be a positive decimal"
    }

    // Intercept range validation
    const interceptRangeNum = Number.parseInt(interceptRange)
    if (!interceptRange || isNaN(interceptRangeNum) || interceptRangeNum <= 0) {
      errors.interceptRange = "Must be a positive integer"
    }

    // Clean range validation
    const cleanRangeNum = Number.parseInt(cleanRange)
    if (!cleanRange || isNaN(cleanRangeNum) || cleanRangeNum <= 0) {
      errors.cleanRange = "Must be a positive integer"
    }

    // Intercept needed validation
    const interceptNeededNum = Number.parseInt(interceptNeeded)
    if (!interceptNeeded || isNaN(interceptNeededNum) || interceptNeededNum <= 0) {
      errors.interceptNeeded = "Must be a positive integer"
    }

    // Asset selection validation
    if (!selectedAsset) {
      errors.selectedAsset = "Please select an asset type"
    }

    setFormErrors(errors)
    return Object.keys(errors).length === 0
  }

  const mapAssetTypeToCode = (assetType: string): string => {
    switch (assetType) {
      case "fast-composite":
        return "T"
      case "stock":
        return "S"
      case "crypto":
        return "C"
      default:
        return "T"
    }
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
        return assetCode
    }
  }

  const handleRunTest = async () => {
    if (!validateForm()) {
      setNotification({
        type: "error",
        message: "Please fix the form errors before running the test",
      })
      return
    }

    if (!session?.accessToken) {
      setNotification({
        type: "error",
        message: "No authentication token available",
      })
      return
    }

    setRunTestLoading(true)
    setResultsOpen(true)

    try {
      // Separate selected algorithms by type
      const publicAlgos = selectedAlgorithms.filter((algo) => algo.type === "public").map((algo) => algo.name)
      const myAlgos = selectedAlgorithms.filter((algo) => algo.type === "user").map((algo) => algo.name)

      const payload = {
        my_algos: myAlgos,
        public_algos: publicAlgos,
        assets: mapAssetTypeToCode(selectedAsset),
        position_length: Number.parseInt(positionLength),
        gain_percentage: Number.parseFloat(gainPercentage),
        loss_percentage: Number.parseFloat(lossPercentage),
        intercept_range: Number.parseInt(interceptRange),
        clean_range: Number.parseInt(cleanRange),
        intercept_needed: Number.parseInt(interceptNeeded),
      }

      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/scripts/run/`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${session.accessToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const data = await response.json()

      // Extract the main result array: ['T', 'Test', 0.0, 0, 0, 0, 0, 0, 0]
      const mainResult = data.results?.[0]?.[0]

      if (mainResult && Array.isArray(mainResult)) {
        const [assetCode, assetName, score, buyWins, buyLoses, sellWins, sellLoses, buyActions, sellActions] =
          mainResult

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
      } else {
        throw new Error("Invalid response format")
      }
    } catch (err) {
      console.error("Test run error:", err)
      setNotification({
        type: "error",
        message: err instanceof Error ? err.message : "Failed to run test",
      })
      setResultsOpen(false)
    } finally {
      setRunTestLoading(false)
    }
  }

  const closeResultsModal = () => {
    setResultsOpen(false)
    setTestResult(null)

    // Reset form fields
    setPositionLength("")
    setGainPercentage("")
    setLossPercentage("")
    setInterceptRange("")
    setCleanRange("")
    setInterceptNeeded("")
    setSelectedAsset("")
    setSelectedAlgorithms([])
  }

  const copyToClipboard = async () => {
    try {
      await navigator.clipboard.writeText(previewCode)
    } catch (err) {
      console.error("Failed to copy code:", err)
    }
  }

  const closePreview = () => {
    setPreviewOpen(false)
    setPreviewCode("")
    setPreviewAlgoName("")
    setPreviewDescription("")
  }

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString("en-US", {
      year: "numeric",
      month: "short",
      day: "numeric",
    })
  }

  const truncateDescription = (text: string, maxLength = 40) => {
    if (text.length <= maxLength) return text
    return text.substring(0, maxLength) + "..."
  }

  if (loading) {
    return (
      <div
        className="w-full min-h-[calc(100vh-64px)] flex items-center justify-center"
        style={{ backgroundColor: "#1c1e21" }}
      >
        <div className="text-slate-400 text-center">
          <p>Loading algorithms...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="w-full min-h-[calc(100vh-64px)]" style={{ backgroundColor: "#1c1e21" }}>
      {/* Notification Banner */}
      {notification && (
        <div
          className={`fixed top-4 left-1/2 transform -translate-x-1/2 z-50 px-6 py-3 rounded-lg shadow-lg border transition-all duration-300 ${
            notification.type === "success"
              ? "bg-green-900/90 border-green-700 text-green-200"
              : "bg-red-900/90 border-red-700 text-red-200"
          }`}
        >
          <div className="flex items-center gap-2">
            {notification.type === "success" ? (
              <CheckCircle className="h-4 w-4" />
            ) : (
              <AlertCircle className="h-4 w-4" />
            )}
            <span className="font-medium">{notification.message}</span>
          </div>
        </div>
      )}

      {/* Header section - Darker background */}
      <div className="p-8">
        <div className="flex items-center justify-between">
          <div className="relative">
            <div className="flex items-center gap-4 mb-3">
              <div className="w-12 h-12 bg-gradient-to-br from-teal-500 to-cyan-500 rounded-xl flex items-center justify-center shadow-lg shadow-teal-500/25">
                <BarChart3 className="h-6 w-6 text-white" />
              </div>
              <h1 className="text-5xl font-bold text-white tracking-tight">Evaluate Algorithms</h1>
            </div>
            <div className="flex items-center">
              <p className="text-lg text-white font-medium">Test and evaluate trading algorithms</p>
            </div>
          </div>
          <Button
            onClick={() => router.push("/all")}
            className="bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-6 py-2.5 h-auto rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02] relative overflow-hidden group"
          >
            <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700"></div>
            <span className="relative z-10">See Past Evaluations</span>
          </Button>
        </div>
      </div>

      <div className="p-8">
        {/* Middle section - Bluish grey background with algorithms */}
        <div className="mb-8">
          <div className="grid grid-cols-[1fr_1px_1fr] gap-8">
            {/* Left side - Public Algorithms */}
            <div className="space-y-4">
              <h2 className="text-2xl font-semibold text-white tracking-wide border-b border-slate-600/30 pb-2">
                <div className="flex items-center gap-2">
                  <Globe className="h-6 w-6 text-slate-400" />
                  <span>Public Algorithms</span>
                </div>
              </h2>
              <div className="space-y-3 max-h-96 overflow-y-auto pr-2">
                {publicAlgorithms.length === 0 ? (
                  <div className="text-center py-8">
                    <p className="text-slate-400">No public algorithms available</p>
                  </div>
                ) : (
                  publicAlgorithms.map((algorithm, index) => (
                    <div
                      key={index}
                      className={`relative p-4 bg-white/5 border rounded-lg hover:bg-white/10 transition-all duration-200 group ${
                        isAlgorithmSelected(algorithm.algoname, "public")
                          ? "border-green-500/50 bg-green-500/10"
                          : "border-slate-600/30 hover:border-slate-500/50"
                      }`}
                    >
                      <div className="flex items-start justify-between">
                        <div className="flex-1 min-w-0">
                          <h3 className="text-white font-medium text-base mb-2 truncate">{algorithm.algoname}</h3>
                          <p className="text-slate-300 text-sm leading-relaxed">
                            {truncateDescription(algorithm.summary)}
                          </p>
                        </div>
                        <div className="flex items-center gap-2 ml-4 flex-shrink-0">
                          <Button
                            size="sm"
                            onClick={() => handlePreview(algorithm.algoname, "public", algorithm.summary)}
                            className="bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-400 hover:text-emerald-300 border border-emerald-500/30 hover:border-emerald-500/50 h-8 w-8 p-0"
                          >
                            <Eye className="h-3.5 w-3.5" />
                          </Button>
                          <Button
                            size="sm"
                            onClick={() => handleAlgorithmSelect(algorithm.algoname, "public")}
                            className={`px-6 py-2 h-8 rounded-md border-2 transition-all duration-200 ${
                              isAlgorithmSelected(algorithm.algoname, "public")
                                ? "bg-red-500 border-red-500 text-white hover:bg-red-600"
                                : "bg-transparent border-green-500 text-green-400 hover:border-green-400 hover:text-green-300"
                            }`}
                          >
                            {isAlgorithmSelected(algorithm.algoname, "public") ? "Drop" : "Use"}
                          </Button>
                        </div>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>

            {/* Sleek divider */}
            <div className="bg-gradient-to-b from-transparent via-slate-600/50 to-transparent"></div>

            {/* Right side - Your Algorithms */}
            <div className="space-y-4">
              <h2 className="text-2xl font-semibold text-white tracking-wide border-b border-slate-600/30 pb-2">
                <div className="flex items-center gap-2">
                  <User className="h-6 w-6 text-slate-400" />
                  <span>Your Algorithms</span>
                </div>
              </h2>
              <div className="space-y-3 max-h-96 overflow-y-auto pr-2">
                {userAlgorithms.length === 0 ? (
                  <div className="text-center py-8">
                    <p className="text-slate-400">No algorithms created yet</p>
                  </div>
                ) : (
                  userAlgorithms.map((algorithm, index) => (
                    <div
                      key={index}
                      className={`relative p-4 bg-white/5 border rounded-lg hover:bg-white/10 transition-all duration-200 group ${
                        isAlgorithmSelected(algorithm.algoname, "user")
                          ? "border-green-500/50 bg-green-500/10"
                          : "border-slate-600/30 hover:border-slate-500/50"
                      }`}
                    >
                      <div className="flex items-start justify-between">
                        <div className="flex-1 min-w-0">
                          <h3 className="text-white font-medium text-base mb-2 truncate">{algorithm.algoname}</h3>
                          <p className="text-slate-300 text-sm leading-relaxed mb-1">
                            {truncateDescription(algorithm.description)}
                          </p>
                          <p className="text-slate-500 text-xs">{formatDate(algorithm.date_added)}</p>
                        </div>
                        <div className="flex items-center gap-2 ml-4 flex-shrink-0">
                          <Button
                            size="sm"
                            onClick={() => handlePreview(algorithm.algoname, "user", algorithm.description)}
                            className="bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-400 hover:text-emerald-300 border border-emerald-500/30 hover:border-emerald-500/50 h-8 w-8 p-0"
                          >
                            <Eye className="h-3.5 w-3.5" />
                          </Button>
                          <Button
                            size="sm"
                            onClick={() => handleAlgorithmSelect(algorithm.algoname, "user")}
                            className={`px-6 py-2 h-8 rounded-md border-2 transition-all duration-200 ${
                              isAlgorithmSelected(algorithm.algoname, "user")
                                ? "bg-red-500 border-red-500 text-white hover:bg-red-600"
                                : "bg-transparent border-green-500 text-green-400 hover:border-green-400 hover:text-green-300"
                            }`}
                          >
                            {isAlgorithmSelected(algorithm.algoname, "user") ? "Drop" : "Use"}
                          </Button>
                        </div>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Bottom section - Position Metrics with darker background */}
        <div className="bg-slate-800/80 rounded-xl p-8 shadow-lg border border-slate-700/50">
          <h2 className="text-2xl font-semibold text-white tracking-wide mb-6 border-b border-slate-600/30 pb-2">
            <div className="flex items-center gap-2">
              <TrendingUp className="h-6 w-6 text-slate-400" />
              <span>Position Metrics</span>
            </div>
          </h2>

          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              <div className="space-y-2">
                <Label htmlFor="positionLength" className="text-sm text-white font-medium">
                  Position Length
                </Label>
                <Input
                  id="positionLength"
                  type="number"
                  min="1"
                  value={positionLength}
                  onChange={(e) => setPositionLength(e.target.value)}
                  placeholder="must be positive integer"
                  className={`bg-transparent border-x-0 border-t-0 border-b-2 rounded-none text-slate-300 placeholder:text-slate-400 focus:border-teal-400 focus:ring-0 h-10 px-0 [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none ${
                    formErrors.positionLength ? "border-b-red-500" : "border-b-slate-600"
                  }`}
                />
                {formErrors.positionLength && <p className="text-red-400 text-xs">{formErrors.positionLength}</p>}
              </div>

              <div className="space-y-2">
                <Label htmlFor="gainPercentage" className="text-sm text-white font-medium">
                  Gain Percentage
                </Label>
                <Input
                  id="gainPercentage"
                  type="number"
                  step="0.01"
                  min="0.01"
                  value={gainPercentage}
                  onChange={(e) => setGainPercentage(e.target.value)}
                  placeholder="must be positive decimal"
                  className={`bg-transparent border-x-0 border-t-0 border-b-2 rounded-none text-slate-300 placeholder:text-slate-400 focus:border-teal-400 focus:ring-0 h-10 px-0 [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none ${
                    formErrors.gainPercentage ? "border-b-red-500" : "border-b-slate-600"
                  }`}
                />
                {formErrors.gainPercentage && <p className="text-red-400 text-xs">{formErrors.gainPercentage}</p>}
              </div>

              <div className="space-y-2">
                <Label htmlFor="lossPercentage" className="text-sm text-white font-medium">
                  Loss Percentage
                </Label>
                <Input
                  id="lossPercentage"
                  type="number"
                  step="0.01"
                  min="0.01"
                  value={lossPercentage}
                  onChange={(e) => setLossPercentage(e.target.value)}
                  placeholder="must be positive decimal"
                  className={`bg-transparent border-x-0 border-t-0 border-b-2 rounded-none text-slate-300 placeholder:text-slate-400 focus:border-teal-400 focus:ring-0 h-10 px-0 [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none ${
                    formErrors.lossPercentage ? "border-b-red-500" : "border-b-slate-600"
                  }`}
                />
                {formErrors.lossPercentage && <p className="text-red-400 text-xs">{formErrors.lossPercentage}</p>}
              </div>

              <div className="space-y-2">
                <Label htmlFor="interceptRange" className="text-sm text-white font-medium">
                  Intercept Range
                </Label>
                <Input
                  id="interceptRange"
                  type="number"
                  min="1"
                  value={interceptRange}
                  onChange={(e) => setInterceptRange(e.target.value)}
                  placeholder="must be positive integer"
                  className={`bg-transparent border-x-0 border-t-0 border-b-2 rounded-none text-slate-300 placeholder:text-slate-400 focus:border-teal-400 focus:ring-0 h-10 px-0 [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none ${
                    formErrors.interceptRange ? "border-b-red-500" : "border-b-slate-600"
                  }`}
                />
                {formErrors.interceptRange && <p className="text-red-400 text-xs">{formErrors.interceptRange}</p>}
              </div>

              <div className="space-y-2">
                <Label htmlFor="cleanRange" className="text-sm text-white font-medium">
                  Clean Range
                </Label>
                <Input
                  id="cleanRange"
                  type="number"
                  min="1"
                  value={cleanRange}
                  onChange={(e) => setCleanRange(e.target.value)}
                  placeholder="must be positive integer"
                  className={`bg-transparent border-x-0 border-t-0 border-b-2 rounded-none text-slate-300 placeholder:text-slate-400 focus:border-teal-400 focus:ring-0 h-10 px-0 [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none ${
                    formErrors.cleanRange ? "border-b-red-500" : "border-b-slate-600"
                  }`}
                />
                {formErrors.cleanRange && <p className="text-red-400 text-xs">{formErrors.cleanRange}</p>}
              </div>

              <div className="space-y-2">
                <Label htmlFor="interceptNeeded" className="text-sm text-white font-medium">
                  Intercept Needed
                </Label>
                <Input
                  id="interceptNeeded"
                  type="number"
                  min="1"
                  value={interceptNeeded}
                  onChange={(e) => setInterceptNeeded(e.target.value)}
                  placeholder="must be positive integer"
                  className={`bg-transparent border-x-0 border-t-0 border-b-2 rounded-none text-slate-300 placeholder:text-slate-400 focus:border-teal-400 focus:ring-0 h-10 px-0 [appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none ${
                    formErrors.interceptNeeded ? "border-b-red-500" : "border-b-slate-600"
                  }`}
                />
                {formErrors.interceptNeeded && <p className="text-red-400 text-xs">{formErrors.interceptNeeded}</p>}
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 items-end">
              <div className="space-y-2">
                <Label htmlFor="assetType" className="text-sm text-white font-medium">
                  Asset Type
                </Label>
                <Select value={selectedAsset} onValueChange={setSelectedAsset}>
                  <SelectTrigger
                    className={`bg-transparent border-x-0 border-t-0 border-b-2 rounded-none text-slate-300 h-10 px-0 hover:border-slate-500 ${
                      formErrors.selectedAsset ? "border-b-red-500" : "border-b-slate-600"
                    }`}
                  >
                    <SelectValue placeholder="Select asset type" />
                  </SelectTrigger>
                  <SelectContent className="bg-slate-800 border-slate-700">
                    <SelectItem value="crypto" className="text-white hover:bg-slate-700 focus:bg-slate-700">
                      Crypto
                    </SelectItem>
                    <SelectItem value="stock" className="text-white hover:bg-slate-700 focus:bg-slate-700">
                      Stock
                    </SelectItem>
                    <SelectItem value="fast-composite" className="text-white hover:bg-slate-700 focus:bg-slate-700">
                      Fast Composite
                    </SelectItem>
                  </SelectContent>
                </Select>
                {formErrors.selectedAsset && <p className="text-red-400 text-xs">{formErrors.selectedAsset}</p>}
              </div>

              <div className="space-y-2">
                {formErrors.selectedAlgorithms && (
                  <p className="text-red-400 text-sm font-medium">{formErrors.selectedAlgorithms}</p>
                )}
                <Button
                  onClick={handleRunTest}
                  disabled={runTestLoading}
                  className="w-full bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-12 py-3 h-12 rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02] disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:scale-100"
                >
                  {runTestLoading ? "This may take a while, Running Test..." : "Run Test"}
                </Button>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Preview Modal */}
      {previewOpen && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center z-50 p-4">
          <div className="bg-black border border-slate-800 rounded-xl w-full max-w-4xl max-h-[80vh] flex flex-col shadow-2xl">
            {/* Header */}
            <div className="flex items-center justify-between p-6 border-b border-slate-600 bg-slate-800/50">
              <div className="flex-1 min-w-0">
                <div className="mb-3">
                  <label className="text-sm text-slate-400 font-medium">Algorithm:</label>
                  <h2 className="text-2xl font-bold text-white break-words">{previewAlgoName}</h2>
                </div>
                <div>
                  <label className="text-sm text-slate-400 font-medium">Description:</label>
                  <p className="text-slate-300 text-sm break-words overflow-hidden max-h-20 overflow-y-auto">
                    {previewDescription}
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={copyToClipboard}
                  className="p-2 text-white hover:bg-slate-700/50 rounded-lg transition-colors"
                  title="Copy code"
                >
                  <Copy className="h-5 w-5" />
                </button>
                <button
                  onClick={closePreview}
                  className="p-2 text-white hover:bg-slate-700/50 rounded-lg transition-colors"
                  title="Close"
                >
                  <X className="h-5 w-5" />
                </button>
              </div>
            </div>

            {/* Content */}
            <div className="flex-1 overflow-auto p-6 bg-black">
              {previewLoading ? (
                <div className="flex items-center justify-center py-12">
                  <div className="text-slate-400 text-center">
                    <p>Loading algorithm preview...</p>
                  </div>
                </div>
              ) : (
                <div className="bg-black rounded-lg overflow-hidden">
                  <SyntaxHighlighter
                    language="python"
                    style={vscDarkPlus}
                    customStyle={{
                      margin: 0,
                      background: "transparent",
                      fontSize: "14px",
                      lineHeight: "1.5",
                    }}
                  >
                    {previewCode}
                  </SyntaxHighlighter>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Results Modal */}
      {resultsOpen && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center z-50 p-4">
          <div className="bg-slate-900 border border-slate-700 rounded-xl w-full max-w-3xl max-h-[80vh] flex flex-col shadow-2xl">
            {/* Header */}
            <div className="flex items-center justify-between p-6 border-b border-slate-700">
              <h2 className="text-2xl font-bold text-white">Evaluation</h2>
              <button
                onClick={closeResultsModal}
                className="p-2 text-white hover:bg-slate-700/50 rounded-lg transition-colors"
                title="Close"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            {/* Content */}
            <div className="flex-1 overflow-auto p-6">
              {runTestLoading ? (
                <div className="flex flex-col items-center justify-center py-12 space-y-4">
                  <Loader2 className="h-8 w-8 text-slate-400 animate-spin" />
                  <p className="text-slate-400">Running tests...</p>
                </div>
              ) : (
                <div className="space-y-6">
                  {/* Top section - Algorithms and Metrics */}
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    {/* Algorithms Used */}
                    <div className="space-y-3">
                      <h3 className="text-lg font-semibold text-white border-b border-slate-700 pb-2">
                        Algorithms Used
                      </h3>
                      <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-4 max-h-40 overflow-y-auto">
                        {selectedAlgorithms.length === 0 ? (
                          <p className="text-slate-400 text-sm">No algorithms selected</p>
                        ) : (
                          <ul className="space-y-2">
                            {selectedAlgorithms.map((algo, index) => (
                              <li key={index} className="text-slate-300 text-sm">
                                {algo.name}
                              </li>
                            ))}
                          </ul>
                        )}
                      </div>
                    </div>

                    {/* Metrics */}
                    <div className="space-y-3">
                      <h3 className="text-lg font-semibold text-white border-b border-slate-700 pb-2">Metrics</h3>
                      <div className="bg-slate-800/50 border border-slate-700 rounded-lg p-4 max-h-40 overflow-y-auto">
                        <div className="space-y-2">
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Position Length:</span>
                            <span className="text-slate-300 text-sm">{positionLength}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Gain Percentage:</span>
                            <span className="text-slate-300 text-sm">{gainPercentage}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Loss Percentage:</span>
                            <span className="text-slate-300 text-sm">{lossPercentage}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Intercept Range:</span>
                            <span className="text-slate-300 text-sm">{interceptRange}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Clean Range:</span>
                            <span className="text-slate-300 text-sm">{cleanRange}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Intercept Needed:</span>
                            <span className="text-slate-300 text-sm">{interceptNeeded}</span>
                          </div>
                          <div className="flex justify-between">
                            <span className="text-slate-400 text-sm">Asset Type:</span>
                            <span className="text-slate-300 text-sm">
                              {selectedAsset === "fast-composite"
                                ? "Quick Composite"
                                : selectedAsset.charAt(0).toUpperCase() + selectedAsset.slice(1)}
                            </span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Results Section */}
                  {testResult && (
                    <div className="bg-slate-800/80 border border-slate-700 rounded-xl p-6 space-y-6">
                      <div className="flex items-center justify-between border-b border-slate-700 pb-3">
                        <div className="flex-1"></div>
                        <h3 className="text-xl font-semibold text-white">Results</h3>
                        <div className="flex-1 flex justify-end">
                          <Button
                            onClick={() => router.push("/all")}
                            className="bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-4 py-2 h-auto rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02] text-sm"
                          >
                            See All Evaluations
                          </Button>
                        </div>
                      </div>

                      {/* Score */}
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

                      {/* Asset Type */}
                      <div className="flex justify-center">
                        <div className="px-4 py-1 bg-slate-700/50 rounded-full">
                          <span className="text-slate-300 text-sm">{mapAssetCodeToName(testResult.assetCode)}</span>
                        </div>
                      </div>

                      {/* Stats Grid */}
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

                      {/* Actions Summary */}
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
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
