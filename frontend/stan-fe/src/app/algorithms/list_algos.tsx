"use client"

import { useState, useEffect } from "react"
import { Button } from "@/components/ui/button"
import { useSession } from "next-auth/react"
import { Eye, Activity, Code2, Copy, X, Minus, FileText, Calendar } from "lucide-react"
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter"
import { vscDarkPlus } from "react-syntax-highlighter/dist/esm/styles/prism"

interface Algorithm {
  algoname: string
  description: string
  date_added: string
}

interface ApiResponse {
  algorithms: Algorithm[]
}

export type SortMethod = "alphabetical" | "date_added"

interface ListAlgosProps {
  sortMethod?: SortMethod
}

export default function ListAlgos({ sortMethod = "date_added" }: ListAlgosProps) {
  const [algorithms, setAlgorithms] = useState<Algorithm[]>([])
  const [sortedAlgorithms, setSortedAlgorithms] = useState<Algorithm[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [deletingAlgo, setDeletingAlgo] = useState<string | null>(null)
  const [showDeleteModal, setShowDeleteModal] = useState(false)
  const [algorithmToDelete, setAlgorithmToDelete] = useState<string | null>(null)
  const { data: session, status } = useSession()

  const [previewOpen, setPreviewOpen] = useState(false)
  const [previewCode, setPreviewCode] = useState("")
  const [previewAlgoName, setPreviewAlgoName] = useState("")
  const [previewLoading, setPreviewLoading] = useState(false)

  useEffect(() => {
    if (status !== "authenticated") return

    const fetchAlgorithms = async () => {
      if (!session?.accessToken) {
        setError("No authentication token available")
        setLoading(false)
        return
      }

      try {
        const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/scripts/user/`, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${session.accessToken}`,
            "Content-Type": "application/json",
          },
        })

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`)
        }

        const data: ApiResponse = await response.json()
        setAlgorithms(data.algorithms || [])
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to fetch algorithms")
      } finally {
        setLoading(false)
      }
    }

    fetchAlgorithms()
  }, [session, status])

  useEffect(() => {
    if (algorithms.length === 0) {
      setSortedAlgorithms([])
      return
    }

    const sorted = [...algorithms]

    if (sortMethod === "alphabetical") {
      sorted.sort((a, b) => a.algoname.localeCompare(b.algoname))
    } else if (sortMethod === "date_added") {
      sorted.sort((a, b) => new Date(b.date_added).getTime() - new Date(a.date_added).getTime())
    }

    setSortedAlgorithms(sorted)
  }, [algorithms, sortMethod])

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString("en-US", {
      year: "numeric",
      month: "short",
      day: "numeric",
    })
  }

  const truncateDescription = (text: string, maxChars = 15) => {
    if (!text) return ""
    if (text.length <= maxChars) return text
    return text.slice(0, maxChars) + "..."
  }

  const handlePreview = async (algoname: string) => {
    if (!session?.accessToken) {
      setError("No authentication token available")
      return
    }

    setPreviewLoading(true)
    setPreviewOpen(true)
    setPreviewAlgoName(algoname)
    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/scripts/preview/`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${session.accessToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          algo_type: "user",
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

  const handleDeleteClick = (algoname: string) => {
    setAlgorithmToDelete(algoname)
    setShowDeleteModal(true)
  }

  const handleDeleteConfirm = async () => {
    if (!algorithmToDelete || !session?.accessToken) {
      return
    }

    setDeletingAlgo(algorithmToDelete)
    
    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/scripts/user/delete/`, {
        method: "DELETE",
        headers: {
          Authorization: `Bearer ${session.accessToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          algoname: algorithmToDelete,
        }),
      })

      if (!response.ok) {
        const data = await response.json()
        throw new Error(data.error || `HTTP error! status: ${response.status}`)
      }

      setAlgorithms((prev) => prev.filter((algo) => algo.algoname !== algorithmToDelete))
      setShowDeleteModal(false)
      setAlgorithmToDelete(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete algorithm")
    } finally {
      setDeletingAlgo(null)
    }
  }

  const handleDeleteCancel = () => {
    setShowDeleteModal(false)
    setAlgorithmToDelete(null)
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
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="text-slate-400 text-center">
          <p>Loading algorithms...</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="text-center py-12">
        <div className="text-red-400 bg-red-500/10 border border-red-500/20 rounded-lg p-6 max-w-md mx-auto">
          <p className="font-medium">Error loading algorithms</p>
          <p className="text-sm text-red-300 mt-1">{error}</p>
        </div>
      </div>
    )
  }

  return (
    <div className="w-full">
      {sortedAlgorithms.length === 0 ? (
        <div className="bg-slate-800/50 rounded-xl p-12 border border-slate-700/50 shadow-lg text-center">
          <div className="relative w-16 h-16 mx-auto mb-4">
            <div className="w-16 h-16 bg-gradient-to-br from-slate-700/50 to-slate-800/50 rounded-full flex items-center justify-center">
              <Activity className="h-8 w-8 text-slate-500" />
            </div>
            <div className="absolute -top-1 -right-1 w-5 h-5 bg-gradient-to-r from-teal-400 to-cyan-400 rounded-full flex items-center justify-center">
              <div className="w-2 h-2 bg-white rounded-full"></div>
            </div>
          </div>
          <h3 className="text-white font-medium text-lg mb-2">No algorithms yet</h3>
          <p className="text-slate-400 text-sm">Create your first algorithm to get started with trading analysis.</p>
        </div>
      ) : (
        <div>
          <div className="grid grid-cols-12 gap-6 pb-4 mb-6 border-b border-slate-700/50">
            <div className="col-span-3 text-slate-400 font-medium text-sm uppercase tracking-wide flex items-center">
              <Code2 className="h-4 w-4 text-slate-500 mr-2" />
              Algorithm Name
            </div>
            <div className="col-span-4 text-slate-400 font-medium text-sm uppercase tracking-wide flex items-center">
              <FileText className="h-4 w-4 text-slate-500 mr-2" />
              Description
            </div>
            <div className="col-span-3 text-slate-400 font-medium text-sm uppercase tracking-wide flex items-center">
              <Calendar className="h-4 w-4 text-slate-500 mr-2" />
              Date Added
            </div>
            <div className="col-span-2 text-slate-400 font-medium text-sm uppercase tracking-wide flex items-center justify-center">
              <Eye className="h-4 w-4 text-slate-500 mr-2" />
              Preview
            </div>
          </div>

          <div className="h-[calc(100vh-280px)] overflow-y-auto pr-2 space-y-4 custom-scrollbar">
            {sortedAlgorithms.map((algorithm, index) => (
              <div
                key={index}
                className="bg-slate-800/50 rounded-xl p-6 border border-slate-700/50 shadow-lg hover:bg-slate-800/70 transition-all duration-200 group relative"
              >
                <button
                  onClick={() => handleDeleteClick(algorithm.algoname)}
                  className="absolute top-2 right-2 w-8 h-8 bg-red-500/20 hover:bg-red-500/30 text-red-400 hover:text-red-300 border border-red-500/30 hover:border-red-500/50 rounded-lg flex items-center justify-center transition-all duration-200 opacity-0 group-hover:opacity-100"
                  title="Delete algorithm"
                >
                  <Minus className="h-4 w-4" />
                </button>

                <div className="grid grid-cols-12 gap-6">
                  <div className="col-span-3 flex items-center">
                    <div className="relative w-10 h-10 bg-gradient-to-br from-teal-500/20 to-cyan-500/20 rounded-lg flex items-center justify-center mr-3 border border-teal-500/30">
                      <Code2 className="h-5 w-5 text-teal-400" />
                      <div className="absolute -top-1 -right-1 w-3 h-3 bg-gradient-to-r from-emerald-400 to-teal-400 rounded-full"></div>
                    </div>
                    <div className="flex-1 min-w-0">
                      <h3 className="text-lg font-semibold text-white mb-1 truncate">{algorithm.algoname}</h3>
                      <p className="text-slate-400 text-xs">Trading Algorithm</p>
                    </div>
                  </div>

                  <div className="col-span-4 flex items-center">
                    <div className="flex items-center justify-between w-full">
                      <p className="text-slate-300 text-sm leading-relaxed flex-1">
                        {truncateDescription(algorithm.description)}
                      </p>
                    </div>
                  </div>

                  <div className="col-span-3 flex items-center">
                    <div className="flex flex-col pl-6">
                      <p className="text-slate-500 text-sm">{formatDate(algorithm.date_added)}</p>
                    </div>
                  </div>

                  <div className="col-span-2 flex items-center justify-center">
                    <Button
                      onClick={() => handlePreview(algorithm.algoname)}
                      className="bg-gradient-to-r from-emerald-500/20 to-teal-500/20 hover:from-emerald-500/30 hover:to-teal-500/30 text-emerald-400 hover:text-emerald-300 border border-emerald-500/30 hover:border-emerald-500/50 font-medium px-4 py-2 h-8 rounded-lg transition-all duration-200 hover:scale-105"
                    >
                      <Eye className="mr-1.5 h-3.5 w-3.5" />
                      Preview
                    </Button>
                  </div>
                </div>
              </div>
            ))}
          </div>

          <style jsx global>{`
            .custom-scrollbar::-webkit-scrollbar {
              width: 8px;
            }
            .custom-scrollbar::-webkit-scrollbar-track {
              background: rgba(30, 41, 59, 0.2);
              border-radius: 8px;
            }
            .custom-scrollbar::-webkit-scrollbar-thumb {
              background: rgba(71, 85, 105, 0.5);
              border-radius: 8px;
            }
            .custom-scrollbar::-webkit-scrollbar-thumb:hover {
              background: rgba(71, 85, 105, 0.7);
            }
          `}</style>
        </div>
      )}

      {showDeleteModal && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center z-50 p-4">
          <div className="bg-slate-800 border border-slate-700 rounded-xl w-full max-w-sm shadow-2xl">
            <div className="flex items-center justify-between p-6 border-b border-slate-600">
              <h2 className="text-lg font-bold text-white">Delete Algorithm</h2>
              <button onClick={handleDeleteCancel} className="p-1 text-slate-400 hover:text-white transition-colors">
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="p-6">
              <p className="text-slate-300 text-sm mb-6">
                Are you sure you want to delete "{algorithmToDelete}"? This action cannot be undone.
              </p>

              <div className="flex justify-end gap-3">
                <Button
                  onClick={handleDeleteCancel}
                  variant="outline"
                  className="bg-white hover:bg-gray-100 border-gray-300 text-black hover:text-black"
                  disabled={deletingAlgo === algorithmToDelete}
                >
                  Cancel
                </Button>
                <Button
                  onClick={handleDeleteConfirm}
                  className="bg-red-600 hover:bg-red-700 text-red-100 hover:text-red-50 border-red-600 hover:border-red-700"
                  disabled={deletingAlgo === algorithmToDelete}
                >
                  {deletingAlgo === algorithmToDelete ? (
                    <>
                      <div className="w-3 h-3 border border-red-200 border-t-transparent rounded-full animate-spin mr-2"></div>
                      Deleting...
                    </>
                  ) : (
                    "Delete"
                  )}
                </Button>
              </div>
            </div>
          </div>
        </div>
      )}

      {previewOpen && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center z-50 p-4">
          <div className="bg-black border border-slate-800 rounded-xl w-full max-w-4xl max-h-[80vh] flex flex-col shadow-2xl">
            <div className="flex items-center justify-between p-6 border-b border-slate-600 bg-slate-800/50">
              <div className="flex-1 min-w-0">
                <div className="mb-3">
                  <label className="text-sm text-slate-400 font-medium">Algorithm:</label>
                  <h2 className="text-2xl font-bold text-white break-words">{previewAlgoName}</h2>
                </div>
                <div>
                  <label className="text-sm text-slate-400 font-medium">Description:</label>
                  <p className="text-slate-300 text-sm break-words overflow-hidden max-h-20 overflow-y-auto">
                    {sortedAlgorithms.find((algo) => algo.algoname === previewAlgoName)?.description || ""}
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

            <div className="flex-1 overflow-auto p-6 bg-black">
              {previewLoading ? (
                <div className="flex items-center justify-center py-12">
                  <div className="text-slate-400 text-center">
                    <p>Loading code preview...</p>
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
    </div>
  )
}
