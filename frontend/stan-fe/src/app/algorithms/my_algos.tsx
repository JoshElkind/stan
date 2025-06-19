"use client"

import type React from "react"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { useSession } from "next-auth/react"
import { Button } from "@/components/ui/button"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"
import { ChevronDown, Plus, X, Upload, FileCode, CheckCircle, AlertCircle, Activity } from "lucide-react"
import ListAlgos, { type SortMethod } from "./list_algos"
import { signOut } from 'next-auth/react'

const MyAlgos = () => {
  const [sortMethod, setSortMethod] = useState<SortMethod>("date_added")
  const [showUploadModal, setShowUploadModal] = useState(false)
  const [uploadFile, setUploadFile] = useState<File | null>(null)
  const [algoName, setAlgoName] = useState("")
  const [description, setDescription] = useState("")
  const [isUploading, setIsUploading] = useState(false)
  const [notification, setNotification] = useState<{
    type: "success" | "error"
    message: string
  } | null>(null)
  const [modalError, setModalError] = useState<string | null>(null)
  const [refreshKey, setRefreshKey] = useState(0)

  const router = useRouter()
  const { data: session } = useSession()

  // Auto-hide notification after 3 seconds
  useEffect(() => {
    if (notification) {
      const timer = setTimeout(() => {
        setNotification(null)
      }, 3000)
      return () => clearTimeout(timer)
    }
  }, [notification])

  const handleCreateAlgorithm = () => {
    router.push("/make")
  }

  const handleUploadAlgorithm = () => {
    setShowUploadModal(true)
    setModalError(null)
  }

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (file) {
      setUploadFile(file)
    }
  }

  const handleDescriptionChange = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    const value = event.target.value
    if (value.length <= 500) {
      setDescription(value)
    }
  }

  const getErrorMessage = (errorMessage: string) => {
    if (errorMessage === "Algorithm with this name already exists.") {
      return "Algorithm with this name already exists."
    }

    if (errorMessage.startsWith("Invalid Python file:")) {
      return errorMessage
    }

    if (errorMessage === "Return must be list or numpy array.") {
      return "Return type for your script must be a list or numpy array."
    }

    if (errorMessage.startsWith("No function")) {
      return "Must name the algorithm the same as function inside the script."
    }

    return errorMessage
  }

  const handleUploadSubmit = async () => {
    if (!uploadFile || !algoName.trim()) {
      setModalError("Please provide both algorithm name and file")
      return
    }

    if (!session?.accessToken) {
      setModalError("No authentication token available")
      return
    }

    setIsUploading(true)
    setModalError(null)

    try {
      const formData = new FormData()
      formData.append("file", uploadFile)
      formData.append("algoname", algoName.trim())
      formData.append("algodescription", description.trim())

      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/scripts/user/upload/`, {
        method: "POST",
        credentials: 'include',
        headers: {
          Authorization: `Bearer ${session.accessToken}`,
        },
        body: formData,
      })

      const data = await response.json()

      if (!response.ok) {
    
        const data = await response.json()
        if (response.status === 403 && data?.detail?.includes("Token expired")) {
          console.warn("You've been idle for too long. Please sign in again.")
          setNotification({
            type: "error",
            message: "No authentication token available",
          })
          signOut()
          return
        }

        throw new Error(`HTTP error! status: ${response.status}`)
      }

      // Success case - show notification and close modal
      setNotification({
        type: "success",
        message: "Algorithm Script Uploaded Successfully",
      })
      // Force refresh of the algorithm list
      setRefreshKey((prev) => prev + 1)
      closeUploadModal()
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Upload failed"
      // For network/other errors, also show in modal
      setModalError(errorMessage)
    } finally {
      setIsUploading(false)
    }
  }

  const closeUploadModal = () => {
    setShowUploadModal(false)
    setUploadFile(null)
    setAlgoName("")
    setDescription("")
    setModalError(null)
  }

  return (
    <div className="w-full min-h-[calc(100vh-64px)]" style={{ backgroundColor: "#1c1e21" }}>
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

      <div className="p-8 h-full">
        <div className="flex items-center justify-between mb-8">
          <div className="relative">
            <div className="relative">
              <div className="flex items-center gap-4 mb-3">
                <div className="w-12 h-12 bg-gradient-to-br from-teal-500 to-cyan-500 rounded-xl flex items-center justify-center shadow-lg shadow-teal-500/25">
                  <Activity className="h-6 w-6 text-white" />
                </div>

                <h1 className="text-5xl font-bold text-white tracking-tight">My Algorithms</h1>
              </div>

              <div className="flex items-center">
                <p className="text-lg text-white font-medium">Manage and organize your trading algorithms</p>
              </div>

              <div className="mt-4 h-1 w-32 bg-gradient-to-r from-teal-500 to-cyan-500 rounded-full"></div>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button
                  variant="outline"
                  className="bg-slate-800/50 border-slate-700 text-slate-300 hover:bg-slate-700/70 hover:text-white hover:border-slate-600 transition-all duration-200"
                >
                  Sort By: {sortMethod === "alphabetical" ? "Alphabetical" : "Date Added"}
                  <ChevronDown className="ml-2 h-4 w-4" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent className="bg-slate-800 border-slate-700 shadow-xl">
                <DropdownMenuItem
                  className={`text-slate-300 hover:bg-slate-700 focus:bg-slate-700 hover:text-white ${sortMethod === "alphabetical" ? "bg-slate-700 text-white" : ""}`}
                  onClick={() => setSortMethod("alphabetical")}
                >
                  Alphabetical
                </DropdownMenuItem>
                <DropdownMenuItem
                  className={`text-slate-300 hover:bg-slate-700 focus:bg-slate-700 hover:text-white ${sortMethod === "date_added" ? "bg-slate-700 text-white" : ""}`}
                  onClick={() => setSortMethod("date_added")}
                >
                  Date Added
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>

            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button className="bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-6 py-2.5 h-auto rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02] relative overflow-hidden group">
                  <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700"></div>
                  <Plus className="mr-2 h-4 w-4 relative z-10" />
                  <span className="relative z-10">Add Algorithm</span>
                  <ChevronDown className="ml-2 h-4 w-4 relative z-10" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent className="bg-slate-800 border-slate-700 shadow-xl">
                <DropdownMenuItem
                  className="text-slate-300 hover:bg-slate-700 focus:bg-slate-700 hover:text-white cursor-pointer"
                  onClick={handleCreateAlgorithm}
                >
                  <FileCode className="mr-2 h-4 w-4" />
                  Create Algorithm
                </DropdownMenuItem>
                <DropdownMenuItem
                  className="text-slate-300 hover:bg-slate-700 focus:bg-slate-700 hover:text-white cursor-pointer"
                  onClick={handleUploadAlgorithm}
                >
                  <Upload className="mr-2 h-4 w-4" />
                  Upload Algorithm
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>

        <div className="relative mb-6">
          <div className="h-px bg-gradient-to-r from-transparent via-slate-600 to-transparent"></div>
        </div>

        <ListAlgos sortMethod={sortMethod} key={refreshKey} />
      </div>

      {showUploadModal && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center z-50 p-4">
          <div className="bg-slate-800 border border-slate-700 rounded-xl w-full max-w-md shadow-2xl">
            <div className="flex items-center justify-between p-6 border-b border-slate-600">
              <h2 className="text-xl font-bold text-white">Upload Script</h2>
              <button onClick={closeUploadModal} className="p-1 text-slate-400 hover:text-white transition-colors">
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="p-6 space-y-4">
              {modalError && (
                <div className="bg-red-900/50 border border-red-700 rounded-lg p-3">
                  <div className="flex items-center gap-2">
                    <AlertCircle className="h-4 w-4 text-red-400" />
                    <span className="text-red-200 text-sm">{modalError}</span>
                  </div>
                </div>
              )}

              <div>
                <label htmlFor="algoname" className="block text-sm font-medium text-white mb-2">
                  Algorithm Name
                </label>
                <input
                  type="text"
                  id="algoname"
                  value={algoName}
                  onChange={(e) => setAlgoName(e.target.value)}
                  className="w-full px-3 py-2 bg-slate-700 border border-slate-600 rounded-lg text-white placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-teal-500 focus:border-transparent"
                  placeholder="Enter algorithm name"
                  disabled={isUploading}
                />
              </div>

              <div>
                <label htmlFor="description" className="block text-sm font-medium text-white mb-2">
                  Description
                </label>
                <textarea
                  id="description"
                  value={description}
                  onChange={handleDescriptionChange}
                  rows={3}
                  className="w-full px-3 py-2 bg-slate-700 border border-slate-600 rounded-lg text-white placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-teal-500 focus:border-transparent resize-none"
                  placeholder="Enter algorithm description..."
                  disabled={isUploading}
                />
                <div className="flex justify-between items-center mt-1">
                  <span className="text-xs text-slate-400">Optional</span>
                  <span className="text-xs text-slate-400">{description.length}/500</span>
                </div>
              </div>

              <div>
                <label htmlFor="file-upload" className="block text-sm font-medium text-white mb-2">
                  Script File
                </label>
                <div className="relative">
                  <input
                    type="file"
                    id="file-upload"
                    onChange={handleFileChange}
                    accept=".py,.txt"
                    className="hidden"
                    disabled={isUploading}
                  />
                  <label
                    htmlFor="file-upload"
                    className={`w-full flex items-center justify-center px-4 py-3 bg-slate-700 border-2 border-dashed border-slate-600 rounded-lg cursor-pointer hover:bg-slate-600 hover:border-slate-500 transition-colors ${isUploading ? "opacity-50 cursor-not-allowed" : ""}`}
                  >
                    <Upload className="h-5 w-5 text-slate-400 mr-2" />
                    <span className="text-slate-300">{uploadFile ? uploadFile.name : "Choose file to upload"}</span>
                  </label>
                </div>
              </div>

              <p className="text-xs text-teal-400 mt-2">
                Algorithm name should match the name of the function in your script.
              </p>

              <div className="flex justify-end gap-3 pt-4">
                <Button
                  onClick={closeUploadModal}
                  variant="outline"
                  className="bg-transparent border-slate-600 text-slate-300 hover:bg-slate-700 hover:text-white"
                  disabled={isUploading}
                >
                  Cancel
                </Button>
                <Button
                  onClick={handleUploadSubmit}
                  disabled={!uploadFile || !algoName.trim() || isUploading}
                  className="bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {isUploading ? "Uploading..." : "Upload"}
                </Button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default MyAlgos
