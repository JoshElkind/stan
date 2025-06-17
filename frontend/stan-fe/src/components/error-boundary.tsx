"use client"

import type React from "react"

import { AlertTriangle, RefreshCw } from "lucide-react"
import { useEffect, useState } from "react"
import { useSession, signIn } from "next-auth/react"

interface ErrorBoundaryProps {
  children: React.ReactNode
}

export default function ErrorBoundary({ children }: ErrorBoundaryProps) {
  const [hasError, setHasError] = useState(false)
  const [errorMessage, setErrorMessage] = useState("")
  const { data: session, update } = useSession()

  useEffect(() => {
    const handleApiError = (event: ErrorEvent) => {
      if (
        event.error?.message?.includes("403") ||
        event.error?.message?.includes("401") ||
        event.error?.message?.includes("Unauthorized")
      ) {
        setHasError(true)
        setErrorMessage("Session expired. Please refresh or sign in again.")
      }
    }

    window.addEventListener("error", handleApiError)
    return () => window.removeEventListener("error", handleApiError)
  }, [])

  useEffect(() => {
    if (session && !session.error) {
      setHasError(false)
      setErrorMessage("")
    }
  }, [session])

  const handleRefresh = async () => {
    try {
      setHasError(false)
      await update() // try to refresh the token
      window.location.reload() // reload the page to retry fetching data
    } catch (error) {
      setHasError(true)
      setErrorMessage("Failed to refresh session. Please sign in again.")
    }
  }

  const handleSignIn = () => {
    signIn("google")
  }

  if (hasError) {
    return (
      <div className="min-h-[300px] flex items-center justify-center">
        <div className="bg-slate-800 border border-red-600 rounded-lg p-6 max-w-sm w-full shadow-xl">
          <AlertTriangle className="h-8 w-8 text-red-400 mx-auto mb-3" />
          <h3 className="text-white text-lg font-medium mb-2 text-center">Session Error</h3>
          <p className="text-slate-300 text-sm mb-4 text-center">{errorMessage}</p>

          <div className="flex gap-3 justify-center">
            <button
              onClick={handleRefresh}
              className="bg-slate-700 hover:bg-slate-600 text-white font-medium px-4 py-2 text-sm rounded-lg border border-slate-600 transition-all duration-200 flex items-center"
            >
              <RefreshCw className="mr-2 h-4 w-4" />
              Refresh
            </button>
            <button
              onClick={handleSignIn}
              className="bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-4 py-2 text-sm rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200"
            >
              Sign In
            </button>
          </div>
        </div>
      </div>
    )
  }

  return <>{children}</>
}
