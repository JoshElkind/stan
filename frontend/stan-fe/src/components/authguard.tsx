"use client"
import { useSession, signIn, signOut } from "next-auth/react"
import type React from "react"
import { useRouter, usePathname } from "next/navigation"
import { useEffect, useState, useRef } from "react"
import { LogIn, X, AlertTriangle } from "lucide-react"
import { useSessionStorage, useIsClient } from "@/hooks/useStorage"

// Define public routes that don't require authentication
const PUBLIC_ROUTES = ["/", "/about", "/faq"]

export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const { data: session, status } = useSession()
  const router = useRouter()
  const pathname = usePathname()
  const isClient = useIsClient()
  
  // Use the SSR-safe sessionStorage hook
  const [redirectAfterLogin, setRedirectAfterLogin] = useSessionStorage("redirectAfterLogin", "")
  
  const [showAccessDenied, setShowAccessDenied] = useState(false)
  const [showTokenExpired, setShowTokenExpired] = useState(false)
  const [blockedPath, setBlockedPath] = useState<string | null>(null)
  const [currentPath, setCurrentPath] = useState<string>("/")
  const preventNavigation = useRef(false)

  // Check if current route is public
  const isPublicRoute = PUBLIC_ROUTES.includes(pathname)

  useEffect(() => {
    if (session?.error === "RefreshAccessTokenError") {
      setShowTokenExpired(true)
    }
  }, [session])

  useEffect(() => {
    if (session || isPublicRoute) {
      setCurrentPath(pathname)
      preventNavigation.current = false
    }
  }, [session, pathname, isPublicRoute])

  useEffect(() => {
    if (status === "loading" || !isClient) return

    // If route is public, always allow access
    if (isPublicRoute) {
      preventNavigation.current = false
      return
    }

    // For protected routes, check authentication
    if (session) {
      preventNavigation.current = false
      return
    }

    if (!session && !preventNavigation.current) {
      preventNavigation.current = true
      setBlockedPath(pathname)
      setShowAccessDenied(true)
      router.replace(currentPath)
    }
  }, [session, status, pathname, router, currentPath, isClient, isPublicRoute])

  const handleSignIn = async () => {
    // Now using the SSR-safe hook - no need for manual client-side checks
    if (blockedPath) {
      setRedirectAfterLogin(blockedPath)
    }
    
    setShowAccessDenied(false)
    setShowTokenExpired(false)
    setBlockedPath(null)
    preventNavigation.current = false

    await signIn("google", {
      callbackUrl: blockedPath || "/",
    })
  }

  const handleCloseModal = () => {
    setShowAccessDenied(false)
    setBlockedPath(null)
    preventNavigation.current = false
  }

  const handleTokenExpiredClose = () => {
    setShowTokenExpired(false)
  }

  const handleForceSignOut = async () => {
    setShowTokenExpired(false)
    // Clear the redirect path when signing out
    setRedirectAfterLogin("")
    await signOut({ callbackUrl: "/" })
  }

  // Show loading state during SSR or initial client load
  if (status === "loading" || !isClient) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-white text-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-white mx-auto mb-4"></div>
          <p>Loading...</p>
        </div>
      </div>
    )
  }

  return (
    <>
      <div className={showAccessDenied || showTokenExpired ? "blur-[2px]" : ""}>{children}</div>

      {showAccessDenied && (
        <div className="fixed inset-0 bg-black/30 backdrop-blur-[1px] flex items-center justify-center z-50 p-4">
          <div className="bg-slate-800 border border-slate-600 rounded-lg p-4 max-w-xs w-full shadow-xl relative">
            <button
              onClick={handleCloseModal}
              className="absolute top-2 right-2 p-1 text-slate-400 hover:text-white transition-colors"
              aria-label="Close modal"
            >
              <X className="h-3 w-3" />
            </button>

            <div className="text-center pr-6">
              <p className="text-white text-sm mb-4">You must sign in.</p>
              <div className="flex justify-center">
                <button
                  onClick={handleSignIn}
                  className="bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-4 py-2 text-sm rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02] relative overflow-hidden group inline-flex items-center"
                >
                  <div className="absolute inset-0 bg-gradient-to-r from-white/0 via-white/20 to-white/0 translate-x-[-100%] group-hover:translate-x-[100%] transition-transform duration-700"></div>
                  <LogIn className="mr-2 h-3 w-3 relative z-10" />
                  <span className="relative z-10">Sign In</span>
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {showTokenExpired && (
        <div className="fixed inset-0 bg-black/30 backdrop-blur-[1px] flex items-center justify-center z-50 p-4">
          <div className="bg-slate-800 border border-red-600 rounded-lg p-4 max-w-sm w-full shadow-xl relative">
            <button
              onClick={handleTokenExpiredClose}
              className="absolute top-2 right-2 p-1 text-slate-400 hover:text-white transition-colors"
              aria-label="Close modal"
            >
              <X className="h-3 w-3" />
            </button>

            <div className="text-center pr-6">
              <AlertTriangle className="h-8 w-8 text-red-400 mx-auto mb-3" />
              <h3 className="text-white text-sm font-medium mb-2">Session Expired</h3>
              <p className="text-slate-300 text-xs mb-4">Your session has expired. Please sign in again.</p>

              <div className="flex gap-2 justify-center">
                <button
                  onClick={handleSignIn}
                  className="bg-gradient-to-r from-teal-500 to-cyan-500 hover:from-teal-400 hover:to-cyan-400 text-white font-medium px-3 py-1.5 text-xs rounded-lg shadow-lg shadow-teal-500/25 border border-teal-400/30 transition-all duration-200 hover:shadow-teal-400/40 hover:scale-[1.02] relative overflow-hidden group inline-flex items-center"
                >
                  <LogIn className="mr-1 h-3 w-3" />
                  Sign In
                </button>
                <button
                  onClick={handleForceSignOut}
                  className="bg-slate-700 hover:bg-slate-600 text-white font-medium px-3 py-1.5 text-xs rounded-lg border border-slate-600 transition-all duration-200"
                >
                  Sign Out
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  )
}