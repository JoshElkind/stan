"use client"

import type React from "react"
import { useSession, signOut, SessionProvider } from "next-auth/react"
import { useRouter } from "next/navigation"
import { useEffect, useRef } from "react"
import AuthGuard from "@/components/authguard"
import TokenRefreshProvider from "@/components/token-refresh-provider"
import ErrorBoundary from "@/components/error-boundary"
import Header from "@/components/header"

function LayoutContent({ children }: { children: React.ReactNode }) {
  const { data: session, status } = useSession()
  const router = useRouter()
  const hasRedirected = useRef(false)

  useEffect(() => {
    if (session && status === "authenticated" && !hasRedirected.current) {
      const redirectPath = sessionStorage.getItem("redirectAfterLogin")
      
      if (redirectPath && redirectPath !== "/" && redirectPath !== window.location.pathname) {
        // Clean the redirect path to remove any encoded quotes or extra characters
        const cleanPath = redirectPath
          .replace(/['"]/g, '')  // Remove any literal quotes
          .replace(/%22/g, '')   // Remove URL-encoded quotes
          .replace(/^\/+/, '/')  // Remove duplicate leading slashes
        
        // Validate that the cleaned path is a valid route
        if (cleanPath && cleanPath.startsWith('/') && cleanPath !== window.location.pathname) {
          hasRedirected.current = true
          sessionStorage.removeItem("redirectAfterLogin")
          
          console.log('Redirecting to cleaned path:', cleanPath) // Debug log
          router.push(cleanPath)
        } else {
          // If path is invalid, clear it and stay on current page
          sessionStorage.removeItem("redirectAfterLogin")
        }
      }
    }

    if (status === "unauthenticated") {
      hasRedirected.current = false
    }
  }, [session, status, router])

  
  return <AuthGuard>{children}</AuthGuard>
}

export default function ClientLayout({ children }: { children: React.ReactNode }) {
  return (
    <SessionProvider>
      <TokenRefreshProvider>
        <Header />
        <ErrorBoundary>
          <LayoutContent>{children}</LayoutContent>
        </ErrorBoundary>
      </TokenRefreshProvider>
    </SessionProvider>
  )
}