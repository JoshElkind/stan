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
        hasRedirected.current = true
        sessionStorage.removeItem("redirectAfterLogin")
        router.push(redirectPath)
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