"use client"

import type React from "react"

import { useTokenRefresh } from "@/lib/auth-refresh"
import { useEffect, useState } from "react"
import { usePathname, useRouter } from "next/navigation"
import { useSession } from "next-auth/react"

export default function TokenRefreshProvider({ children }: { children: React.ReactNode }) {
  const { isRefreshing } = useTokenRefresh()
  const { data: session } = useSession()
  const [isLoading, setIsLoading] = useState(false)
  const router = useRouter()
  const pathname = usePathname()

  useEffect(() => {
    if (session?.error === "RefreshAccessTokenError") {
      if (pathname !== "/") {
        setIsLoading(true)
        router.replace("/")
      }
    }
  }, [session, router, pathname])

  if (isLoading || isRefreshing) {
    return (
      <div className="fixed inset-0 bg-black/30 backdrop-blur-[1px] flex items-center justify-center z-50">
        <div className="text-white text-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-white mx-auto mb-4"></div>
          <p>Refreshing session...</p>
        </div>
      </div>
    )
  }

  return <>{children}</>
}
