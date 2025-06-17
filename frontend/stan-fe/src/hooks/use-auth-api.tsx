"use client"
import { useSession, signOut } from "next-auth/react"
import { useCallback, useState, useRef } from "react"
import { useRouter } from "next/navigation"

export function useAuthApi() {
  const { data: session, update } = useSession()
  const router = useRouter()
  const [isRefreshing, setIsRefreshing] = useState(false)
  const refreshDebounceRef = useRef<NodeJS.Timeout | null>(null)

  const makeAuthenticatedRequest = useCallback(
    async (url: string, options: RequestInit = {}) => {
      if (!session?.accessToken) {
        throw new Error("No access token available")
      }

      if (session.error === "RefreshAccessTokenError") {
        await signOut({ callbackUrl: "/" })
        throw new Error("Session expired")
      }

      try {
        const response = await fetch(url, {
          ...options,
          headers: {
            ...options.headers,
            Authorization: `Bearer ${session.accessToken}`,
            "Content-Type": "application/json",
          },
        })

        if (response.status === 401 || response.status === 403) {
          if (isRefreshing) {
            throw new Error(`HTTP error! status: ${response.status}`)
          }

          setIsRefreshing(true)

          if (refreshDebounceRef.current) {
            clearTimeout(refreshDebounceRef.current)
          }

          refreshDebounceRef.current = setTimeout(async () => {
            try {
              const updatedSession = await update()
              setIsRefreshing(false)

              if (updatedSession?.error === "RefreshAccessTokenError") {
                router.replace("/")
                throw new Error("Session expired")
              }
            } catch (error) {
              setIsRefreshing(false)
              throw new Error(`Failed to refresh token: ${error}`)
            }
          }, 300)

          await new Promise((resolve) => setTimeout(resolve, 350))

          if (session?.accessToken) {
            return fetch(url, {
              ...options,
              headers: {
                ...options.headers,
                Authorization: `Bearer ${session.accessToken}`,
                "Content-Type": "application/json",
              },
            })
          }
        }

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`)
        }

        return response
      } catch (error) {
        const errorEvent = new ErrorEvent("error", {
          error: error,
          message: error instanceof Error ? error.message : String(error),
          lineno: 0,
          colno: 0,
          filename: "use-auth-api.tsx",
        })
        window.dispatchEvent(errorEvent)
        throw error
      }
    },
    [session, update, isRefreshing, router],
  )

  return { makeAuthenticatedRequest, session, isRefreshing }
}
