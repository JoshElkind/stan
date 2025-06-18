"use client"
import { useSession, signOut } from "next-auth/react"
import { useCallback, useState, useRef } from "react"
import { useRouter } from "next/navigation"

interface AuthApiError extends Error {
  status?: number
  code?: string
}

export function useAuthApi() {
  const { data: session, update } = useSession()
  const router = useRouter()
  const [isRefreshing, setIsRefreshing] = useState(false)
  const refreshPromiseRef = useRef<Promise<any> | null>(null)

  const refreshToken = useCallback(async () => {
    // Prevent multiple concurrent refresh attempts
    if (refreshPromiseRef.current) {
      return refreshPromiseRef.current
    }

    setIsRefreshing(true)
    
    refreshPromiseRef.current = update()
      .then((updatedSession) => {
        if (updatedSession?.error === "RefreshAccessTokenError") {
          throw new Error("Session expired")
        }
        return updatedSession
      })
      .finally(() => {
        setIsRefreshing(false)
        refreshPromiseRef.current = null
      })

    return refreshPromiseRef.current
  }, [update])

  const makeAuthenticatedRequest = useCallback(
    async (url: string, options: RequestInit = {}) => {
      if (!session?.accessToken) {
        const error: AuthApiError = new Error("No access token available")
        error.code = "NO_ACCESS_TOKEN"
        throw error
      }

      if (session.error === "RefreshAccessTokenError") {
        await signOut({ callbackUrl: "/" })
        const error: AuthApiError = new Error("Session expired")
        error.code = "SESSION_EXPIRED"
        throw error
      }

      const makeRequest = async (token: string) => {
        return fetch(url, {
          ...options,
          headers: {
            ...options.headers,
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
        })
      }

      try {
        const response = await makeRequest(session.accessToken)

        // Handle authentication errors
        if (response.status === 401 || response.status === 403) {
          try {
            const refreshedSession = await refreshToken()
            
            if (refreshedSession?.accessToken) {
              // Retry with new token
              const retryResponse = await makeRequest(refreshedSession.accessToken)
              
              if (!retryResponse.ok) {
                const error: AuthApiError = new Error(`HTTP error! status: ${retryResponse.status}`)
                error.status = retryResponse.status
                throw error
              }
              
              return retryResponse
            } else {
              router.replace("/")
              const error: AuthApiError = new Error("Failed to refresh session")
              error.code = "REFRESH_FAILED"
              throw error
            }
          } catch (refreshError) {
            const error: AuthApiError = new Error("Session refresh failed")
            error.code = "REFRESH_ERROR"
            throw error
          }
        }

        if (!response.ok) {
          const error: AuthApiError = new Error(`HTTP error! status: ${response.status}`)
          error.status = response.status
          throw error
        }

        return response
      } catch (error) {
        // Create custom error event for global error handling
        const errorEvent = new CustomEvent("authApiError", {
          detail: {
            error,
            url,
            timestamp: new Date().toISOString(),
          },
        })
        
        if (typeof window !== "undefined") {
          window.dispatchEvent(errorEvent)
        }
        
        throw error
      }
    },
    [session, refreshToken, router]
  )

  return { 
    makeAuthenticatedRequest, 
    session, 
    isRefreshing,
    refreshToken 
  }
}