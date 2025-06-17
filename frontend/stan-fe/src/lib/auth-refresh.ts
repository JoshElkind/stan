"use client"

import { useEffect, useState } from "react"
import { useSession } from "next-auth/react"


export function useTokenRefresh() {
  const { data: session, status, update } = useSession()
  const [isRefreshing, setIsRefreshing] = useState(false)

  useEffect(() => {
    if (status !== "authenticated" || !session) {
      return
    }

    const interval = setInterval(async () => {
      if (status !== "authenticated" || !session) {
        return
      }

      const isMissingToken = !session?.accessToken
      const isErrored = session?.error === "RefreshAccessTokenError"
      const isMissingExpiry = !session?.accessTokenExpires

      if (isMissingToken || isErrored || isMissingExpiry) {
        return 
      }

      const isTokenExpiring = Date.now() > session.accessTokenExpires - 60_000
      
      if (isTokenExpiring && !isRefreshing) {
        try {
          setIsRefreshing(true)
          await update()
        } catch (err) {

        } finally {
          setIsRefreshing(false)
        }
      }
    }, 5 * 60 * 1000) 

    return () => clearInterval(interval)
  }, [status, session?.accessToken, session?.accessTokenExpires, session?.error, update, isRefreshing])

  return { isRefreshing, status }
}