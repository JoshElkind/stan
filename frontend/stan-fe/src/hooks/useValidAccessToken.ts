import { useSession } from "next-auth/react"

export const useValidAccessToken = () => {
  const { data: session, update, status } = useSession()

  const getAccessToken = async () => {
    if (status !== "authenticated" || !session) {
      return null
    }

    const needsRefresh = 
      session?.error === "RefreshAccessTokenError" ||
      (session?.accessTokenExpires && Date.now() > session.accessTokenExpires - 60_000)

    if (needsRefresh) {
      try {
        await update() 
      } catch (error) {
        console.error("Failed to refresh token:", error)
        return null
      }
    }

    return session?.accessToken || null
  }

  return { getAccessToken, status }
}