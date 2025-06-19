import NextAuth from "next-auth"
import GoogleProvider from "next-auth/providers/google"

const refreshAccessToken = async (token: any) => {
  try {
    const url = "https://oauth2.googleapis.com/token"
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        client_id: process.env.GOOGLE_CLIENT_ID!,
        client_secret: process.env.GOOGLE_CLIENT_SECRET!,
        grant_type: "refresh_token",
        refresh_token: token.refreshToken,
      }),
    })

    const refreshed = await response.json()
    if (!response.ok) throw refreshed

    return {
      ...token,
      accessToken: refreshed.access_token,
      idToken: refreshed.id_token,
      accessTokenExpires: Date.now() + refreshed.expires_in * 1000,
      refreshToken: refreshed.refresh_token ?? token.refreshToken,
    }
  } catch (err) {
    console.error("Failed to refresh access token", err)
    return {
      ...token,
      error: "RefreshAccessTokenError",
    }
  }
}

export const { handlers, auth, signIn, signOut } = NextAuth({
  providers: [
    GoogleProvider({
      clientId: process.env.GOOGLE_CLIENT_ID!,
      clientSecret: process.env.GOOGLE_CLIENT_SECRET!,
      authorization: {
        params: {
          access_type: "offline",
          prompt: "consent",
        },
      },
    }),
  ],
  callbacks: {
    async jwt({ token, account, trigger }) {
      // Initial login
      if (account) {
        return {
          ...token,
          accessToken: account.access_token ?? account.id_token,
          idToken: account.id_token, // ✅ send this to Django
          refreshToken: account.refresh_token,
          accessTokenExpires: account.expires_at
            ? account.expires_at * 1000
            : Date.now() + 60 * 60 * 1000,
        }
      }

      // Token still valid
      if (token.accessTokenExpires && Date.now() < token.accessTokenExpires) {
        return token
      }

      // No refresh token
      if (!token.refreshToken) {
        return token
      }

      // Refresh if expired
      if (trigger === "update" || Date.now() >= token.accessTokenExpires) {
        return await refreshAccessToken(token)
      }

      return token
    },

    async session({ session, token }) {
      session.accessToken = token.accessToken as string
      session.refreshToken = token.refreshToken as string
      session.id_token = token.idToken as string // ✅ add to session
      session.error = token.error as string | undefined
      return session
    },
  },
  pages: {
    signIn: "/",
    error: "/",
  },
  session: {
    strategy: "jwt",
    maxAge: 24 * 60 * 60, // 1 day
  },
})

export default handlers
