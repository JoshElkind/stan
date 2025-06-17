declare module "next-auth" {
  interface Session {
    accessToken?: string
    refreshToken?: string
    error?: string
  }

  interface JWT {
    accessToken?: string
    refreshToken?: string
    accessTokenExpires?: number
    error?: string
  }
}

