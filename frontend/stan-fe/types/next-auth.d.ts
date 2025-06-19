import NextAuth from "next-auth"

declare module "next-auth" {
  interface Session {
    accessToken?: string
    accessTokenExpires?: number
    refreshToken?: string
    id_token?: string 
    error?: string
  }

  interface JWT {
    accessToken?: string
    accessTokenExpires?: number
    refreshToken?: string
    idToken?: string  
    error?: string
  }
}
