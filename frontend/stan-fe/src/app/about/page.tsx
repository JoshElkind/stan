import Header from "@/components/header"
import Footer from "@/components/footer"
import MyPost from "@/app/about/mypost"

export default function AboutPage() {
  return (
    <div className="flex flex-col min-h-screen">
    
      <main className="flex-1 pt-24">
        <MyPost />
      </main>
      <Footer />
    </div>
  )
}
