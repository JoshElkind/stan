import Header from "@/components/header"
import Footer from "@/components/footer"
import Blog from "@/app/faq/blog"

export default function FAQPage() {
  return (
    <div className="flex flex-col min-h-screen">
     
      <main className="flex-1 pt-24">
        <Blog />
      </main>
      <Footer />
    </div>
  )
}
