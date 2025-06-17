import Header from "@/components/header"
import Footer from "@/components/footer"
import AllList from "@/app/all/alllist"

export default function EvaluatePage() {
  return (
    <div className="flex flex-col min-h-screen">
      <main className="flex-1 pt-24">
        <AllList />
      </main>
      <Footer />
    </div>
  )
}
