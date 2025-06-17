import Header from "@/components/header"
import Footer from "@/components/footer"
import Manager from "@/app/evaluate/manager"

export default function EvaluatePage() {
  return (
    <div className="flex flex-col min-h-screen">
      <main className="flex-1 pt-24">
        <Manager />
      </main>
      <Footer />
    </div>
  )
}
