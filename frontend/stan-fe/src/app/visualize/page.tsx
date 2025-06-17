import Header from "@/components/header"
import Footer from "@/components/footer"
import Visualize from "@/app/visualize/logistics"

export default function VisualizePage() {
  return (
    <div className="flex flex-col min-h-screen">
      <main className="flex-1 pt-24">
        <Visualize />
      </main>
      <Footer />
    </div>
  )
}
