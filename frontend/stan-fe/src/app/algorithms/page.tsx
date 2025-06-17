import Header from "@/components/header"
import Footer from "@/components/footer"
import MyAlgos from "@/app/algorithms/my_algos"

export default function AlgorithmsPage() {
  return (
    <div className="flex flex-col min-h-screen">
    
      <main className="flex-1 pt-24">
        <MyAlgos />
      </main>
      <Footer />
    </div>
  )
}
