import Header from "@/components/header"
import Footer from "@/components/footer"
import MakeForm from "@/app/make/make_form"

export default function MakePage() {
  return (
    <div className="flex flex-col min-h-screen">
      <main className="flex-1 pt-24">
        <MakeForm />
      </main>
      <Footer />
    </div>
  )
}
