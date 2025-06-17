import Image from "next/image"

export default function MyPost() {
  return (
    <div className="min-h-screen bg-white">
      <div className="px-65 py-12">
        <h1 className="text-5xl font-bold text-black text-left">About</h1>
      </div>

      <div className="flex justify-center items-center mt-8">
        <div className="max-w-2xl">
          <div className="bg-gray-100 rounded-lg p-8">
            <div className="flex justify-between items-start mb-6">
              <div className="flex items-center gap-1">
                <div className="relative w-16 h-16">
                  <Image
                    src="/images/myface.png"
                    alt="Joshua Elkind"
                    width={64}
                    height={64}
                    className="rounded-full object-cover"
                  />
                </div>
                <h2 className="text-xl font-semibold text-gray-800">Joshua Elkind</h2>
              </div>

              <div className="text-gray-600 text-sm">May 5, 2025</div>
            </div>

            <div className="text-gray-700 leading-relaxed">
              <p>
                Hi, I'm an undergraduate student at the University of Waterloo studying Computer Science, with a strong
                passion for Software Development, Data Science, and Mathematics. As someone who has actively traded
                stocks for years, I often found myself wanting a reliable and flexible platform to test my trading
                strategies before risking real capital. That's what inspired me to build this project. This site allows
                traders—whether you're just starting out or already experienced—to upload or create their own stock
                trading algorithms and evaluate their performance using historical data. It's designed to be a safe,
                insightful space to experiment with your ideas and refine your setups before taking them live.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
