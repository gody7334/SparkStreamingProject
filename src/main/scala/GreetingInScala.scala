class GreetingInScala {
    def greet() {
        val delegate = new GreetingInJava
        delegate.greet()
    }
}